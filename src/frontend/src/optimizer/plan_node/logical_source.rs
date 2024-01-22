// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::{max, min};
use std::ops::Bound;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::rc::Rc;

use fixedbitset::FixedBitSet;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::{
    ColumnCatalog, ColumnDesc, Field, Schema, KAFKA_TIMESTAMP_COLUMN_NAME,
};
use risingwave_common::error::Result;
use risingwave_connector::source::DataType;
use risingwave_pb::plan_common::column_desc::GeneratedOrDefaultColumn;
use risingwave_pb::plan_common::GeneratedColumnDesc;

use super::generic::{GenericPlanRef, SourceNodeKind};
use super::stream_watermark_filter::StreamWatermarkFilter;
use super::utils::{childless_record, Distill};
use super::{
    generic, BatchProject, BatchSource, ColPrunable, ExprRewritable, Logical, LogicalFilter,
    LogicalProject, PlanBase, PlanRef, PredicatePushdown, StreamProject, StreamRowIdGen,
    StreamSource, ToBatch, ToStream,
};
use crate::catalog::source_catalog::SourceCatalog;
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprType, ExprVisitor, InputRef};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::stream_fs_fetch::StreamFsFetch;
use crate::optimizer::plan_node::utils::column_names_pretty;
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, StreamDedup,
    ToStreamContext,
};
use crate::optimizer::property::Distribution::HashShard;
use crate::optimizer::property::{Distribution, Order, RequiredDist};
use crate::utils::{ColIndexMapping, Condition, IndexRewriter};

/// `LogicalSource` returns contents of a table or other equivalent object
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalSource {
    pub base: PlanBase<Logical>,
    pub core: generic::Source,

    /// Expressions to output. This field presents and will be turned to a `Project` when
    /// converting to a physical plan, only if there are generated columns.
    output_exprs: Option<Vec<ExprImpl>>,
    /// When there are generated columns, the `StreamRowIdGen`'s row_id_index is different from
    /// the one in `core`. So we store the one in `output_exprs` here.
    output_row_id_index: Option<usize>,
}

impl LogicalSource {
    pub fn new(
        source_catalog: Option<Rc<SourceCatalog>>,
        column_catalog: Vec<ColumnCatalog>,
        row_id_index: Option<usize>,
        kind: SourceNodeKind,
        ctx: OptimizerContextRef,
    ) -> Result<Self> {
        let kafka_timestamp_range = (Bound::Unbounded, Bound::Unbounded);
        let core = generic::Source {
            catalog: source_catalog,
            column_catalog,
            row_id_index,
            kind,
            ctx,
            kafka_timestamp_range,
        };

        let base = PlanBase::new_logical_with_core(&core);

        let output_exprs = Self::derive_output_exprs_from_generated_columns(&core.column_catalog)?;
        let (core, output_row_id_index) = core.exclude_generated_columns();

        Ok(LogicalSource {
            base,
            core,
            output_exprs,
            output_row_id_index,
        })
    }

    pub fn with_catalog(
        source_catalog: Rc<SourceCatalog>,
        kind: SourceNodeKind,
        ctx: OptimizerContextRef,
    ) -> Result<Self> {
        let column_catalogs = source_catalog.columns.clone();
        let row_id_index = source_catalog.row_id_index;
        if !source_catalog.append_only {
            assert!(row_id_index.is_none());
        }

        Self::new(
            Some(source_catalog),
            column_catalogs,
            row_id_index,
            kind,
            ctx,
        )
    }

    /// If there are no generated columns, returns `None`.
    ///
    /// Otherwise, the returned expressions correspond to all columns.
    /// Non-generated columns are represented by `InputRef`.
    pub fn derive_output_exprs_from_generated_columns(
        columns: &[ColumnCatalog],
    ) -> Result<Option<Vec<ExprImpl>>> {
        if !columns.iter().any(|c| c.is_generated()) {
            return Ok(None);
        }

        let col_mapping = {
            let mut mapping = vec![None; columns.len()];
            let mut cur = 0;
            for (idx, column) in columns.iter().enumerate() {
                if !column.is_generated() {
                    mapping[idx] = Some(cur);
                    cur += 1;
                } else {
                    mapping[idx] = None;
                }
            }
            ColIndexMapping::new(mapping, columns.len())
        };

        let mut rewriter = IndexRewriter::new(col_mapping);
        let mut exprs = Vec::with_capacity(columns.len());
        let mut cur = 0;
        for column in columns {
            let column_desc = &column.column_desc;
            let ret_data_type = column_desc.data_type.clone();

            if let Some(GeneratedOrDefaultColumn::GeneratedColumn(generated_column)) =
                &column_desc.generated_or_default_column
            {
                let GeneratedColumnDesc { expr } = generated_column;
                // TODO(yuhao): avoid this `from_expr_proto`.
                let proj_expr =
                    rewriter.rewrite_expr(ExprImpl::from_expr_proto(expr.as_ref().unwrap())?);
                let casted_expr = proj_expr.cast_assign(ret_data_type)?;
                exprs.push(casted_expr);
            } else {
                let input_ref = InputRef {
                    data_type: ret_data_type,
                    index: cur,
                };
                cur += 1;
                exprs.push(ExprImpl::InputRef(Box::new(input_ref)));
            }
        }

        Ok(Some(exprs))
    }

    /// `StreamSource` (list) -> shuffle -> `StreamDedup`
    fn create_fs_list_plan(core: generic::Source) -> Result<PlanRef> {
        let logical_source = generic::Source {
            column_catalog: vec![
                ColumnCatalog {
                    column_desc: ColumnDesc::from_field_with_column_id(
                        &Field {
                            name: "filename".to_string(),
                            data_type: DataType::Varchar,
                            sub_fields: vec![],
                            type_name: "".to_string(),
                        },
                        0,
                    ),
                    is_hidden: false,
                },
                ColumnCatalog {
                    column_desc: ColumnDesc::from_field_with_column_id(
                        &Field {
                            name: "last_edit_time".to_string(),
                            data_type: DataType::Timestamptz,
                            sub_fields: vec![],
                            type_name: "".to_string(),
                        },
                        1,
                    ),
                    is_hidden: false,
                },
                ColumnCatalog {
                    column_desc: ColumnDesc::from_field_with_column_id(
                        &Field {
                            name: "file_size".to_string(),
                            data_type: DataType::Int64,
                            sub_fields: vec![],
                            type_name: "".to_string(),
                        },
                        0,
                    ),
                    is_hidden: false,
                },
            ],
            row_id_index: None,
            ..core
        };
        let mut new_s3_plan: PlanRef = StreamSource {
            base: PlanBase::new_stream_with_core(
                &logical_source,
                Distribution::Single,
                true, // `list` will keep listing all objects, it must be append-only
                false,
                FixedBitSet::with_capacity(logical_source.column_catalog.len()),
            ),
            core: logical_source,
        }
        .into();
        new_s3_plan = RequiredDist::shard_by_key(3, &[0])
            .enforce_if_not_satisfies(new_s3_plan, &Order::any())?;
        new_s3_plan = StreamDedup::new(generic::Dedup {
            input: new_s3_plan,
            dedup_cols: vec![0],
        })
        .into();

        Ok(new_s3_plan)
    }

    pub fn source_catalog(&self) -> Option<Rc<SourceCatalog>> {
        self.core.catalog.clone()
    }

    fn clone_with_kafka_timestamp_range(&self, range: (Bound<i64>, Bound<i64>)) -> Self {
        let mut core = self.core.clone();
        core.kafka_timestamp_range = range;
        Self {
            base: self.base.clone(),
            core,
            output_exprs: self.output_exprs.clone(),
            output_row_id_index: self.output_row_id_index,
        }
    }
}

impl_plan_tree_node_for_leaf! {LogicalSource}
impl Distill for LogicalSource {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = if let Some(catalog) = self.source_catalog() {
            let src = Pretty::from(catalog.name.clone());
            let time = Pretty::debug(&self.core.kafka_timestamp_range);
            vec![
                ("source", src),
                ("columns", column_names_pretty(self.schema())),
                ("time_range", time),
            ]
        } else {
            vec![]
        };
        childless_record("LogicalSource", fields)
    }
}

impl ColPrunable for LogicalSource {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        let mapping = ColIndexMapping::with_remaining_columns(required_cols, self.schema().len());
        LogicalProject::with_mapping(self.clone().into(), mapping).into()
    }
}

impl ExprRewritable for LogicalSource {
    fn has_rewritable_expr(&self) -> bool {
        self.output_exprs.is_some()
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut output_exprs = self.output_exprs.clone();

        for expr in output_exprs.iter_mut().flatten() {
            *expr = r.rewrite_expr(expr.clone());
        }

        Self {
            output_exprs,
            ..self.clone()
        }
        .into()
    }
}

impl ExprVisitable for LogicalSource {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.output_exprs
            .iter()
            .flatten()
            .for_each(|e| v.visit_expr(e));
    }
}

/// A util function to extract kafka offset timestamp range.
///
/// Currently we only support limiting kafka offset timestamp range using literals, e.g. we only
/// support expressions like `_rw_kafka_timestamp <= '2022-10-11 1:00:00+00:00'`.
///
/// # Parameters
///
/// * `expr`: Expression to be consumed.
/// * `range`: Original timestamp range, if `expr` can be recognized, we will update `range`.
/// * `schema`: Input schema.
///
/// # Return Value
///
/// If `expr` can be recognized and consumed by this function, then we return `None`.
/// Otherwise `expr` is returned.
fn expr_to_kafka_timestamp_range(
    expr: ExprImpl,
    range: &mut (Bound<i64>, Bound<i64>),
    schema: &Schema,
) -> Option<ExprImpl> {
    let merge_upper_bound = |first, second| -> Bound<i64> {
        match (first, second) {
            (first, Unbounded) => first,
            (Unbounded, second) => second,
            (Included(f1), Included(f2)) => Included(min(f1, f2)),
            (Included(f1), Excluded(f2)) => {
                if f1 < f2 {
                    Included(f1)
                } else {
                    Excluded(f2)
                }
            }
            (Excluded(f1), Included(f2)) => {
                if f2 < f1 {
                    Included(f2)
                } else {
                    Excluded(f1)
                }
            }
            (Excluded(f1), Excluded(f2)) => Excluded(min(f1, f2)),
        }
    };

    let merge_lower_bound = |first, second| -> Bound<i64> {
        match (first, second) {
            (first, Unbounded) => first,
            (Unbounded, second) => second,
            (Included(f1), Included(f2)) => Included(max(f1, f2)),
            (Included(f1), Excluded(f2)) => {
                if f1 > f2 {
                    Included(f1)
                } else {
                    Excluded(f2)
                }
            }
            (Excluded(f1), Included(f2)) => {
                if f2 > f1 {
                    Included(f2)
                } else {
                    Excluded(f1)
                }
            }
            (Excluded(f1), Excluded(f2)) => Excluded(max(f1, f2)),
        }
    };

    let extract_timestampz_literal = |expr: &ExprImpl| -> Result<Option<(i64, bool)>> {
        match expr {
            ExprImpl::FunctionCall(function_call) if function_call.inputs().len() == 2 => {
                match (&function_call.inputs()[0], &function_call.inputs()[1]) {
                    (ExprImpl::InputRef(input_ref), literal)
                        if let Some(datum) = literal.try_fold_const().transpose()?
                            && schema.fields[input_ref.index].name
                                == KAFKA_TIMESTAMP_COLUMN_NAME
                            && literal.return_type() == DataType::Timestamptz =>
                    {
                        Ok(Some((
                            datum.unwrap().into_timestamptz().timestamp_millis(),
                            false,
                        )))
                    }
                    (literal, ExprImpl::InputRef(input_ref))
                        if let Some(datum) = literal.try_fold_const().transpose()?
                            && schema.fields[input_ref.index].name
                                == KAFKA_TIMESTAMP_COLUMN_NAME
                            && literal.return_type() == DataType::Timestamptz =>
                    {
                        Ok(Some((
                            datum.unwrap().into_timestamptz().timestamp_millis(),
                            true,
                        )))
                    }
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        }
    };

    match &expr {
        ExprImpl::FunctionCall(function_call) => {
            if let Ok(Some((timestampz_literal, reverse))) = extract_timestampz_literal(&expr) {
                match function_call.func_type() {
                    ExprType::GreaterThan => {
                        if reverse {
                            range.1 = merge_upper_bound(range.1, Excluded(timestampz_literal));
                        } else {
                            range.0 = merge_lower_bound(range.0, Excluded(timestampz_literal));
                        }

                        None
                    }
                    ExprType::GreaterThanOrEqual => {
                        if reverse {
                            range.1 = merge_upper_bound(range.1, Included(timestampz_literal));
                        } else {
                            range.0 = merge_lower_bound(range.0, Included(timestampz_literal));
                        }
                        None
                    }
                    ExprType::Equal => {
                        range.0 = merge_lower_bound(range.0, Included(timestampz_literal));
                        range.1 = merge_upper_bound(range.1, Included(timestampz_literal));
                        None
                    }
                    ExprType::LessThan => {
                        if reverse {
                            range.0 = merge_lower_bound(range.0, Excluded(timestampz_literal));
                        } else {
                            range.1 = merge_upper_bound(range.1, Excluded(timestampz_literal));
                        }
                        None
                    }
                    ExprType::LessThanOrEqual => {
                        if reverse {
                            range.0 = merge_lower_bound(range.0, Included(timestampz_literal));
                        } else {
                            range.1 = merge_upper_bound(range.1, Included(timestampz_literal));
                        }
                        None
                    }
                    _ => Some(expr),
                }
            } else {
                Some(expr)
            }
        }
        _ => Some(expr),
    }
}

impl PredicatePushdown for LogicalSource {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        let mut range = self.core.kafka_timestamp_range;

        let mut new_conjunctions = Vec::with_capacity(predicate.conjunctions.len());
        for expr in predicate.conjunctions {
            if let Some(e) = expr_to_kafka_timestamp_range(expr, &mut range, self.base.schema()) {
                // Not recognized, so push back
                new_conjunctions.push(e);
            }
        }

        let new_source = self.clone_with_kafka_timestamp_range(range).into();

        if new_conjunctions.is_empty() {
            new_source
        } else {
            LogicalFilter::create(
                new_source,
                Condition {
                    conjunctions: new_conjunctions,
                },
            )
        }
    }
}

impl ToBatch for LogicalSource {
    fn to_batch(&self) -> Result<PlanRef> {
        if self.core.is_new_fs_connector() {
            bail_not_implemented!("New fs connector for batch");
        }
        let mut plan: PlanRef = BatchSource::new(self.core.clone()).into();

        if let Some(exprs) = &self.output_exprs {
            let logical_project = generic::Project::new(exprs.to_vec(), plan);
            plan = BatchProject::new(logical_project).into();
        }

        Ok(plan)
    }
}

impl ToStream for LogicalSource {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        let mut plan: PlanRef;

        match self.core.kind {
            SourceNodeKind::CreateTable | SourceNodeKind::CreateSourceWithStreamjob => {
                // Note: for create table, row_id and generated columns is created in plan_root.gen_table_plan.
                // for backfill-able source, row_id and generated columns is created after SourceBackfill node.
                if self.core.is_new_fs_connector() {
                    plan = Self::create_fs_list_plan(self.core.clone())?;
                    plan = StreamFsFetch::new(plan, self.core.clone()).into();
                } else {
                    plan = StreamSource::new(self.core.clone()).into()
                }
            }
            SourceNodeKind::CreateMViewOrBatch => {
                // Create MV on source.
                if self.core.is_new_fs_connector() {
                    plan = Self::create_fs_list_plan(self.core.clone())?;
                    plan = StreamFsFetch::new(plan, self.core.clone()).into();
                } else {
                    plan = StreamSource::new(self.core.clone()).into()
                }

                if let Some(exprs) = &self.output_exprs {
                    let logical_project = generic::Project::new(exprs.to_vec(), plan);
                    plan = StreamProject::new(logical_project).into();
                }

                if let Some(catalog) = self.source_catalog()
                    && !catalog.watermark_descs.is_empty()
                {
                    plan = StreamWatermarkFilter::new(plan, catalog.watermark_descs.clone()).into();
                }

                if let Some(row_id_index) = self.output_row_id_index {
                    plan = StreamRowIdGen::new_with_dist(
                        plan,
                        row_id_index,
                        HashShard(vec![row_id_index]),
                    )
                    .into();
                }
            }
        }
        Ok(plan)
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        Ok((
            self.clone().into(),
            ColIndexMapping::identity(self.schema().len()),
        ))
    }
}
