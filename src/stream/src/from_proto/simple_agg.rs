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

//! Streaming Simple Aggregator

use risingwave_expr::aggregate::AggCall;
use risingwave_pb::stream_plan::SimpleAggNode;

use super::agg_common::{
    build_agg_state_storages_from_proto, build_distinct_dedup_table_from_proto,
};
use super::*;
use crate::common::table::state_table::StateTable;
use crate::executor::agg_common::{AggExecutorArgs, SimpleAggExecutorExtraArgs};
use crate::executor::SimpleAggExecutor;

pub struct SimpleAggExecutorBuilder;

impl ExecutorBuilder for SimpleAggExecutorBuilder {
    type Node = SimpleAggNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<BoxedExecutor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();
        let agg_calls: Vec<AggCall> = node
            .get_agg_calls()
            .iter()
            .map(AggCall::from_protobuf)
            .try_collect()?;
        let storages =
            build_agg_state_storages_from_proto(node.get_agg_call_states(), store.clone(), None)
                .await;
        // disable sanity check so that old value is not required when updating states
        let intermediate_state_table = StateTable::from_table_catalog_inconsistent_op(
            node.get_intermediate_state_table().unwrap(),
            store.clone(),
            None,
        )
        .await;
        let distinct_dedup_tables =
            build_distinct_dedup_table_from_proto(node.get_distinct_dedup_tables(), store, None)
                .await;

        Ok(SimpleAggExecutor::new(AggExecutorArgs {
            version: node.version(),

            input,
            actor_ctx: params.actor_context,
            info: params.info,

            extreme_cache_size: params.env.config().developer.unsafe_extreme_cache_size,

            agg_calls,
            row_count_index: node.get_row_count_index() as usize,
            storages,
            intermediate_state_table,
            distinct_dedup_tables,
            watermark_epoch: params.watermark_epoch,
            extra: SimpleAggExecutorExtraArgs {},
        })?
        .boxed())
    }
}
