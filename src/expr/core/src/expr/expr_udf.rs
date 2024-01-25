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

use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, LazyLock, Weak};
use std::time::Duration;

use anyhow::Context;
use arrow_schema::{Field, Fields, Schema};
use arrow_udf_js::{CallMode, Runtime as JsRuntime};
use arrow_udf_wasm::Runtime as WasmRuntime;
use await_tree::InstrumentAwait;
use cfg_or_panic::cfg_or_panic;
use moka::future::Cache;
use risingwave_common::array::{ArrayError, ArrayRef, DataChunk};
use risingwave_common::config::ObjectStoreConfig;
use risingwave_common::types::DataType;
use risingwave_object_store::object::build_remote_object_store;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_pb::expr::ExprNode;
use risingwave_udf::ArrowFlightUdfClient;
use thiserror_ext::AsReport;

use super::{BoxedExpression, Build};
use crate::expr::Expression;
use crate::{bail, Result};

#[derive(Debug)]
pub struct UserDefinedFunction {
    children: Vec<BoxedExpression>,
    return_type: DataType,
    arg_schema: Arc<Schema>,
    imp: UdfImpl,
    identifier: String,
    span: await_tree::Span,
    /// Number of remaining successful calls until retry is enabled.
    /// If non-zero, we will not retry on connection errors to prevent blocking the stream.
    /// On each connection error, the count will be reset to `INITIAL_RETRY_COUNT`.
    /// On each successful call, the count will be decreased by 1.
    /// See <https://github.com/risingwavelabs/risingwave/issues/13791>.
    disable_retry_count: AtomicU8,
}

const INITIAL_RETRY_COUNT: u8 = 16;

#[derive(Debug)]
enum UdfImpl {
    External(Arc<ArrowFlightUdfClient>),
    Wasm(Arc<WasmRuntime>),
    JavaScript(JsRuntime),
}

#[async_trait::async_trait]
impl Expression for UserDefinedFunction {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    #[cfg_or_panic(not(madsim))]
    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let vis = input.visibility();
        let mut columns = Vec::with_capacity(self.children.len());
        for child in &self.children {
            let array = child.eval(input).await?;
            columns.push(array);
        }
        self.eval_inner(columns, vis).await
    }
}

impl UserDefinedFunction {
    async fn eval_inner(
        &self,
        columns: Vec<ArrayRef>,
        vis: &risingwave_common::buffer::Bitmap,
    ) -> Result<ArrayRef> {
        let chunk = DataChunk::new(columns, vis.clone());
        let compacted_chunk = chunk.compact_cow();
        let compacted_columns: Vec<arrow_array::ArrayRef> = compacted_chunk
            .columns()
            .iter()
            .map(|c| {
                c.as_ref()
                    .try_into()
                    .expect("failed covert ArrayRef to arrow_array::ArrayRef")
            })
            .collect();
        let opts = arrow_array::RecordBatchOptions::default()
            .with_row_count(Some(compacted_chunk.capacity()));
        let input = arrow_array::RecordBatch::try_new_with_options(
            self.arg_schema.clone(),
            compacted_columns,
            &opts,
        )
        .expect("failed to build record batch");

        let output: arrow_array::RecordBatch = match &self.imp {
            UdfImpl::Wasm(runtime) => runtime.call(&self.identifier, &input)?,
            UdfImpl::JavaScript(runtime) => runtime.call(&self.identifier, &input)?,
            UdfImpl::External(client) => {
                let disable_retry_count = self.disable_retry_count.load(Ordering::Relaxed);
                let result = if disable_retry_count != 0 {
                    client
                        .call(&self.identifier, input)
                        .instrument_await(self.span.clone())
                        .await
                } else {
                    client
                        .call_with_retry(&self.identifier, input)
                        .instrument_await(self.span.clone())
                        .await
                };
                let disable_retry_count = self.disable_retry_count.load(Ordering::Relaxed);
                let connection_error = matches!(&result, Err(e) if e.is_connection_error());
                if connection_error && disable_retry_count != INITIAL_RETRY_COUNT {
                    // reset count on connection error
                    self.disable_retry_count
                        .store(INITIAL_RETRY_COUNT, Ordering::Relaxed);
                } else if !connection_error && disable_retry_count != 0 {
                    // decrease count on success, ignore if exchange failed
                    _ = self.disable_retry_count.compare_exchange(
                        disable_retry_count,
                        disable_retry_count - 1,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    );
                }
                result?
            }
        };
        if output.num_rows() != vis.count_ones() {
            bail!(
                "UDF returned {} rows, but expected {}",
                output.num_rows(),
                vis.len(),
            );
        }

        let data_chunk = DataChunk::try_from(&output)?;
        let output = data_chunk.uncompact(vis.clone());

        let Some(array) = output.columns().first() else {
            bail!("UDF returned no columns");
        };
        if !array.data_type().equals_datatype(&self.return_type) {
            bail!(
                "UDF returned {:?}, but expected {:?}",
                array.data_type(),
                self.return_type,
            );
        }

        Ok(array.clone())
    }
}

#[cfg_or_panic(not(madsim))]
impl Build for UserDefinedFunction {
    fn build(
        prost: &ExprNode,
        build_child: impl Fn(&ExprNode) -> Result<BoxedExpression>,
    ) -> Result<Self> {
        let return_type = DataType::from(prost.get_return_type().unwrap());
        let udf = prost.get_rex_node().unwrap().as_udf().unwrap();

        let identifier = udf.get_identifier()?;
        let imp = match udf.language.as_str() {
            "wasm" => {
                let link = udf.get_link()?;
                // Use `block_in_place` as an escape hatch to run async code here in sync context.
                // Calling `block_on` directly will panic.
                UdfImpl::Wasm(tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(get_or_create_wasm_runtime(link))
                })?)
            }
            "javascript" => {
                let mut rt = JsRuntime::new()?;
                let body = format!(
                    "export function {}({}) {{ {} }}",
                    identifier,
                    udf.arg_names.join(","),
                    udf.get_body()?
                );
                rt.add_function(
                    identifier,
                    arrow_schema::DataType::try_from(&return_type)?,
                    CallMode::CalledOnNullInput,
                    &body,
                )?;
                UdfImpl::JavaScript(rt)
            }
            _ => {
                let link = udf.get_link()?;
                UdfImpl::External(get_or_create_flight_client(link)?)
            }
        };

        let arg_schema = Arc::new(Schema::new(
            udf.arg_types
                .iter()
                .map::<Result<_>, _>(|t| {
                    Ok(Field::new(
                        "",
                        DataType::from(t).try_into().map_err(|e: ArrayError| {
                            risingwave_udf::Error::unsupported(e.to_report_string())
                        })?,
                        true,
                    ))
                })
                .try_collect::<Fields>()?,
        ));

        Ok(Self {
            children: udf.children.iter().map(build_child).try_collect()?,
            return_type,
            arg_schema,
            imp,
            identifier: identifier.clone(),
            span: format!("udf_call({})", identifier).into(),
            disable_retry_count: AtomicU8::new(0),
        })
    }
}

#[cfg(not(madsim))]
/// Get or create a client for the given UDF service.
///
/// There is a global cache for clients, so that we can reuse the same client for the same service.
pub(crate) fn get_or_create_flight_client(link: &str) -> Result<Arc<ArrowFlightUdfClient>> {
    static CLIENTS: LazyLock<std::sync::Mutex<HashMap<String, Weak<ArrowFlightUdfClient>>>> =
        LazyLock::new(Default::default);
    let mut clients = CLIENTS.lock().unwrap();
    if let Some(client) = clients.get(link).and_then(|c| c.upgrade()) {
        // reuse existing client
        Ok(client)
    } else {
        // create new client
        let client = Arc::new(ArrowFlightUdfClient::connect_lazy(link)?);
        clients.insert(link.into(), Arc::downgrade(&client));
        Ok(client)
    }
}

/// Get or create a wasm runtime.
///
/// Runtimes returned by this function are cached inside for at least 60 seconds.
/// Later calls with the same link will reuse the same runtime.
#[cfg_or_panic(not(madsim))]
pub async fn get_or_create_wasm_runtime(link: &str) -> Result<Arc<WasmRuntime>> {
    static RUNTIMES: LazyLock<Cache<String, Arc<WasmRuntime>>> = LazyLock::new(|| {
        Cache::builder()
            .time_to_idle(Duration::from_secs(60))
            .build()
    });

    if let Some(runtime) = RUNTIMES.get(link).await {
        return Ok(runtime.clone());
    }

    // create new runtime
    let (wasm_storage_url, object_name) = link
        .rsplit_once('/')
        .context("invalid link for wasm function")?;

    // load wasm binary from object store
    let object_store = build_remote_object_store(
        wasm_storage_url,
        Arc::new(ObjectStoreMetrics::unused()),
        "Wasm Engine",
        ObjectStoreConfig::default(),
    )
    .await;
    let binary = object_store
        .read(object_name, ..)
        .await
        .context("failed to load wasm binary from object storage")?;

    let runtime = Arc::new(arrow_udf_wasm::Runtime::new(&binary)?);
    RUNTIMES.insert(link.into(), runtime.clone()).await;
    Ok(runtime)
}
