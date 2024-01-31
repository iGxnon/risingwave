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

use await_tree::InstrumentAwait;
use futures::{Stream, StreamExt, TryStreamExt};
use risingwave_pb::stream_service::stream_service_server::StreamService;
use risingwave_pb::stream_service::streaming_control_stream_request::InitRequest;
use risingwave_pb::stream_service::*;
use risingwave_storage::dispatch_state_store;
use risingwave_stream::error::StreamError;
use risingwave_stream::task::{LocalStreamManager, StreamEnvironment};
use thiserror_ext::AsReport;
use tokio::sync::mpsc::unbounded_channel;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status, Streaming};

#[derive(Clone)]
pub struct StreamServiceImpl {
    mgr: LocalStreamManager,
    env: StreamEnvironment,
}

impl StreamServiceImpl {
    pub fn new(mgr: LocalStreamManager, env: StreamEnvironment) -> Self {
        StreamServiceImpl { mgr, env }
    }
}

#[async_trait::async_trait]
impl StreamService for StreamServiceImpl {
    type StreamingControlStreamStream =
        impl Stream<Item = std::result::Result<StreamingControlStreamResponse, tonic::Status>>;

    #[cfg_attr(coverage, coverage(off))]
    async fn update_actors(
        &self,
        request: Request<UpdateActorsRequest>,
    ) -> std::result::Result<Response<UpdateActorsResponse>, Status> {
        let req = request.into_inner();
        let res = self.mgr.update_actors(req.actors).await;
        match res {
            Err(e) => {
                error!(error = %e.as_report(), "failed to update stream actor");
                Err(e.into())
            }
            Ok(()) => Ok(Response::new(UpdateActorsResponse { status: None })),
        }
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn build_actors(
        &self,
        request: Request<BuildActorsRequest>,
    ) -> std::result::Result<Response<BuildActorsResponse>, Status> {
        let req = request.into_inner();

        let actor_id = req.actor_id;
        let res = self.mgr.build_actors(actor_id).await;
        match res {
            Err(e) => {
                error!(error = %e.as_report(), "failed to build actors");
                Err(e.into())
            }
            Ok(()) => Ok(Response::new(BuildActorsResponse {
                request_id: req.request_id,
                status: None,
            })),
        }
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn broadcast_actor_info_table(
        &self,
        request: Request<BroadcastActorInfoTableRequest>,
    ) -> std::result::Result<Response<BroadcastActorInfoTableResponse>, Status> {
        let req = request.into_inner();

        let res = self.mgr.update_actor_info(&req.info);
        match res {
            Err(e) => {
                error!(error = %e.as_report(), "failed to update actor info table actor");
                Err(e.into())
            }
            Ok(()) => Ok(Response::new(BroadcastActorInfoTableResponse {
                status: None,
            })),
        }
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn drop_actors(
        &self,
        request: Request<DropActorsRequest>,
    ) -> std::result::Result<Response<DropActorsResponse>, Status> {
        let req = request.into_inner();
        let actors = req.actor_ids;
        self.mgr.drop_actors(actors).await?;
        Ok(Response::new(DropActorsResponse {
            request_id: req.request_id,
            status: None,
        }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn wait_epoch_commit(
        &self,
        request: Request<WaitEpochCommitRequest>,
    ) -> Result<Response<WaitEpochCommitResponse>, Status> {
        let epoch = request.into_inner().epoch;

        dispatch_state_store!(self.env.state_store(), store, {
            use risingwave_hummock_sdk::HummockReadEpoch;
            use risingwave_storage::StateStore;

            store
                .try_wait_epoch(HummockReadEpoch::Committed(epoch))
                .instrument_await(format!("wait_epoch_commit (epoch {})", epoch))
                .await
                .map_err(StreamError::from)?;
        });

        Ok(Response::new(WaitEpochCommitResponse { status: None }))
    }

    async fn streaming_control_stream(
        &self,
        request: Request<Streaming<StreamingControlStreamRequest>>,
    ) -> Result<Response<Self::StreamingControlStreamStream>, Status> {
        let mut stream = request.into_inner().boxed();
        let first_request = stream
            .try_next()
            .await?
            .ok_or_else(|| Status::invalid_argument(format!("failed to receive first request")))?;
        match first_request {
            StreamingControlStreamRequest {
                request: Some(streaming_control_stream_request::Request::Init(InitRequest {})),
            } => {}
            other => {
                return Err(Status::invalid_argument(format!(
                    "unexpected first request: {:?}",
                    other
                )));
            }
        };
        let (tx, rx) = unbounded_channel();
        self.mgr.handle_new_control_stream(tx, stream);
        Ok(Response::new(UnboundedReceiverStream::new(rx)))
    }
}
