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

use std::collections::{HashMap, HashSet};
use std::future::pending;
use std::sync::Arc;

use anyhow::anyhow;
use futures::stream::{BoxStream, FuturesUnordered};
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use parking_lot::Mutex;
use risingwave_pb::stream_service::barrier_complete_response::{
    GroupedSstableInfo, PbCreateMviewProgress,
};
use rw_futures_util::{pending_on_none, AttachedFuture};
use thiserror_ext::AsReport;
use tokio::select;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::Status;

use self::managed_state::ManagedBarrierState;
use crate::error::{IntoUnexpectedExit, StreamError, StreamResult};
use crate::task::{ActorHandle, ActorId, AtomicU64Ref, SharedContext, StreamEnvironment};

mod managed_state;
mod progress;
#[cfg(test)]
mod tests;

pub use progress::CreateMviewProgress;
use risingwave_common::util::runtime::BackgroundShutdownRuntime;
use risingwave_hummock_sdk::table_stats::to_prost_table_stats_map;
use risingwave_hummock_sdk::LocalSstableInfo;
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::barrier::BarrierKind;
use risingwave_pb::stream_service::streaming_control_stream_request::Request;
use risingwave_pb::stream_service::streaming_control_stream_response::InitResponse;
use risingwave_pb::stream_service::{
    streaming_control_stream_response, BarrierCompleteResponse, StreamingControlStreamRequest,
    StreamingControlStreamResponse,
};
use risingwave_storage::store::SyncResult;

use crate::executor::monitor::StreamingMetrics;
use crate::executor::{Actor, Barrier, DispatchExecutor};
use crate::task::barrier_manager::progress::BackfillState;
use crate::task::barrier_manager::LocalBarrierEvent::{ReportActorCollected, ReportActorFailure};

/// If enabled, all actors will be grouped in the same tracing span within one epoch.
/// Note that this option will significantly increase the overhead of tracing.
pub const ENABLE_BARRIER_AGGREGATION: bool = false;

/// Collect result of some barrier on current compute node. Will be reported to the meta service.
#[derive(Debug)]
pub struct BarrierCompleteResult {
    /// The result returned from `sync` of `StateStore`.
    pub sync_result: Option<SyncResult>,

    /// The updated creation progress of materialized view after this barrier.
    pub create_mview_progress: Vec<PbCreateMviewProgress>,
}

pub(super) enum LocalBarrierEvent {
    NewControlStream {
        sender: UnboundedSender<Result<StreamingControlStreamResponse, Status>>,
        request_stream: BoxStream<'static, Result<StreamingControlStreamRequest, Status>>,
    },
    RegisterSender {
        actor_id: ActorId,
        sender: UnboundedSender<Barrier>,
    },
    ReportActorCollected {
        actor_id: ActorId,
        barrier: Barrier,
    },
    ReportActorFailure {
        actor_id: ActorId,
        err: StreamError,
    },
    ReportCreateProgress {
        current_epoch: u64,
        actor: ActorId,
        state: BackfillState,
    },
    DropActors {
        actors: Vec<ActorId>,
        result_sender: oneshot::Sender<()>,
    },
    UpdateActors {
        actors: Vec<stream_plan::StreamActor>,
        result_sender: oneshot::Sender<StreamResult<()>>,
    },
    BuildActors {
        actors: Vec<ActorId>,
        result_sender: oneshot::Sender<StreamResult<()>>,
    },
    #[cfg(test)]
    Flush(oneshot::Sender<()>),
}

pub(crate) struct StreamActorManagerState {
    /// Each processor runs in a future. Upon receiving a `Terminate` message, they will exit.
    /// `handles` store join handles of these futures, and therefore we could wait their
    /// termination.
    pub(super) handles: HashMap<ActorId, ActorHandle>,

    /// Stores all actor information, taken after actor built.
    pub(super) actors: HashMap<ActorId, stream_plan::StreamActor>,

    /// Stores all actor tokio runtime monitoring tasks.
    pub(super) actor_monitor_tasks: HashMap<ActorId, ActorHandle>,

    #[expect(clippy::type_complexity)]
    pub(super) creating_actors: FuturesUnordered<
        AttachedFuture<
            JoinHandle<StreamResult<Vec<Actor<DispatchExecutor>>>>,
            oneshot::Sender<StreamResult<()>>,
        >,
    >,
}

impl StreamActorManagerState {
    fn new() -> Self {
        Self {
            handles: HashMap::new(),
            actors: HashMap::new(),
            actor_monitor_tasks: HashMap::new(),
            creating_actors: FuturesUnordered::new(),
        }
    }

    async fn next_created_actors(
        &mut self,
    ) -> (
        oneshot::Sender<StreamResult<()>>,
        StreamResult<Vec<Actor<DispatchExecutor>>>,
    ) {
        let (join_result, sender) = pending_on_none(self.creating_actors.next()).await;
        (
            sender,
            join_result
                .map_err(|join_error| {
                    anyhow!(
                        "failed to join creating actors futures: {:?}",
                        join_error.as_report()
                    )
                    .into()
                })
                .and_then(|result| result),
        )
    }
}

pub(crate) struct StreamActorManager {
    pub(super) env: StreamEnvironment,
    pub(super) context: Arc<SharedContext>,
    pub(super) streaming_metrics: Arc<StreamingMetrics>,

    /// Watermark epoch number.
    pub(super) watermark_epoch: AtomicU64Ref,

    pub(super) local_barrier_manager: LocalBarrierManager,

    /// Manages the await-trees of all actors.
    pub(super) await_tree_reg: Option<Arc<Mutex<await_tree::Registry<ActorId>>>>,

    /// Runtime for the streaming actors.
    pub(super) runtime: BackgroundShutdownRuntime,
}

/// [`LocalBarrierWorker`] manages barrier control flow, used by local stream manager.
/// Specifically, [`LocalBarrierWorker`] serve barrier injection from meta server, send the
/// barriers to and collect them from all actors, and finally report the progress.
pub(super) struct LocalBarrierWorker {
    /// Stores all streaming job source sender.
    barrier_senders: HashMap<ActorId, Vec<UnboundedSender<Barrier>>>,

    /// Current barrier collection state.
    state: ManagedBarrierState,

    control_stream_handle: Option<(
        UnboundedSender<Result<StreamingControlStreamResponse, Status>>,
        BoxStream<'static, Result<StreamingControlStreamRequest, Status>>,
    )>,

    pub(super) actor_manager: Arc<StreamActorManager>,

    pub(super) actor_manager_state: StreamActorManagerState,
}

impl LocalBarrierWorker {
    pub(super) fn new(actor_manager: Arc<StreamActorManager>) -> Self {
        Self {
            barrier_senders: HashMap::new(),
            state: ManagedBarrierState::new(
                actor_manager.env.state_store(),
                actor_manager.streaming_metrics.clone(),
            ),
            control_stream_handle: None,
            actor_manager,
            actor_manager_state: StreamActorManagerState::new(),
        }
    }

    async fn run(mut self, mut event_rx: UnboundedReceiver<LocalBarrierEvent>) {
        loop {
            select! {
                (sender, create_actors_result) = self.actor_manager_state.next_created_actors() => {
                    self.handle_actor_created(sender, create_actors_result);
                }
                completed_epoch = self.state.next_completed_epoch() => {
                    let result = self.on_epoch_completed(completed_epoch);
                    self.inspect_result(result);
                },
                result = async {
                    if let Some((_, stream)) = &mut self.control_stream_handle {
                        stream.try_next().await.and_then(|opt| opt.ok_or_else(|| Status::internal("end of stream")))
                    } else {
                        pending().await
                    }
                } => {
                    match result {
                        Ok(request) => {
                            let result = self.handle_streaming_control_request(request);
                            self.inspect_result(result);
                        },
                        Err(e) => {
                            self.reset_stream_with_err(Status::internal(format!("failed to receive request: {:?}", e.as_report())));
                        }
                    }
                },
                event = event_rx.recv() => {
                    if let Some(event) = event {
                        match event {
                            LocalBarrierEvent::NewControlStream { sender, request_stream  } => {
                                self.reset_stream_with_err(Status::internal("control stream has been reset to a new one"));
                                self.reset().await;
                                self.control_stream_handle = Some((sender, request_stream));
                                self.send_response(StreamingControlStreamResponse {
                                    response: Some(streaming_control_stream_response::Response::Init(InitResponse {}))
                                });
                            }
                            event => {
                                self.handle_event(event);
                            }
                        }
                    }
                    else {
                        break;
                    }
                }
            }
        }
    }

    fn handle_actor_created(
        &mut self,
        sender: oneshot::Sender<StreamResult<()>>,
        create_actor_result: StreamResult<Vec<Actor<DispatchExecutor>>>,
    ) {
        let result = create_actor_result.map(|actors| {
            self.spawn_actors(actors);
        });

        let _ = sender.send(result);
    }

    fn reset_stream_with_err(&mut self, err: Status) {
        let (sender, _) = self
            .control_stream_handle
            .take()
            .expect("should not be empty when called");
        warn!("control stream reset with: {:?}", err.as_report());
        if sender.send(Err(err)).is_err() {
            warn!("failed to notify finish of control stream");
        }
    }

    fn inspect_result(&mut self, result: StreamResult<()>) {
        if let Err(e) = result {
            self.reset_stream_with_err(Status::internal(format!("get error: {:?}", e.as_report())));
        }
    }

    fn send_response(&mut self, response: StreamingControlStreamResponse) {
        let (sender, _) = self
            .control_stream_handle
            .as_ref()
            .expect("should not be None");
        if sender.send(Ok(response)).is_err() {
            self.control_stream_handle = None;
            warn!("fail to send response. control stream reset");
        }
    }

    fn handle_streaming_control_request(
        &mut self,
        request: StreamingControlStreamRequest,
    ) -> StreamResult<()> {
        match request.request.expect("should not be empty") {
            Request::InjectBarrier(req) => {
                let barrier = Barrier::from_protobuf(req.get_barrier().unwrap())?;
                self.send_barrier(
                    &barrier,
                    req.actor_ids_to_send.into_iter().collect(),
                    req.actor_ids_to_collect.into_iter().collect(),
                )?;
                Ok(())
            }
            Request::Init(_) => {
                unreachable!()
            }
        }
    }

    fn handle_event(&mut self, event: LocalBarrierEvent) {
        match event {
            LocalBarrierEvent::RegisterSender { actor_id, sender } => {
                self.register_sender(actor_id, sender);
            }
            LocalBarrierEvent::NewControlStream { .. } => {
                unreachable!("NewControlStream event should be handled separately in async context")
            }
            ReportActorCollected { actor_id, barrier } => self.collect(actor_id, &barrier),
            ReportActorFailure { actor_id, err } => {
                self.notify_failure(actor_id, err);
            }
            LocalBarrierEvent::ReportCreateProgress {
                current_epoch,
                actor,
                state,
            } => {
                self.update_create_mview_progress(current_epoch, actor, state);
            }
            #[cfg(test)]
            LocalBarrierEvent::Flush(sender) => sender.send(()).unwrap(),
            LocalBarrierEvent::DropActors {
                actors,
                result_sender,
            } => {
                self.drop_actors(&actors);
                let _ = result_sender.send(());
            }
            LocalBarrierEvent::UpdateActors {
                actors,
                result_sender,
            } => {
                let result = self.update_actors(actors);
                let _ = result_sender.send(result);
            }
            LocalBarrierEvent::BuildActors {
                actors,
                result_sender,
            } => self.start_create_actors(&actors, result_sender),
        }
    }
}

// event handler
impl LocalBarrierWorker {
    fn on_epoch_completed(&mut self, epoch: u64) -> StreamResult<()> {
        let result = self
            .state
            .pop_completed_epoch(epoch)
            .expect("should exist")
            .expect("should have completed")?;

        let BarrierCompleteResult {
            create_mview_progress,
            sync_result,
        } = result;

        let (synced_sstables, table_watermarks) = sync_result
            .map(|sync_result| (sync_result.uncommitted_ssts, sync_result.table_watermarks))
            .unwrap_or_default();

        let result = StreamingControlStreamResponse {
            response: Some(
                streaming_control_stream_response::Response::CompleteBarrier(
                    BarrierCompleteResponse {
                        request_id: "todo".to_string(),
                        status: None,
                        create_mview_progress,
                        synced_sstables: synced_sstables
                            .into_iter()
                            .map(
                                |LocalSstableInfo {
                                     compaction_group_id,
                                     sst_info,
                                     table_stats,
                                 }| GroupedSstableInfo {
                                    compaction_group_id,
                                    sst: Some(sst_info),
                                    table_stats_map: to_prost_table_stats_map(table_stats),
                                },
                            )
                            .collect_vec(),
                        worker_id: self.actor_manager.env.worker_id(),
                        table_watermarks: table_watermarks
                            .into_iter()
                            .map(|(key, value)| (key.table_id, value.to_protobuf()))
                            .collect(),
                    },
                ),
            ),
        };

        self.send_response(result);
        Ok(())
    }

    /// Register sender for source actors, used to send barriers.
    fn register_sender(&mut self, actor_id: ActorId, sender: UnboundedSender<Barrier>) {
        tracing::debug!(
            target: "events::stream::barrier::manager",
            actor_id = actor_id,
            "register sender"
        );
        self.barrier_senders
            .entry(actor_id)
            .or_default()
            .push(sender);
    }

    /// Broadcast a barrier to all senders. Save a receiver which will get notified when this
    /// barrier is finished, in managed mode.
    fn send_barrier(
        &mut self,
        barrier: &Barrier,
        to_send: HashSet<ActorId>,
        to_collect: HashSet<ActorId>,
    ) -> StreamResult<()> {
        #[cfg(not(test))]
        {
            // The barrier might be outdated and been injected after recovery in some certain extreme
            // scenarios. So some newly creating actors in the barrier are possibly not rebuilt during
            // recovery. Check it here and return an error here if some actors are not found to
            // avoid collection hang. We need some refine in meta side to remove this workaround since
            // it will cause another round of unnecessary recovery.
            let missing_actor_ids = to_collect
                .iter()
                .filter(|id| !self.actor_manager_state.handles.contains_key(id))
                .collect_vec();
            if !missing_actor_ids.is_empty() {
                panic!(
                    "to collect actors not found, they should be cleaned when recovering: {:?}",
                    missing_actor_ids
                );
            }
        }

        if barrier.kind == BarrierKind::Initial {
            self.actor_manager
                .watermark_epoch
                .store(barrier.epoch.curr, std::sync::atomic::Ordering::SeqCst);
        }
        debug!(
            target: "events::stream::barrier::manager::send",
            "send barrier {:?}, senders = {:?}, actor_ids_to_collect = {:?}",
            barrier,
            to_send,
            to_collect
        );

        // There must be some actors to collect from.
        assert!(!to_collect.is_empty());

        self.state.transform_to_issued(barrier, to_collect);

        for actor_id in to_send {
            match self.barrier_senders.get(&actor_id) {
                Some(senders) => {
                    for sender in senders {
                        if let Err(_err) = sender.send(barrier.clone()) {
                            // return err to trigger recovery.
                            return Err(StreamError::barrier_send(
                                barrier.clone(),
                                actor_id,
                                "channel closed",
                            ));
                        }
                    }
                }
                None => {
                    return Err(StreamError::barrier_send(
                        barrier.clone(),
                        actor_id,
                        "sender not found",
                    ));
                }
            }
        }

        // Actors to stop should still accept this barrier, but won't get sent to in next times.
        if let Some(actors) = barrier.all_stop_actors() {
            debug!(
                target: "events::stream::barrier::manager",
                "remove actors {:?} from senders",
                actors
            );
            for actor in actors {
                self.barrier_senders.remove(actor);
            }
        }
        Ok(())
    }

    /// Reset all internal states.
    pub(super) fn reset_state(&mut self) {
        *self = Self::new(self.actor_manager.clone());
    }

    /// When a [`crate::executor::StreamConsumer`] (typically [`crate::executor::DispatchExecutor`]) get a barrier, it should report
    /// and collect this barrier with its own `actor_id` using this function.
    fn collect(&mut self, actor_id: ActorId, barrier: &Barrier) {
        self.state.collect(actor_id, barrier)
    }

    /// When a actor exit unexpectedly, it should report this event using this function, so meta
    /// will notice actor's exit while collecting.
    fn notify_failure(&mut self, actor_id: ActorId, err: StreamError) {
        let err = err.into_unexpected_exit(actor_id);
        self.inspect_result(Err(err));
    }
}

#[derive(Clone)]
pub struct LocalBarrierManager {
    barrier_event_sender: UnboundedSender<LocalBarrierEvent>,
}

impl LocalBarrierManager {
    /// Create a [`LocalBarrierWorker`] with managed mode.
    pub fn new(
        context: Arc<SharedContext>,
        env: StreamEnvironment,
        streaming_metrics: Arc<StreamingMetrics>,
        await_tree_reg: Option<Arc<Mutex<await_tree::Registry<ActorId>>>>,
        watermark_epoch: AtomicU64Ref,
    ) -> Self {
        let runtime = {
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            if let Some(worker_threads_num) = context.config.actor_runtime_worker_threads_num {
                builder.worker_threads(worker_threads_num);
            }
            builder
                .thread_name("rw-streaming")
                .enable_all()
                .build()
                .unwrap()
        };

        let (tx, rx) = unbounded_channel();
        let local_barrier_manager = Self {
            barrier_event_sender: tx,
        };
        let actor_manager = Arc::new(StreamActorManager {
            context: context.clone(),
            env: env.clone(),
            streaming_metrics,
            watermark_epoch,
            local_barrier_manager: local_barrier_manager.clone(),
            await_tree_reg,
            runtime: runtime.into(),
        });
        let worker = LocalBarrierWorker::new(actor_manager);
        let _join_handle = tokio::spawn(worker.run(rx));
        local_barrier_manager
    }

    pub(super) fn send_event(&self, event: LocalBarrierEvent) {
        self.barrier_event_sender
            .send(event)
            .expect("should be able to send event")
    }

    pub(super) async fn send_and_await<RSP>(
        &self,
        make_event: impl FnOnce(oneshot::Sender<RSP>) -> LocalBarrierEvent,
    ) -> StreamResult<RSP> {
        let (tx, rx) = oneshot::channel();
        let event = make_event(tx);
        self.send_event(event);
        rx.await
            .map_err(|_| anyhow!("barrier manager maybe reset").into())
    }
}

impl LocalBarrierManager {
    /// Register sender for source actors, used to send barriers.
    pub fn register_sender(&self, actor_id: ActorId, sender: UnboundedSender<Barrier>) {
        self.send_event(LocalBarrierEvent::RegisterSender { actor_id, sender });
    }

    /// Broadcast a barrier to all senders. Save a receiver which will get notified when this
    /// barrier is finished, in managed mode.
    pub async fn send_barrier(
        &self,
        _barrier: Barrier,
        _actor_ids_to_send: impl IntoIterator<Item = ActorId>,
        _actor_ids_to_collect: impl IntoIterator<Item = ActorId>,
    ) -> StreamResult<()> {
        // self.send_and_await(move |result_sender| LocalBarrierEvent::InjectBarrier {
        //     barrier,
        //     actor_ids_to_send: actor_ids_to_send.into_iter().collect(),
        //     actor_ids_to_collect: actor_ids_to_collect.into_iter().collect(),
        //     result_sender,
        // })
        // .await?
        todo!()
    }

    /// Use `prev_epoch` to remove collect rx and return rx.
    pub async fn await_epoch_completed(
        &self,
        _prev_epoch: u64,
    ) -> StreamResult<BarrierCompleteResult> {
        // self.send_and_await(|result_sender| LocalBarrierEvent::AwaitEpochCompleted {
        //     epoch: prev_epoch,
        //     result_sender,
        // })
        // .await?
        todo!()
    }

    /// When a [`crate::executor::StreamConsumer`] (typically [`crate::executor::DispatchExecutor`]) get a barrier, it should report
    /// and collect this barrier with its own `actor_id` using this function.
    pub fn collect(&self, actor_id: ActorId, barrier: &Barrier) {
        self.send_event(ReportActorCollected {
            actor_id,
            barrier: barrier.clone(),
        })
    }

    /// When a actor exit unexpectedly, it should report this event using this function, so meta
    /// will notice actor's exit while collecting.
    pub fn notify_failure(&self, actor_id: ActorId, err: StreamError) {
        self.send_event(ReportActorFailure { actor_id, err })
    }
}

#[cfg(test)]
impl LocalBarrierManager {
    pub fn for_test() -> Self {
        use std::sync::atomic::AtomicU64;
        Self::new(
            Arc::new(SharedContext::for_test()),
            StreamEnvironment::for_test(),
            Arc::new(StreamingMetrics::unused()),
            None,
            Arc::new(AtomicU64::new(0)),
        )
    }

    pub async fn flush_all_events(&self) {
        let (tx, rx) = oneshot::channel();
        self.send_event(LocalBarrierEvent::Flush(tx));
        rx.await.unwrap()
    }
}
