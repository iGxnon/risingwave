// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ops::Bound;
use std::pin::Pin;

use futures::{pin_mut, FutureExt, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use pin_project_lite::pin_project;
use risingwave_common::error::Result as RwResult;
use risingwave_common::util::addr::HostAddr;
use risingwave_common_service::observer_manager::{Channel, NotificationClient};
use risingwave_hummock_trace::{
    GlobalReplay, LocalReplay, LocalReplayRead, ReplayItem, ReplayItemStream, ReplayRead,
    ReplayStateStore, ReplayWrite, Result, TraceError, TraceSubResp, TracedBytes,
    TracedNewLocalOptions, TracedReadOptions,
};
use risingwave_meta::manager::{MessageStatus, MetaSrvEnv, NotificationManagerRef, WorkerKey};
use risingwave_meta::storage::{MemStore, MetaStore};
use risingwave_pb::common::WorkerNode;
use risingwave_pb::meta::subscribe_response::{Info, Operation as RespOperation};
use risingwave_pb::meta::{SubscribeResponse, SubscribeType};
use risingwave_storage::hummock::store::state_store::LocalHummockStorage;
use risingwave_storage::hummock::HummockStorage;
use risingwave_storage::store::{
    LocalStateStore, StateStoreIterItemStream, StateStoreRead, SyncResult,
};
use risingwave_storage::{StateStore, StateStoreIterItem, StateStoreReadIterStream};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
pub(crate) struct GlobalReplayIter<S>
where
    S: StateStoreReadIterStream,
{
    inner: S,
}

impl<S> GlobalReplayIter<S>
where
    S: StateStoreReadIterStream,
{
    pub(crate) fn new(inner: S) -> Self {
        Self { inner }
    }

    #[try_stream(ok = (TracedBytes, TracedBytes), error = TraceError)]
    pub(crate) async fn into_stream(self) {
        let inner = self.inner;
        pin_mut!(inner);
        while let Ok(Some((key, value))) = inner.try_next().await {
            yield (key.user_key.table_key.0.into(), value.into())
        }
    }
}

pub(crate) struct LocalReplayIter<S>
where
    S: StateStoreIterItemStream,
{
    inner: S,
}

impl<S> LocalReplayIter<S>
where
    S: StateStoreIterItemStream,
{
    pub(crate) fn new(inner: S) -> Self {
        Self { inner }
    }

    #[try_stream(ok = (TracedBytes, TracedBytes), error = TraceError)]
    pub(crate) async fn into_stream(self) {
        let inner = self.inner;
        pin_mut!(inner);
        while let Ok(Some((key, value))) = inner.try_next().await {
            yield (key.user_key.table_key.0.into(), value.into())
        }
    }
}

pub(crate) struct GlobalReplayInterface {
    store: HummockStorage,
    notifier: NotificationManagerRef<MemStore>,
}

impl GlobalReplayInterface {
    pub(crate) fn new(store: HummockStorage, notifier: NotificationManagerRef<MemStore>) -> Self {
        Self { store, notifier }
    }
}

impl GlobalReplay for GlobalReplayInterface {}

#[async_trait::async_trait]
impl ReplayRead for GlobalReplayInterface {
    async fn iter(
        &self,
        key_range: (Bound<TracedBytes>, Bound<TracedBytes>),
        epoch: u64,
        read_options: TracedReadOptions,
    ) -> Result<Pin<Box<dyn ReplayItemStream>>> {
        let key_range = (
            key_range.0.map(|b| b.into_bytes()),
            key_range.1.map(|b| b.into_bytes()),
        );

        let iter = self
            .store
            .iter(key_range, epoch, read_options.into())
            .await
            .unwrap();
        let iter = iter.boxed();

        Ok(Box::pin(HummockReplayIter::new(iter).into_stream()))
    }

    async fn get(
        &self,
        key: TracedBytes,
        epoch: u64,
        read_options: TracedReadOptions,
    ) -> Result<Option<TracedBytes>> {
        Ok(self
            .store
            .get(key.into_bytes(), epoch, read_options.into())
            .await
            .unwrap()
            .map(TracedBytes::from))
    }
}

#[async_trait::async_trait]
impl ReplayStateStore for GlobalReplayInterface {
    async fn sync(&self, id: u64) -> Result<usize> {
        let result: SyncResult = self
            .store
            .sync(id)
            .await
            .map_err(|e| TraceError::SyncFailed(format!("{e}")))?;
        Ok(result.sync_size)
    }

    async fn seal_epoch(&self, epoch_id: u64, is_checkpoint: bool) {
        self.store.seal_epoch(epoch_id, is_checkpoint);
    }

    async fn notify_hummock(
        &self,
        info: Info,
        op: RespOperation,
        version: NotificationVersion,
    ) -> Result<u64> {
        let prev_version_id = match &info {
            Info::HummockVersionDeltas(deltas) => deltas.version_deltas.last().map(|d| d.prev_id),
            _ => None,
        };

        self.notifier
            .notify_hummock_with_version(op, info, Some(version));

        // wait till version updated
        if let Some(prev_version_id) = prev_version_id {
            self.store.wait_version_update(prev_version_id).await;
        }
        Ok(version)
    }

    async fn new_local(&self, options: TracedNewLocalOptions) -> Box<dyn LocalReplay> {
        let local_storage = self.store.new_local(options).await;
        Box::new(LocalReplayInterface(local_storage))
    }
}
pub(crate) struct LocalReplayInterface(LocalHummockStorage);

impl LocalReplay for LocalReplayInterface {}

#[async_trait::async_trait]
impl LocalReplayRead for LocalReplayInterface {
    async fn iter(
        &self,
        key_range: (Bound<TracedBytes>, Bound<TracedBytes>),
        read_options: TracedReadOptions,
    ) -> Result<Box<dyn ReplayItemStream>> {
        let key_range = (
            key_range.0.map(|b| b.into_bytes()),
            key_range.1.map(|b| b.into_bytes()),
        );

        let iter = LocalStateStore::iter(&self.0, key_range, read_options.into())
            .await
            .unwrap();

        let iter = iter.boxed();

        Ok(Box::pin(HummockReplayIter::new(iter).into_stream()))
    }

    async fn get(
        &self,
        key: TracedBytes,
        read_options: TracedReadOptions,
    ) -> Result<Option<TracedBytes>> {
        Ok(
            LocalStateStore::get(&self.0, key.into_bytes(), read_options.into())
                .await
                .unwrap()
                .map(TracedBytes::from),
        )
    }
}

#[async_trait::async_trait]
impl ReplayWrite for LocalReplayInterface {
    async fn insert(
        &mut self,
        key: TracedBytes,
        new_val: TracedBytes,
        old_val: Option<TracedBytes>,
    ) -> Result<()> {
        Ok(LocalStateStore::insert(
            &mut self.0,
            key.into_bytes(),
            new_val.into_bytes(),
            old_val.map(|b| b.into_bytes()),
        )
        .unwrap())
    }

    async fn delete(&mut self, key: TracedBytes, old_val: TracedBytes) -> Result<()> {
        Ok(LocalStateStore::delete(&mut self.0, key.into_bytes(), old_val.into_bytes()).unwrap())
    }
}

pub struct ReplayNotificationClient<S: MetaStore> {
    addr: HostAddr,
    notification_manager: NotificationManagerRef<S>,
    first_resp: Box<TraceSubResp>,
}

impl<S: MetaStore> ReplayNotificationClient<S> {
    pub fn new(
        addr: HostAddr,
        notification_manager: NotificationManagerRef<S>,
        first_resp: Box<TraceSubResp>,
    ) -> Self {
        Self {
            addr,
            notification_manager,
            first_resp,
        }
    }
}

#[async_trait::async_trait]
impl<S: MetaStore> NotificationClient for ReplayNotificationClient<S> {
    type Channel = ReplayChannel<SubscribeResponse>;

    async fn subscribe(&self, subscribe_type: SubscribeType) -> RwResult<Self::Channel> {
        let (tx, rx) = unbounded_channel();

        self.notification_manager
            .insert_sender(subscribe_type, WorkerKey(self.addr.to_protobuf()), tx)
            .await;

        // send the first snapshot message
        let op = self.first_resp.0.operation();
        let info = self.first_resp.0.info.clone();

        self.notification_manager
            .notify_hummock(op, info.unwrap())
            .await;

        Ok(ReplayChannel(rx))
    }
}

pub fn get_replay_notification_client(
    env: MetaSrvEnv<MemStore>,
    worker_node: WorkerNode,
    first_resp: Box<TraceSubResp>,
) -> ReplayNotificationClient<MemStore> {
    ReplayNotificationClient::new(
        worker_node.get_host().unwrap().into(),
        env.notification_manager_ref(),
        first_resp,
    )
}

pub struct ReplayChannel<T>(UnboundedReceiver<std::result::Result<T, MessageStatus>>);

#[async_trait::async_trait]
impl<T: Send + 'static> Channel for ReplayChannel<T> {
    type Item = T;

    async fn message(&mut self) -> std::result::Result<Option<T>, MessageStatus> {
        match self.0.recv().await {
            None => Ok(None),
            Some(result) => result.map(|r| Some(r)),
        }
    }
}
