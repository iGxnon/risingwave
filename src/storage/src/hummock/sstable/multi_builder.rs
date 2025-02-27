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

use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;

use bytes::Bytes;
use num_integer::Integer;
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::epoch::is_max_epoch;
use risingwave_hummock_sdk::key::{FullKey, PointRange, UserKey};
use risingwave_hummock_sdk::{HummockEpoch, LocalSstableInfo};
use tokio::task::JoinHandle;

use super::MonotonicDeleteEvent;
use crate::hummock::compactor::task_progress::TaskProgress;
use crate::hummock::sstable::filter::FilterBuilder;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::value::HummockValue;
use crate::hummock::{
    BatchUploadWriter, BlockMeta, CachePolicy, HummockResult, MemoryLimiter, SstableBuilder,
    SstableBuilderOptions, SstableWriter, SstableWriterOptions, Xor16FilterBuilder,
};
use crate::monitor::CompactorMetrics;

pub type UploadJoinHandle = JoinHandle<HummockResult<()>>;

#[async_trait::async_trait]
pub trait TableBuilderFactory {
    type Writer: SstableWriter<Output = UploadJoinHandle>;
    type Filter: FilterBuilder;
    async fn open_builder(&mut self) -> HummockResult<SstableBuilder<Self::Writer, Self::Filter>>;
}

pub struct SplitTableOutput {
    pub sst_info: LocalSstableInfo,
    pub upload_join_handle: UploadJoinHandle,
}

/// A wrapper for [`SstableBuilder`] which automatically split key-value pairs into multiple tables,
/// based on their target capacity set in options.
///
/// When building is finished, one may call `finish` to get the results of zero, one or more tables.
pub struct CapacitySplitTableBuilder<F>
where
    F: TableBuilderFactory,
{
    /// When creating a new [`SstableBuilder`], caller use this factory to generate it.
    builder_factory: F,

    sst_outputs: Vec<SplitTableOutput>,

    current_builder: Option<SstableBuilder<F::Writer, F::Filter>>,

    /// Statistics.
    pub compactor_metrics: Arc<CompactorMetrics>,

    /// Update the number of sealed Sstables.
    task_progress: Option<Arc<TaskProgress>>,

    last_table_id: u32,
    table_partition_vnode: BTreeMap<u32, u32>,
    split_weight_by_vnode: u32,
    /// When vnode of the coming key is greater than `largest_vnode_in_current_partition`, we will
    /// switch SST.
    largest_vnode_in_current_partition: usize,
}

impl<F> CapacitySplitTableBuilder<F>
where
    F: TableBuilderFactory,
{
    /// Creates a new [`CapacitySplitTableBuilder`] using given configuration generator.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        builder_factory: F,
        compactor_metrics: Arc<CompactorMetrics>,
        task_progress: Option<Arc<TaskProgress>>,
        table_partition_vnode: BTreeMap<u32, u32>,
    ) -> Self {
        Self {
            builder_factory,
            sst_outputs: Vec::new(),
            current_builder: None,
            compactor_metrics,
            task_progress,
            last_table_id: 0,
            table_partition_vnode,
            split_weight_by_vnode: 0,
            largest_vnode_in_current_partition: VirtualNode::MAX.to_index(),
        }
    }

    pub fn for_test(builder_factory: F) -> Self {
        Self {
            builder_factory,
            sst_outputs: Vec::new(),
            current_builder: None,
            compactor_metrics: Arc::new(CompactorMetrics::unused()),
            task_progress: None,
            last_table_id: 0,
            table_partition_vnode: BTreeMap::default(),
            split_weight_by_vnode: 0,
            largest_vnode_in_current_partition: VirtualNode::MAX.to_index(),
        }
    }

    /// Returns the number of [`SstableBuilder`]s.
    pub fn len(&self) -> usize {
        self.sst_outputs.len() + self.current_builder.is_some() as usize
    }

    /// Returns true if no builder is created.
    pub fn is_empty(&self) -> bool {
        self.sst_outputs.is_empty() && self.current_builder.is_none()
    }

    pub async fn add_full_key_for_test(
        &mut self,
        full_key: FullKey<&[u8]>,
        value: HummockValue<&[u8]>,
        is_new_user_key: bool,
    ) -> HummockResult<()> {
        self.add_full_key(full_key, value, is_new_user_key).await
    }

    pub async fn add_raw_block(
        &mut self,
        buf: Bytes,
        filter_data: Vec<u8>,
        smallest_key: FullKey<Vec<u8>>,
        largest_key: Vec<u8>,
        block_meta: BlockMeta,
    ) -> HummockResult<bool> {
        if self.current_builder.is_none() {
            if let Some(progress) = &self.task_progress {
                progress.inc_num_pending_write_io()
            }
            let builder = self.builder_factory.open_builder().await?;
            self.current_builder = Some(builder);
        }

        let builder = self.current_builder.as_mut().unwrap();
        builder
            .add_raw_block(buf, filter_data, smallest_key, largest_key, block_meta)
            .await
    }

    /// Adds a key-value pair to the underlying builders.
    ///
    /// If `allow_split` and the current builder reaches its capacity, this function will create a
    /// new one with the configuration generated by the closure provided earlier.
    ///
    /// Note that in some cases like compaction of the same user key, automatic splitting is not
    /// allowed, where `allow_split` should be `false`.
    pub async fn add_full_key(
        &mut self,
        full_key: FullKey<&[u8]>,
        value: HummockValue<&[u8]>,
        is_new_user_key: bool,
    ) -> HummockResult<()> {
        let switch_builder = self.check_switch_builder(&full_key.user_key);

        // We use this `need_seal_current` flag to store whether we need to call `seal_current` and
        // then call `seal_current` later outside the `if let` instead of calling
        // `seal_current` at where we set `need_seal_current = true`. This is because
        // `seal_current` is an async method, and if we call `seal_current` within the `if let`,
        // this temporary reference to `current_builder` will be captured in the future generated
        // from the current method. Since this generated future is usually required to be `Send`,
        // the captured reference to `current_builder` is also required to be `Send`, and then
        // `current_builder` itself is required to be `Sync`, which is unnecessary.
        let mut need_seal_current = false;
        let mut last_range_tombstone_epoch = HummockEpoch::MAX;
        if let Some(builder) = self.current_builder.as_mut() {
            if is_new_user_key {
                need_seal_current = switch_builder || builder.reach_capacity();
            }
            if need_seal_current
                && let Some(event) = builder.last_range_tombstone()
                && !is_max_epoch(event.new_epoch)
            {
                last_range_tombstone_epoch = event.new_epoch;
                if event
                    .event_key
                    .left_user_key
                    .as_ref()
                    .eq(&full_key.user_key)
                {
                    // If the last range tombstone equals the new key, we can not create new file because we must keep the new key in origin file.
                    need_seal_current = false;
                } else {
                    builder.add_monotonic_delete(MonotonicDeleteEvent {
                        event_key: PointRange::from_user_key(full_key.user_key.to_vec(), false),
                        new_epoch: HummockEpoch::MAX,
                    });
                }
            }
        }

        if need_seal_current {
            self.seal_current().await?;
        }

        if self.current_builder.is_none() {
            if let Some(progress) = &self.task_progress {
                progress.inc_num_pending_write_io();
            }
            let mut builder = self.builder_factory.open_builder().await?;
            // If last_range_tombstone_epoch is not MAX, it means that we cut one range-tombstone to
            // two half and add the right half as a new range to next sstable.
            if need_seal_current && !is_max_epoch(last_range_tombstone_epoch) {
                builder.add_monotonic_delete(MonotonicDeleteEvent {
                    event_key: PointRange::from_user_key(full_key.user_key.to_vec(), false),
                    new_epoch: last_range_tombstone_epoch,
                });
            }
            self.current_builder = Some(builder);
        }

        let builder = self.current_builder.as_mut().unwrap();
        builder.add(full_key, value).await
    }

    pub fn check_switch_builder(&mut self, user_key: &UserKey<&[u8]>) -> bool {
        let mut switch_builder = false;
        if user_key.table_id.table_id != self.last_table_id {
            let new_vnode_partition_count =
                self.table_partition_vnode.get(&user_key.table_id.table_id);

            if new_vnode_partition_count.is_some()
                || self.table_partition_vnode.contains_key(&self.last_table_id)
            {
                if new_vnode_partition_count.is_some() {
                    self.split_weight_by_vnode = *new_vnode_partition_count.unwrap();
                } else {
                    self.split_weight_by_vnode = 0;
                }

                // table_id change
                self.last_table_id = user_key.table_id.table_id;
                switch_builder = true;
                if self.split_weight_by_vnode > 1 {
                    self.largest_vnode_in_current_partition =
                        VirtualNode::COUNT / (self.split_weight_by_vnode as usize) - 1;
                } else {
                    // default
                    self.largest_vnode_in_current_partition = VirtualNode::MAX.to_index();
                }
            }
        }
        if self.largest_vnode_in_current_partition != VirtualNode::MAX.to_index() {
            let key_vnode = user_key.get_vnode_id();
            if key_vnode > self.largest_vnode_in_current_partition {
                // vnode partition change
                switch_builder = true;

                // SAFETY: `self.split_weight_by_vnode > 1` here.
                let (basic, remainder) =
                    VirtualNode::COUNT.div_rem(&(self.split_weight_by_vnode as usize));
                let small_segments_area = basic * (self.split_weight_by_vnode as usize - remainder);
                self.largest_vnode_in_current_partition = (if key_vnode < small_segments_area {
                    (key_vnode / basic + 1) * basic
                } else {
                    ((key_vnode - small_segments_area) / (basic + 1) + 1) * (basic + 1)
                        + small_segments_area
                }) - 1;
                debug_assert!(key_vnode <= self.largest_vnode_in_current_partition);
            }
        }
        switch_builder
    }

    pub fn need_flush(&self) -> bool {
        self.current_builder
            .as_ref()
            .map(|builder| builder.reach_capacity())
            .unwrap_or(false)
    }

    /// Add kv pair to sstable.
    pub async fn add_monotonic_delete(&mut self, event: MonotonicDeleteEvent) -> HummockResult<()> {
        if let Some(builder) = self.current_builder.as_mut()
            && builder.reach_capacity()
            && !is_max_epoch(event.new_epoch)
        {
            if !is_max_epoch(builder.last_range_tombstone_epoch()) {
                builder.add_monotonic_delete(MonotonicDeleteEvent {
                    event_key: event.event_key.clone(),
                    new_epoch: HummockEpoch::MAX,
                });
            }
            self.seal_current().await?;
        }

        if self.current_builder.is_none() {
            if is_max_epoch(event.new_epoch) {
                return Ok(());
            }

            if let Some(progress) = &self.task_progress {
                progress.inc_num_pending_write_io();
            }
            let builder = self.builder_factory.open_builder().await?;
            self.current_builder = Some(builder);
        }
        let builder = self.current_builder.as_mut().unwrap();
        builder.add_monotonic_delete(event);
        Ok(())
    }

    /// Marks the current builder as sealed. Next call of `add` will always create a new table.
    ///
    /// If there's no builder created, or current one is already sealed before, then this function
    /// will be no-op.
    pub async fn seal_current(&mut self) -> HummockResult<()> {
        if let Some(builder) = self.current_builder.take() {
            let builder_output = builder.finish().await?;
            {
                // report
                if let Some(progress) = &self.task_progress {
                    progress.inc_ssts_sealed();
                }

                if builder_output.bloom_filter_size != 0 {
                    self.compactor_metrics
                        .sstable_bloom_filter_size
                        .observe(builder_output.bloom_filter_size as _);
                }

                if builder_output.sst_info.file_size() != 0 {
                    self.compactor_metrics
                        .sstable_file_size
                        .observe(builder_output.sst_info.file_size() as _);
                }

                if builder_output.avg_key_size != 0 {
                    self.compactor_metrics
                        .sstable_avg_key_size
                        .observe(builder_output.avg_key_size as _);
                }

                if builder_output.avg_value_size != 0 {
                    self.compactor_metrics
                        .sstable_avg_value_size
                        .observe(builder_output.avg_value_size as _);
                }

                if builder_output.epoch_count != 0 {
                    self.compactor_metrics
                        .sstable_distinct_epoch_count
                        .observe(builder_output.epoch_count as _);
                }
            }
            self.sst_outputs.push(SplitTableOutput {
                upload_join_handle: builder_output.writer_output,
                sst_info: builder_output.sst_info,
            });
        }
        Ok(())
    }

    /// Finalizes all the tables to be ids, blocks and metadata.
    pub async fn finish(mut self) -> HummockResult<Vec<SplitTableOutput>> {
        self.seal_current().await?;
        Ok(self.sst_outputs)
    }
}

/// Used for unit tests and benchmarks.
pub struct LocalTableBuilderFactory {
    next_id: AtomicU64,
    sstable_store: SstableStoreRef,
    options: SstableBuilderOptions,
    policy: CachePolicy,
    limiter: MemoryLimiter,
}

impl LocalTableBuilderFactory {
    pub fn new(
        next_id: u64,
        sstable_store: SstableStoreRef,
        options: SstableBuilderOptions,
    ) -> Self {
        Self {
            next_id: AtomicU64::new(next_id),
            sstable_store,
            options,
            policy: CachePolicy::NotFill,
            limiter: MemoryLimiter::new(1000000),
        }
    }
}

#[async_trait::async_trait]
impl TableBuilderFactory for LocalTableBuilderFactory {
    type Filter = Xor16FilterBuilder;
    type Writer = BatchUploadWriter;

    async fn open_builder(
        &mut self,
    ) -> HummockResult<SstableBuilder<BatchUploadWriter, Xor16FilterBuilder>> {
        let id = self.next_id.fetch_add(1, SeqCst);
        let tracker = self.limiter.require_memory(1).await;
        let writer_options = SstableWriterOptions {
            capacity_hint: Some(self.options.capacity),
            tracker: Some(tracker),
            policy: self.policy,
        };
        let writer = self
            .sstable_store
            .clone()
            .create_sst_writer(id, writer_options);
        let builder = SstableBuilder::for_test(id, writer, self.options.clone());

        Ok(builder)
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use itertools::Itertools;
    use risingwave_common::catalog::TableId;
    use risingwave_common::hash::VirtualNode;
    use risingwave_hummock_sdk::can_concat;
    use risingwave_hummock_sdk::key::PointRange;

    use super::*;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::test_utils::delete_range::CompactionDeleteRangesBuilder;
    use crate::hummock::test_utils::{default_builder_opt_for_test, test_key_of, test_user_key_of};
    use crate::hummock::{SstableBuilderOptions, DEFAULT_RESTART_INTERVAL};

    #[tokio::test]
    async fn test_empty() {
        let block_size = 1 << 10;
        let table_capacity = 4 * block_size;
        let opts = SstableBuilderOptions {
            capacity: table_capacity,
            block_capacity: block_size,
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: 0.1,
            ..Default::default()
        };
        let builder_factory = LocalTableBuilderFactory::new(1001, mock_sstable_store(), opts);
        let builder = CapacitySplitTableBuilder::for_test(builder_factory);
        let results = builder.finish().await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_lots_of_tables() {
        let block_size = 1 << 10;
        let table_capacity = 4 * block_size;
        let opts = SstableBuilderOptions {
            capacity: table_capacity,
            block_capacity: block_size,
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: 0.1,
            ..Default::default()
        };
        let builder_factory = LocalTableBuilderFactory::new(1001, mock_sstable_store(), opts);
        let mut builder = CapacitySplitTableBuilder::for_test(builder_factory);

        for i in 0..table_capacity {
            builder
                .add_full_key_for_test(
                    FullKey::from_user_key(
                        test_user_key_of(i).as_ref(),
                        (table_capacity - i) as u64,
                    ),
                    HummockValue::put(b"value"),
                    true,
                )
                .await
                .unwrap();
        }

        let results = builder.finish().await.unwrap();
        assert!(results.len() > 1);
    }

    #[tokio::test]
    async fn test_table_seal() {
        let opts = default_builder_opt_for_test();
        let mut builder = CapacitySplitTableBuilder::for_test(LocalTableBuilderFactory::new(
            1001,
            mock_sstable_store(),
            opts,
        ));
        let mut epoch = 100;

        macro_rules! add {
            () => {
                epoch -= 1;
                builder
                    .add_full_key_for_test(
                        FullKey::from_user_key(test_user_key_of(1).as_ref(), epoch),
                        HummockValue::put(b"v"),
                        true,
                    )
                    .await
                    .unwrap();
            };
        }

        assert_eq!(builder.len(), 0);
        builder.seal_current().await.unwrap();
        assert_eq!(builder.len(), 0);
        add!();
        assert_eq!(builder.len(), 1);
        add!();
        assert_eq!(builder.len(), 1);
        builder.seal_current().await.unwrap();
        assert_eq!(builder.len(), 1);
        add!();
        assert_eq!(builder.len(), 2);
        builder.seal_current().await.unwrap();
        assert_eq!(builder.len(), 2);
        builder.seal_current().await.unwrap();
        assert_eq!(builder.len(), 2);

        let results = builder.finish().await.unwrap();
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_initial_not_allowed_split() {
        let opts = default_builder_opt_for_test();
        let mut builder = CapacitySplitTableBuilder::for_test(LocalTableBuilderFactory::new(
            1001,
            mock_sstable_store(),
            opts,
        ));
        builder
            .add_full_key_for_test(test_key_of(0).to_ref(), HummockValue::put(b"v"), false)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_expand_boundary_by_range_tombstone() {
        let opts = default_builder_opt_for_test();
        let table_id = TableId::default();
        let mut builder = CompactionDeleteRangesBuilder::default();
        builder.add_delete_events(
            100,
            table_id,
            vec![(
                Bound::Included(Bytes::copy_from_slice(
                    &[VirtualNode::ZERO.to_be_bytes().as_slice(), b"k"].concat(),
                )),
                Bound::Excluded(Bytes::copy_from_slice(
                    &[VirtualNode::ZERO.to_be_bytes().as_slice(), b"kkk"].concat(),
                )),
            )],
        );
        builder.add_delete_events(
            200,
            table_id,
            vec![(
                Bound::Included(Bytes::copy_from_slice(
                    &[VirtualNode::ZERO.to_be_bytes().as_slice(), b"aaa"].concat(),
                )),
                Bound::Excluded(Bytes::copy_from_slice(
                    &[VirtualNode::ZERO.to_be_bytes().as_slice(), b"ddd"].concat(),
                )),
            )],
        );
        let mut del_iter = builder.build_for_compaction();
        del_iter.rewind().await.unwrap();
        let mut builder = CapacitySplitTableBuilder::new(
            LocalTableBuilderFactory::new(1001, mock_sstable_store(), opts),
            Arc::new(CompactorMetrics::unused()),
            None,
            BTreeMap::default(),
        );
        let full_key = FullKey::for_test(
            table_id,
            [VirtualNode::ZERO.to_be_bytes().as_slice(), b"k"].concat(),
            233,
        );
        let target_extended_user_key = PointRange::from_user_key(full_key.user_key.as_ref(), false);
        while del_iter.is_valid() && del_iter.key().as_ref().le(&target_extended_user_key) {
            let event_key = del_iter.key().to_vec();
            del_iter.next().await.unwrap();
            builder
                .add_monotonic_delete(MonotonicDeleteEvent {
                    new_epoch: del_iter.earliest_epoch(),
                    event_key,
                })
                .await
                .unwrap();
        }
        builder
            .add_full_key(full_key.to_ref(), HummockValue::put(b"v"), false)
            .await
            .unwrap();
        while del_iter.is_valid() {
            let event_key = del_iter.key().to_vec();
            del_iter.next().await.unwrap();
            builder
                .add_monotonic_delete(MonotonicDeleteEvent {
                    event_key,
                    new_epoch: del_iter.earliest_epoch(),
                })
                .await
                .unwrap();
        }
        let mut sst_infos = builder.finish().await.unwrap();
        let key_range = sst_infos
            .pop()
            .unwrap()
            .sst_info
            .sst_info
            .key_range
            .unwrap();
        assert_eq!(
            key_range.left,
            FullKey::for_test(
                table_id,
                &[VirtualNode::ZERO.to_be_bytes().as_slice(), b"aaa"].concat(),
                HummockEpoch::MAX,
            )
            .encode()
        );
        assert_eq!(
            key_range.right,
            FullKey::for_test(
                table_id,
                &[VirtualNode::ZERO.to_be_bytes().as_slice(), b"kkk"].concat(),
                HummockEpoch::MAX
            )
            .encode()
        );
    }

    #[tokio::test]
    async fn test_only_delete_range() {
        let block_size = 1 << 10;
        let table_capacity = 4 * block_size;
        let opts = SstableBuilderOptions {
            capacity: table_capacity,
            block_capacity: block_size,
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: 0.1,
            ..Default::default()
        };
        let table_id = TableId::new(1);
        let mut builder = CompactionDeleteRangesBuilder::default();
        builder.add_delete_events(
            100,
            table_id,
            vec![(
                Bound::Included(Bytes::copy_from_slice(b"k")),
                (Bound::Excluded(Bytes::copy_from_slice(b"kkk"))),
            )],
        );
        builder.add_delete_events(
            200,
            table_id,
            vec![(
                Bound::Included(Bytes::copy_from_slice(b"aaa")),
                (Bound::Excluded(Bytes::copy_from_slice(b"ddd"))),
            )],
        );
        let mut del_iter = builder.build_for_compaction();
        let mut builder = CapacitySplitTableBuilder::new(
            LocalTableBuilderFactory::new(1001, mock_sstable_store(), opts),
            Arc::new(CompactorMetrics::unused()),
            None,
            BTreeMap::default(),
        );
        del_iter.rewind().await.unwrap();
        assert!(is_max_epoch(del_iter.earliest_epoch()));
        while del_iter.is_valid() {
            let event_key = del_iter.key().to_vec();
            del_iter.next().await.unwrap();
            builder
                .add_monotonic_delete(MonotonicDeleteEvent {
                    new_epoch: del_iter.earliest_epoch(),
                    event_key,
                })
                .await
                .unwrap();
        }

        let results = builder.finish().await.unwrap();
        assert_eq!(results[0].sst_info.sst_info.table_ids, vec![1]);
    }

    #[tokio::test]
    async fn test_delete_range_cut_sst() {
        let block_size = 256;
        let table_capacity = 2 * block_size;
        let opts = SstableBuilderOptions {
            capacity: table_capacity,
            block_capacity: block_size,
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: 0.1,
            ..Default::default()
        };
        let table_id = TableId::new(1);
        let mut builder = CapacitySplitTableBuilder::new(
            LocalTableBuilderFactory::new(1001, mock_sstable_store(), opts),
            Arc::new(CompactorMetrics::unused()),
            None,
            BTreeMap::default(),
        );
        builder
            .add_monotonic_delete(MonotonicDeleteEvent {
                event_key: PointRange::from_user_key(
                    UserKey::for_test(table_id, b"aaaa".to_vec()),
                    false,
                ),
                new_epoch: 10,
            })
            .await
            .unwrap();
        let v = vec![5u8; 220];
        let epoch = 12;
        builder
            .add_full_key(
                FullKey::from_user_key(UserKey::for_test(table_id, b"bbbb"), epoch),
                HummockValue::put(v.as_slice()),
                true,
            )
            .await
            .unwrap();
        builder
            .add_full_key(
                FullKey::from_user_key(UserKey::for_test(table_id, b"cccc"), epoch),
                HummockValue::put(v.as_slice()),
                true,
            )
            .await
            .unwrap();
        builder
            .add_monotonic_delete(MonotonicDeleteEvent {
                event_key: PointRange::from_user_key(
                    UserKey::for_test(table_id, b"eeee".to_vec()),
                    false,
                ),
                new_epoch: 11,
            })
            .await
            .unwrap();
        builder
            .add_monotonic_delete(MonotonicDeleteEvent {
                event_key: PointRange::from_user_key(
                    UserKey::for_test(table_id, b"ffff".to_vec()),
                    false,
                ),
                new_epoch: 10,
            })
            .await
            .unwrap();
        builder
            .add_full_key(
                FullKey::from_user_key(UserKey::for_test(table_id, b"ffff"), epoch),
                HummockValue::put(v.as_slice()),
                true,
            )
            .await
            .unwrap();
        builder
            .add_monotonic_delete(MonotonicDeleteEvent {
                event_key: PointRange::from_user_key(
                    UserKey::for_test(table_id, b"gggg".to_vec()),
                    false,
                ),
                new_epoch: HummockEpoch::MAX,
            })
            .await
            .unwrap();
        let ret = builder.finish().await.unwrap();
        assert_eq!(ret.len(), 2);
        assert_eq!(ret[0].sst_info.sst_info.range_tombstone_count, 2);
        let ssts = ret
            .iter()
            .map(|output| output.sst_info.sst_info.clone())
            .collect_vec();
        assert!(can_concat(&ssts));

        let key_range = ssts[0].key_range.as_ref().unwrap();
        let expected_left =
            FullKey::from_user_key(UserKey::for_test(table_id, b"aaaa"), HummockEpoch::MAX)
                .encode();
        let expected_right =
            FullKey::from_user_key(UserKey::for_test(table_id, b"eeee"), HummockEpoch::MAX)
                .encode();
        assert_eq!(key_range.left, expected_left);
        assert_eq!(key_range.right, expected_right);
        assert!(key_range.right_exclusive);

        let key_range = ssts[1].key_range.as_ref().unwrap();
        let expected_left =
            FullKey::from_user_key(UserKey::for_test(table_id, b"eeee"), HummockEpoch::MAX)
                .encode();
        let expected_right =
            FullKey::from_user_key(UserKey::for_test(table_id, b"gggg"), HummockEpoch::MAX)
                .encode();
        assert_eq!(key_range.left, expected_left);
        assert_eq!(key_range.right, expected_right);
        assert!(key_range.right_exclusive);
    }

    #[tokio::test]
    async fn test_check_table_and_vnode_change() {
        let block_size = 256;
        let table_capacity = 2 * block_size;
        let opts = SstableBuilderOptions {
            capacity: table_capacity,
            block_capacity: block_size,
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: 0.1,
            ..Default::default()
        };

        let table_partition_vnode =
            BTreeMap::from([(1_u32, 4_u32), (2_u32, 4_u32), (3_u32, 4_u32)]);

        let mut builder = CapacitySplitTableBuilder::new(
            LocalTableBuilderFactory::new(1001, mock_sstable_store(), opts),
            Arc::new(CompactorMetrics::unused()),
            None,
            table_partition_vnode,
        );

        let mut table_key = VirtualNode::from_index(0).to_be_bytes().to_vec();
        table_key.extend_from_slice("a".as_bytes());

        let switch_builder =
            builder.check_switch_builder(&UserKey::for_test(TableId::from(1), &table_key));
        assert!(switch_builder);

        {
            let mut table_key = VirtualNode::from_index(62).to_be_bytes().to_vec();
            table_key.extend_from_slice("a".as_bytes());
            let switch_builder =
                builder.check_switch_builder(&UserKey::for_test(TableId::from(1), &table_key));
            assert!(!switch_builder);

            let mut table_key = VirtualNode::from_index(63).to_be_bytes().to_vec();
            table_key.extend_from_slice("a".as_bytes());
            let switch_builder =
                builder.check_switch_builder(&UserKey::for_test(TableId::from(1), &table_key));
            assert!(!switch_builder);

            let mut table_key = VirtualNode::from_index(64).to_be_bytes().to_vec();
            table_key.extend_from_slice("a".as_bytes());
            let switch_builder =
                builder.check_switch_builder(&UserKey::for_test(TableId::from(1), &table_key));
            assert!(switch_builder);
        }

        let switch_builder =
            builder.check_switch_builder(&UserKey::for_test(TableId::from(2), &table_key));
        assert!(switch_builder);
        let switch_builder =
            builder.check_switch_builder(&UserKey::for_test(TableId::from(3), &table_key));
        assert!(switch_builder);
        let switch_builder =
            builder.check_switch_builder(&UserKey::for_test(TableId::from(4), &table_key));
        assert!(switch_builder);
        let switch_builder =
            builder.check_switch_builder(&UserKey::for_test(TableId::from(5), &table_key));
        assert!(!switch_builder);
    }
}
