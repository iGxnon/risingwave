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

use std::cmp::Ordering;
use std::collections::vec_deque::VecDeque;
use std::collections::HashSet;
use std::iter::once;
use std::ops::Bound::Included;
use std::sync::Arc;

use await_tree::InstrumentAwait;
use bytes::Bytes;
use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::MAX_SPILL_TIMES;
use risingwave_hummock_sdk::key::{
    bound_table_key_range, is_empty_key_range, FullKey, TableKey, TableKeyRange, UserKey,
};
use risingwave_hummock_sdk::key_range::KeyRangeCommon;
use risingwave_hummock_sdk::table_watermark::{
    ReadTableWatermark, TableWatermarksIndex, VnodeWatermark, WatermarkDirection,
};
use risingwave_hummock_sdk::version::HummockVersionDelta;
use risingwave_hummock_sdk::{EpochWithGap, HummockEpoch, LocalSstableInfo};
use risingwave_pb::hummock::{LevelType, SstableInfo};
use sync_point::sync_point;
use tracing::Instrument;

use super::StagingDataIterator;
use crate::error::StorageResult;
use crate::hummock::event_handler::HummockReadVersionRef;
use crate::hummock::iterator::{
    ConcatIterator, ForwardMergeRangeIterator, HummockIteratorUnion, MergeIterator,
    SkipWatermarkIterator, UserIterator,
};
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::sstable::SstableIteratorReadOptions;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::utils::{
    check_subset_preserve_order, filter_single_sst, prune_nonoverlapping_ssts,
    prune_overlapping_ssts, range_overlap, search_sst_idx,
};
use crate::hummock::{
    get_from_batch, get_from_sstable_info, hit_sstable_bloom_filter, HummockStorageIterator,
    HummockStorageIteratorInner, LocalHummockStorageIterator, ReadVersionTuple, Sstable,
    SstableDeleteRangeIterator, SstableIterator,
};
use crate::mem_table::{ImmId, ImmutableMemtable, MemTableHummockIterator};
use crate::monitor::{
    GetLocalMetricsGuard, HummockStateStoreMetrics, MayExistLocalMetricsGuard, StoreLocalStatistic,
};
use crate::store::{gen_min_epoch, ReadOptions, StateStoreIterExt, StreamTypeOfIter};

pub type CommittedVersion = PinnedVersion;

/// Data not committed to Hummock. There are two types of staging data:
/// - Immutable memtable: data that has been written into local state store but not persisted.
/// - Uncommitted SST: data that has been uploaded to persistent storage but not committed to
///   hummock version.

#[derive(Clone, Debug, PartialEq)]
pub struct StagingSstableInfo {
    // newer data comes first
    sstable_infos: Vec<LocalSstableInfo>,
    /// Epochs whose data are included in the Sstable. The newer epoch comes first.
    /// The field must not be empty.
    epochs: Vec<HummockEpoch>,
    imm_ids: Vec<ImmId>,
    imm_size: usize,
}

impl StagingSstableInfo {
    pub fn new(
        sstable_infos: Vec<LocalSstableInfo>,
        epochs: Vec<HummockEpoch>,
        imm_ids: Vec<ImmId>,
        imm_size: usize,
    ) -> Self {
        // the epochs are sorted from higher epoch to lower epoch
        assert!(epochs.is_sorted_by(|epoch1, epoch2| epoch2 <= epoch1));
        Self {
            sstable_infos,
            epochs,
            imm_ids,
            imm_size,
        }
    }

    pub fn sstable_infos(&self) -> &Vec<LocalSstableInfo> {
        &self.sstable_infos
    }

    pub fn imm_size(&self) -> usize {
        self.imm_size
    }

    pub fn epochs(&self) -> &Vec<HummockEpoch> {
        &self.epochs
    }

    pub fn imm_ids(&self) -> &Vec<ImmId> {
        &self.imm_ids
    }
}

#[derive(Clone)]
pub enum StagingData {
    ImmMem(ImmutableMemtable),
    MergedImmMem(ImmutableMemtable),
    Sst(StagingSstableInfo),
}

pub enum VersionUpdate {
    /// a new staging data entry will be added.
    Staging(StagingData),
    CommittedDelta(HummockVersionDelta),
    CommittedSnapshot(CommittedVersion),
    NewTableWatermark {
        direction: WatermarkDirection,
        epoch: HummockEpoch,
        vnode_watermarks: Vec<VnodeWatermark>,
    },
}

#[derive(Clone)]
pub struct StagingVersion {
    // newer data comes first
    // Note: Currently, building imm and writing to staging version is not atomic, and therefore
    // imm of smaller batch id may be added later than one with greater batch id
    pub imm: VecDeque<ImmutableMemtable>,

    // newer data comes first
    pub sst: VecDeque<StagingSstableInfo>,
}

impl StagingVersion {
    /// Get the overlapping `imm`s and `sst`s that overlap respectively with `table_key_range` and
    /// the user key range derived from `table_id`, `epoch` and `table_key_range`.
    pub fn prune_overlap<'a>(
        &'a self,
        max_epoch_inclusive: HummockEpoch,
        table_id: TableId,
        table_key_range: &'a TableKeyRange,
    ) -> (
        impl Iterator<Item = &ImmutableMemtable> + 'a,
        impl Iterator<Item = &SstableInfo> + 'a,
    ) {
        let (ref left, ref right) = table_key_range;
        let left = left.as_ref().map(|key| TableKey(key.0.as_ref()));
        let right = right.as_ref().map(|key| TableKey(key.0.as_ref()));
        let overlapped_imms = self.imm.iter().filter(move |imm| {
            // retain imm which is overlapped with (min_epoch_exclusive, max_epoch_inclusive]
            imm.min_epoch() <= max_epoch_inclusive
                && imm.table_id == table_id
                && range_overlap(
                    &(left, right),
                    &imm.start_table_key(),
                    Included(&imm.end_table_key()),
                )
        });

        // TODO: Remove duplicate sst based on sst id
        let overlapped_ssts = self
            .sst
            .iter()
            .filter(move |staging_sst| {
                let sst_max_epoch = *staging_sst.epochs.last().expect("epochs not empty");
                sst_max_epoch <= max_epoch_inclusive
            })
            .flat_map(move |staging_sst| {
                // TODO: sstable info should be concat-able after each streaming table owns a read
                // version. May use concat sstable iter instead in some cases.
                staging_sst
                    .sstable_infos
                    .iter()
                    .map(|sstable| &sstable.sst_info)
                    .filter(move |sstable: &&SstableInfo| {
                        filter_single_sst(sstable, table_id, table_key_range)
                    })
            });
        (overlapped_imms, overlapped_ssts)
    }
}

#[derive(Clone)]
/// A container of information required for reading from hummock.
pub struct HummockReadVersion {
    table_id: TableId,

    /// Local version for staging data.
    staging: StagingVersion,

    /// Remote version for committed data.
    committed: CommittedVersion,

    /// Indicate if this is replicated. If it is, we should ignore it during
    /// global state store read, to avoid duplicated results.
    /// Otherwise for local state store, it is fine, see we will see the
    /// ReadVersion just for that local state store.
    is_replicated: bool,

    table_watermarks: Option<TableWatermarksIndex>,
}

impl HummockReadVersion {
    pub fn new_with_replication_option(
        table_id: TableId,
        committed_version: CommittedVersion,
        is_replicated: bool,
    ) -> Self {
        // before build `HummockReadVersion`, we need to get the a initial version which obtained
        // from meta. want this initialization after version is initialized (now with
        // notification), so add a assert condition to guarantee correct initialization order
        assert!(committed_version.is_valid());
        Self {
            table_id,
            table_watermarks: committed_version
                .table_watermark_index()
                .get(&table_id)
                .cloned(),
            staging: StagingVersion {
                imm: VecDeque::default(),
                sst: VecDeque::default(),
            },

            committed: committed_version,

            is_replicated,
        }
    }

    pub fn new(table_id: TableId, committed_version: CommittedVersion) -> Self {
        Self::new_with_replication_option(table_id, committed_version, false)
    }

    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Updates the read version with `VersionUpdate`.
    /// There will be three data types to be processed
    /// `VersionUpdate::Staging`
    ///     - `StagingData::ImmMem` -> Insert into memory's `staging_imm`
    ///     - `StagingData::Sst` -> Update the sst to memory's `staging_sst` and remove the
    ///       corresponding `staging_imms` according to the `batch_id`
    /// `VersionUpdate::CommittedDelta` -> Unimplemented yet
    /// `VersionUpdate::CommittedSnapshot` -> Update `committed_version` , and clean up related
    /// `staging_sst` and `staging_imm` in memory according to epoch
    pub fn update(&mut self, info: VersionUpdate) {
        match info {
            VersionUpdate::Staging(staging) => match staging {
                // TODO: add a check to ensure that the added batch id of added imm is greater than
                // the batch id of imm at the front
                StagingData::ImmMem(imm) => {
                    if let Some(item) = self.staging.imm.front() {
                        // check batch_id order from newest to old
                        debug_assert!(item.batch_id() < imm.batch_id());
                    }

                    self.staging.imm.push_front(imm)
                }
                StagingData::MergedImmMem(merged_imm) => {
                    self.add_merged_imm(merged_imm);
                }
                StagingData::Sst(staging_sst) => {
                    // The following properties must be ensured:
                    // 1) self.staging.imm is sorted by imm id descendingly
                    // 2) staging_sst.imm_ids preserves the imm id partial
                    //    ordering of the participating read version imms. Example:
                    //    If staging_sst contains two read versions r1: [i1, i3] and  r2: [i2, i4],
                    //    then [i2, i1, i3, i4] is valid while [i3, i1, i2, i4] is invalid.
                    // 3) The intersection between staging_sst.imm_ids and self.staging.imm
                    //    are always the suffix of self.staging.imm

                    // Check 1)
                    debug_assert!(self
                        .staging
                        .imm
                        .iter()
                        .rev()
                        .is_sorted_by_key(|imm| imm.batch_id()));

                    // Calculate intersection
                    let staging_imm_ids_from_imms: HashSet<u64> =
                        self.staging.imm.iter().map(|imm| imm.batch_id()).collect();

                    // intersected batch_id order from oldest to newest
                    let intersect_imm_ids = staging_sst
                        .imm_ids
                        .iter()
                        .rev()
                        .copied()
                        .filter(|id| staging_imm_ids_from_imms.contains(id))
                        .collect_vec();

                    if !intersect_imm_ids.is_empty() {
                        // Check 2)
                        debug_assert!(check_subset_preserve_order(
                            intersect_imm_ids.iter().copied(),
                            self.staging.imm.iter().map(|imm| imm.batch_id()).rev(),
                        ));

                        // Check 3) and replace imms with a staging sst
                        for imm_id in &intersect_imm_ids {
                            if let Some(imm) = self.staging.imm.back() {
                                if *imm_id == imm.batch_id() {
                                    self.staging.imm.pop_back();
                                }
                            } else {
                                let local_imm_ids = self
                                    .staging
                                    .imm
                                    .iter()
                                    .map(|imm| imm.batch_id())
                                    .collect_vec();

                                unreachable!(
                                    "should not reach here staging_sst.size {},
                                    staging_sst.imm_ids {:?},
                                    staging_sst.epochs {:?},
                                    local_imm_ids {:?},
                                    intersect_imm_ids {:?}",
                                    staging_sst.imm_size,
                                    staging_sst.imm_ids,
                                    staging_sst.epochs,
                                    local_imm_ids,
                                    intersect_imm_ids,
                                );
                            }
                        }
                        self.staging.sst.push_front(staging_sst);
                    }
                }
            },

            VersionUpdate::CommittedDelta(_) => {
                unimplemented!()
            }

            VersionUpdate::CommittedSnapshot(committed_version) => {
                let max_committed_epoch = committed_version.max_committed_epoch();
                self.committed = committed_version;

                {
                    // TODO: remove it when support update staging local_sst
                    self.staging
                        .imm
                        .retain(|imm| imm.min_epoch() > max_committed_epoch);

                    self.staging.sst.retain(|sst| {
                        sst.epochs.first().expect("epochs not empty") > &max_committed_epoch
                    });

                    // check epochs.last() > MCE
                    assert!(self.staging.sst.iter().all(|sst| {
                        sst.epochs.last().expect("epochs not empty") > &max_committed_epoch
                    }));
                }

                if let Some(committed_watermarks) =
                    self.committed.table_watermark_index().get(&self.table_id)
                {
                    if let Some(watermark_index) = &mut self.table_watermarks {
                        watermark_index.apply_committed_watermarks(committed_watermarks);
                    } else {
                        self.table_watermarks = Some(committed_watermarks.clone());
                    }
                }
            }
            VersionUpdate::NewTableWatermark {
                direction,
                epoch,
                vnode_watermarks,
            } => self
                .table_watermarks
                .get_or_insert_with(|| {
                    TableWatermarksIndex::new(direction, self.committed.max_committed_epoch())
                })
                .add_epoch_watermark(epoch, &vnode_watermarks, direction),
        }
    }

    pub fn staging(&self) -> &StagingVersion {
        &self.staging
    }

    pub fn committed(&self) -> &CommittedVersion {
        &self.committed
    }

    /// We have assumption that the watermark is increasing monotonically. Therefore,
    /// here if the upper layer usage has passed an regressed watermark, we should
    /// filter out the regressed watermark. Currently the kv log store may write
    /// regressed watermark
    pub fn filter_regress_watermarks(&self, watermarks: &mut Vec<VnodeWatermark>) {
        if let Some(watermark_index) = &self.table_watermarks {
            watermark_index.filter_regress_watermarks(watermarks)
        }
    }

    pub fn add_merged_imm(&mut self, merged_imm: ImmutableMemtable) {
        assert!(merged_imm.get_imm_ids().iter().rev().is_sorted());
        let min_imm_id = *merged_imm.get_imm_ids().last().expect("non-empty");

        let back = self.staging.imm.back().expect("should not be empty");

        // pop and save imms that are written earlier than the oldest imm if there is any
        let earlier_imms = if back.batch_id() < min_imm_id {
            let mut earlier_imms = VecDeque::with_capacity(self.staging.imm.len());
            loop {
                let batch_id = self
                    .staging
                    .imm
                    .back()
                    .expect("should not be empty")
                    .batch_id();
                match batch_id.cmp(&min_imm_id) {
                    Ordering::Less => {
                        let imm = self.staging.imm.pop_back().unwrap();
                        earlier_imms.push_front(imm);
                    }
                    Ordering::Equal => {
                        break;
                    }
                    Ordering::Greater => {
                        let remaining_staging_imm_ids = self
                            .staging
                            .imm
                            .iter()
                            .map(|imm| imm.batch_id())
                            .collect_vec();
                        let earlier_imm_ids =
                            earlier_imms.iter().map(|imm| imm.batch_id()).collect_vec();

                        unreachable!(
                            "must have break in equal: {:?} {:?} {:?}",
                            remaining_staging_imm_ids,
                            earlier_imm_ids,
                            merged_imm.get_imm_ids()
                        )
                    }
                }
            }
            Some(earlier_imms)
        } else {
            assert_eq!(
                back.batch_id(),
                min_imm_id,
                "{:?} {:?}",
                {
                    self.staging
                        .imm
                        .iter()
                        .map(|imm| imm.batch_id())
                        .collect_vec()
                },
                merged_imm.get_imm_ids()
            );
            None
        };

        // iter from smaller imm and take the older imm at the back.
        for imm_id in merged_imm.get_imm_ids().iter().rev() {
            let imm = self.staging.imm.pop_back().expect("should exist");
            assert_eq!(
                imm.batch_id(),
                *imm_id,
                "{:?} {:?} {}",
                {
                    self.staging
                        .imm
                        .iter()
                        .map(|imm| imm.batch_id())
                        .collect_vec()
                },
                merged_imm.get_imm_ids(),
                imm_id,
            );
        }

        self.staging.imm.push_back(merged_imm);
        if let Some(earlier_imms) = earlier_imms {
            self.staging.imm.extend(earlier_imms);
        }
    }

    pub fn is_replicated(&self) -> bool {
        self.is_replicated
    }
}

pub fn read_filter_for_batch(
    epoch: HummockEpoch, // for check
    table_id: TableId,
    mut key_range: TableKeyRange,
    read_version_vec: Vec<HummockReadVersionRef>,
) -> StorageResult<(TableKeyRange, ReadVersionTuple)> {
    assert!(!read_version_vec.is_empty());
    let mut staging_vec = Vec::with_capacity(read_version_vec.len());
    let mut max_mce_version: Option<CommittedVersion> = None;
    let mut table_watermarks: Vec<ReadTableWatermark> = Vec::new();
    for read_version in &read_version_vec {
        let read_version_guard = read_version.read();

        let read_watermark = read_version_guard
            .table_watermarks
            .as_ref()
            .and_then(|watermarks| watermarks.range_watermarks(epoch, &mut key_range));

        if let Some(read_watermark) = read_watermark {
            table_watermarks.push(read_watermark);
        }

        let (imms, ssts) = {
            let (imm_iter, sst_iter) = read_version_guard
                .staging()
                .prune_overlap(epoch, table_id, &key_range);

            (
                imm_iter.cloned().collect_vec(),
                sst_iter.cloned().collect_vec(),
            )
        };

        staging_vec.push((imms, ssts));
        if let Some(version) = &max_mce_version {
            if read_version_guard.committed().max_committed_epoch() > version.max_committed_epoch()
            {
                max_mce_version = Some(read_version_guard.committed.clone());
            }
        } else {
            max_mce_version = Some(read_version_guard.committed.clone());
        }
    }

    let max_mce_version = max_mce_version.expect("should exist for once");

    let mut imm_vec = Vec::default();
    let mut sst_vec = Vec::default();
    let mut seen_imm_ids = HashSet::new();
    let mut seen_sst_ids = HashSet::new();

    // only filter the staging data that epoch greater than max_mce to avoid data duplication
    let (min_epoch, max_epoch) = (max_mce_version.max_committed_epoch(), epoch);
    // prune imm and sst with max_mce
    for (staging_imms, staging_ssts) in staging_vec {
        imm_vec.extend(staging_imms.into_iter().filter(|imm| {
            // There shouldn't be duplicated IMMs because merge imm only operates on a single shard.
            assert!(seen_imm_ids.insert(imm.batch_id()));
            imm.min_epoch() > min_epoch && imm.min_epoch() <= max_epoch
        }));

        sst_vec.extend(staging_ssts.into_iter().filter(|staging_sst| {
            assert!(
                staging_sst.get_max_epoch() <= min_epoch || staging_sst.get_min_epoch() > min_epoch
            );
            // Dedup staging SSTs in different shard. Duplicates can happen in the following case:
            // - Table 1 Shard 1 produces IMM 1
            // - Table 1 Shard 2 produces IMM 2
            // - IMM 1 and IMM 2 are compacted into SST 1 as a Staging SST
            // - SST 1 is added to both Shard 1's and Shard 2's read version
            staging_sst.min_epoch > min_epoch && seen_sst_ids.insert(staging_sst.object_id)
        }));
    }

    Ok((
        key_range,
        (
            imm_vec,
            sst_vec,
            max_mce_version,
            ReadTableWatermark::merge_multiple(table_watermarks),
        ),
    ))
}

pub fn read_filter_for_local(
    epoch: HummockEpoch,
    table_id: TableId,
    mut table_key_range: TableKeyRange,
    read_version: &RwLock<HummockReadVersion>,
) -> StorageResult<(TableKeyRange, ReadVersionTuple)> {
    let read_version_guard = read_version.read();

    let committed_version = read_version_guard.committed().clone();

    let table_watermark = read_version_guard
        .table_watermarks
        .as_ref()
        .and_then(|watermark| watermark.range_watermarks(epoch, &mut table_key_range));

    let (imm_iter, sst_iter) =
        read_version_guard
            .staging()
            .prune_overlap(epoch, table_id, &table_key_range);

    let imms = imm_iter.cloned().collect();
    let ssts = sst_iter.cloned().collect();

    Ok((
        table_key_range,
        (imms, ssts, committed_version, table_watermark),
    ))
}

#[derive(Clone)]
pub struct HummockVersionReader {
    sstable_store: SstableStoreRef,

    /// Statistics
    state_store_metrics: Arc<HummockStateStoreMetrics>,
    preload_retry_times: usize,
}

/// use `HummockVersionReader` to reuse `get` and `iter` implement for both `batch_query` and
/// `streaming_query`
impl HummockVersionReader {
    pub fn new(
        sstable_store: SstableStoreRef,
        state_store_metrics: Arc<HummockStateStoreMetrics>,
        preload_retry_times: usize,
    ) -> Self {
        Self {
            sstable_store,
            state_store_metrics,
            preload_retry_times,
        }
    }

    pub fn stats(&self) -> &Arc<HummockStateStoreMetrics> {
        &self.state_store_metrics
    }
}

const SLOW_ITER_FETCH_META_DURATION_SECOND: f64 = 5.0;

impl HummockVersionReader {
    pub async fn get(
        &self,
        table_key: TableKey<Bytes>,
        epoch: u64,
        read_options: ReadOptions,
        read_version_tuple: ReadVersionTuple,
    ) -> StorageResult<Option<Bytes>> {
        let (imms, uncommitted_ssts, committed_version, watermark) = read_version_tuple;
        let key_vnode = table_key.vnode_part();
        if let Some(read_watermark) = watermark {
            for (vnode, watermark) in read_watermark.vnode_watermarks {
                if vnode == key_vnode {
                    let inner_key = table_key.key_part();
                    if read_watermark
                        .direction
                        .filter_by_watermark(inner_key, watermark)
                    {
                        return Ok(None);
                    }
                }
            }
        }
        let min_epoch = gen_min_epoch(epoch, read_options.retention_seconds.as_ref());
        let mut stats_guard =
            GetLocalMetricsGuard::new(self.state_store_metrics.clone(), read_options.table_id);
        let local_stats = &mut stats_guard.local_stats;
        local_stats.found_key = true;

        // 1. read staging data
        for imm in &imms {
            // skip imm that only holding out-of-date data
            if imm.max_epoch() < min_epoch {
                continue;
            }

            local_stats.staging_imm_get_count += 1;

            if let Some((data, data_epoch)) = get_from_batch(
                imm,
                TableKey(table_key.as_ref()),
                epoch,
                &read_options,
                local_stats,
            ) {
                return Ok(if data_epoch.pure_epoch() < min_epoch {
                    None
                } else {
                    data.into_user_value()
                });
            }
        }

        // 2. order guarantee: imm -> sst
        let dist_key_hash = read_options.prefix_hint.as_ref().map(|dist_key| {
            Sstable::hash_for_bloom_filter(dist_key.as_ref(), read_options.table_id.table_id())
        });

        // Here epoch passed in is pure epoch, and we will seek the constructed `full_key` later.
        // Therefore, it is necessary to construct the `full_key` with `MAX_SPILL_TIMES`, otherwise, the iterator might skip keys with spill offset greater than 0.
        let full_key = FullKey::new_with_gap_epoch(
            read_options.table_id,
            TableKey(table_key.clone()),
            EpochWithGap::new(epoch, MAX_SPILL_TIMES),
        );
        for local_sst in &uncommitted_ssts {
            local_stats.staging_sst_get_count += 1;
            if let Some((data, data_epoch)) = get_from_sstable_info(
                self.sstable_store.clone(),
                local_sst,
                full_key.to_ref(),
                &read_options,
                dist_key_hash,
                local_stats,
            )
            .await?
            {
                return Ok(if data_epoch.pure_epoch() < min_epoch {
                    None
                } else {
                    data.into_user_value()
                });
            }
        }

        // 3. read from committed_version sst file
        // Because SST meta records encoded key range,
        // the filter key needs to be encoded as well.
        assert!(committed_version.is_valid());
        for level in committed_version.levels(read_options.table_id) {
            if level.table_infos.is_empty() {
                continue;
            }

            match level.level_type() {
                LevelType::Overlapping | LevelType::Unspecified => {
                    let single_table_key_range = table_key.clone()..=table_key.clone();
                    let sstable_infos = prune_overlapping_ssts(
                        &level.table_infos,
                        read_options.table_id,
                        &single_table_key_range,
                    );
                    for sstable_info in sstable_infos {
                        local_stats.overlapping_get_count += 1;
                        if let Some((data, data_epoch)) = get_from_sstable_info(
                            self.sstable_store.clone(),
                            sstable_info,
                            full_key.to_ref(),
                            &read_options,
                            dist_key_hash,
                            local_stats,
                        )
                        .await?
                        {
                            return Ok(if data_epoch.pure_epoch() < min_epoch {
                                None
                            } else {
                                data.into_user_value()
                            });
                        }
                    }
                }
                LevelType::Nonoverlapping => {
                    let mut table_info_idx =
                        search_sst_idx(&level.table_infos, full_key.user_key.as_ref());
                    if table_info_idx == 0 {
                        continue;
                    }
                    table_info_idx = table_info_idx.saturating_sub(1);
                    let ord = level.table_infos[table_info_idx]
                        .key_range
                        .as_ref()
                        .unwrap()
                        .compare_right_with_user_key(full_key.user_key.as_ref());
                    // the case that the key falls into the gap between two ssts
                    if ord == Ordering::Less {
                        sync_point!("HUMMOCK_V2::GET::SKIP_BY_NO_FILE");
                        continue;
                    }

                    local_stats.non_overlapping_get_count += 1;
                    if let Some((data, data_epoch)) = get_from_sstable_info(
                        self.sstable_store.clone(),
                        &level.table_infos[table_info_idx],
                        full_key.to_ref(),
                        &read_options,
                        dist_key_hash,
                        local_stats,
                    )
                    .await?
                    {
                        return Ok(if data_epoch.pure_epoch() < min_epoch {
                            None
                        } else {
                            data.into_user_value()
                        });
                    }
                }
            }
        }
        stats_guard.local_stats.found_key = false;
        Ok(None)
    }

    pub async fn iter(
        &self,
        table_key_range: TableKeyRange,
        epoch: u64,
        read_options: ReadOptions,
        read_version_tuple: ReadVersionTuple,
    ) -> StorageResult<StreamTypeOfIter<HummockStorageIterator>> {
        self.iter_inner(
            table_key_range,
            epoch,
            read_options,
            read_version_tuple,
            None,
        )
        .await
    }

    pub async fn iter_with_memtable<'a>(
        &'a self,
        table_key_range: TableKeyRange,
        epoch: u64,
        read_options: ReadOptions,
        read_version_tuple: (
            Vec<ImmutableMemtable>,
            Vec<SstableInfo>,
            CommittedVersion,
            Option<ReadTableWatermark>,
        ),
        memtable_iter: MemTableHummockIterator<'a>,
    ) -> StorageResult<StreamTypeOfIter<LocalHummockStorageIterator<'_>>> {
        self.iter_inner(
            table_key_range,
            epoch,
            read_options,
            read_version_tuple,
            Some(memtable_iter),
        )
        .await
    }

    pub async fn iter_inner<'a, 'b>(
        &'a self,
        table_key_range: TableKeyRange,
        epoch: u64,
        read_options: ReadOptions,
        read_version_tuple: ReadVersionTuple,
        mem_table: Option<MemTableHummockIterator<'b>>,
    ) -> StorageResult<StreamTypeOfIter<HummockStorageIteratorInner<'b>>> {
        let table_id_string = read_options.table_id.to_string();
        let table_id_label = table_id_string.as_str();
        let (imms, uncommitted_ssts, committed, watermark) = read_version_tuple;

        let mut local_stats = StoreLocalStatistic::default();
        let mut staging_iters = Vec::with_capacity(imms.len() + uncommitted_ssts.len());
        let mut delete_range_iter = ForwardMergeRangeIterator::new(epoch);
        local_stats.staging_imm_iter_count = imms.len() as u64;
        for imm in imms {
            staging_iters.push(HummockIteratorUnion::First(imm.into_forward_iter()));
        }

        // 2. build iterator from committed
        // Because SST meta records encoded key range,
        // the filter key range needs to be encoded as well.
        let user_key_range = bound_table_key_range(read_options.table_id, &table_key_range);
        let user_key_range_ref = (
            user_key_range.0.as_ref().map(UserKey::as_ref),
            user_key_range.1.as_ref().map(UserKey::as_ref),
        );
        let mut staging_sst_iter_count = 0;
        // encode once
        let bloom_filter_prefix_hash = read_options
            .prefix_hint
            .as_ref()
            .map(|hint| Sstable::hash_for_bloom_filter(hint, read_options.table_id.table_id()));
        let mut sst_read_options = SstableIteratorReadOptions::from_read_options(&read_options);
        if read_options.prefetch_options.prefetch {
            sst_read_options.must_iterated_end_user_key =
                Some(user_key_range.1.map(|key| key.cloned()));
            sst_read_options.max_preload_retry_times = self.preload_retry_times;
        }
        let sst_read_options = Arc::new(sst_read_options);
        for sstable_info in &uncommitted_ssts {
            let table_holder = self
                .sstable_store
                .sstable(sstable_info, &mut local_stats)
                .instrument(tracing::trace_span!("get_sstable"))
                .await?;

            if !table_holder.meta.monotonic_tombstone_events.is_empty()
                && !read_options.ignore_range_tombstone
            {
                delete_range_iter
                    .add_sst_iter(SstableDeleteRangeIterator::new(table_holder.clone()));
            }
            if let Some(prefix_hash) = bloom_filter_prefix_hash.as_ref() {
                if !hit_sstable_bloom_filter(
                    &table_holder,
                    &user_key_range_ref,
                    *prefix_hash,
                    &mut local_stats,
                ) {
                    continue;
                }
            }

            staging_sst_iter_count += 1;
            staging_iters.push(HummockIteratorUnion::Second(SstableIterator::new(
                table_holder,
                self.sstable_store.clone(),
                sst_read_options.clone(),
            )));
        }
        local_stats.staging_sst_iter_count = staging_sst_iter_count;
        let staging_iter: StagingDataIterator = MergeIterator::new(staging_iters);

        let mut non_overlapping_iters = Vec::new();
        let mut overlapping_iters = Vec::new();
        let timer = self
            .state_store_metrics
            .iter_fetch_meta_duration
            .with_label_values(&[table_id_label])
            .start_timer();

        for level in committed.levels(read_options.table_id) {
            if level.table_infos.is_empty() {
                continue;
            }

            if level.level_type == LevelType::Nonoverlapping as i32 {
                let table_infos = prune_nonoverlapping_ssts(&level.table_infos, user_key_range_ref);
                let sstables = table_infos
                    .filter(|sstable_info| {
                        sstable_info
                            .table_ids
                            .binary_search(&read_options.table_id.table_id)
                            .is_ok()
                    })
                    .cloned()
                    .collect_vec();
                if sstables.is_empty() {
                    continue;
                }
                if sstables.len() > 1 {
                    let ssts_which_have_delete_range = sstables
                        .iter()
                        .filter(|sst| sst.get_range_tombstone_count() > 0)
                        .cloned()
                        .collect_vec();
                    if !ssts_which_have_delete_range.is_empty() {
                        delete_range_iter.add_concat_iter(
                            ssts_which_have_delete_range,
                            self.sstable_store.clone(),
                        );
                    }
                    non_overlapping_iters.push(ConcatIterator::new(
                        sstables,
                        self.sstable_store.clone(),
                        sst_read_options.clone(),
                    ));
                    local_stats.non_overlapping_iter_count += 1;
                } else {
                    let sstable = self
                        .sstable_store
                        .sstable(&sstables[0], &mut local_stats)
                        .instrument(tracing::trace_span!("get_sstable"))
                        .await?;
                    if !sstable.meta.monotonic_tombstone_events.is_empty()
                        && !read_options.ignore_range_tombstone
                    {
                        delete_range_iter
                            .add_sst_iter(SstableDeleteRangeIterator::new(sstable.clone()));
                    }
                    if let Some(dist_hash) = bloom_filter_prefix_hash.as_ref() {
                        if !hit_sstable_bloom_filter(
                            &sstable,
                            &user_key_range_ref,
                            *dist_hash,
                            &mut local_stats,
                        ) {
                            continue;
                        }
                    }
                    // Since there is only one sst to be included for the current non-overlapping
                    // level, there is no need to create a ConcatIterator on it.
                    // We put the SstableIterator in `overlapping_iters` just for convenience since
                    // it overlaps with SSTs in other levels. In metrics reporting, we still count
                    // it in `non_overlapping_iter_count`.
                    overlapping_iters.push(SstableIterator::new(
                        sstable,
                        self.sstable_store.clone(),
                        sst_read_options.clone(),
                    ));
                    local_stats.non_overlapping_iter_count += 1;
                }
            } else {
                let table_infos = prune_overlapping_ssts(
                    &level.table_infos,
                    read_options.table_id,
                    &table_key_range,
                );
                // Overlapping
                let fetch_meta_req = table_infos.rev().collect_vec();
                if fetch_meta_req.is_empty() {
                    continue;
                }
                for sstable_info in fetch_meta_req {
                    let sstable = self
                        .sstable_store
                        .sstable(sstable_info, &mut local_stats)
                        .instrument(tracing::trace_span!("get_sstable"))
                        .await?;
                    assert_eq!(sstable_info.get_object_id(), sstable.id);
                    if !sstable.meta.monotonic_tombstone_events.is_empty()
                        && !read_options.ignore_range_tombstone
                    {
                        delete_range_iter
                            .add_sst_iter(SstableDeleteRangeIterator::new(sstable.clone()));
                    }
                    if let Some(dist_hash) = bloom_filter_prefix_hash.as_ref() {
                        if !hit_sstable_bloom_filter(
                            &sstable,
                            &user_key_range_ref,
                            *dist_hash,
                            &mut local_stats,
                        ) {
                            continue;
                        }
                    }
                    overlapping_iters.push(SstableIterator::new(
                        sstable,
                        self.sstable_store.clone(),
                        sst_read_options.clone(),
                    ));
                    local_stats.overlapping_iter_count += 1;
                }
            }
        }
        let fetch_meta_duration_sec = timer.stop_and_record();
        if fetch_meta_duration_sec > SLOW_ITER_FETCH_META_DURATION_SECOND {
            tracing::warn!("Fetching meta while creating an iter to read table_id {:?} at epoch {:?} is slow: duration = {:?}s, cache unhits = {:?}.",
                table_id_string, epoch, fetch_meta_duration_sec, local_stats.cache_meta_block_miss);
            self.state_store_metrics
                .iter_slow_fetch_meta_cache_unhits
                .set(local_stats.cache_meta_block_miss as i64);
        }

        // 3. build user_iterator
        let merge_iter = MergeIterator::new(
            once(HummockIteratorUnion::First(staging_iter))
                .chain(
                    overlapping_iters
                        .into_iter()
                        .map(HummockIteratorUnion::Second),
                )
                .chain(
                    non_overlapping_iters
                        .into_iter()
                        .map(HummockIteratorUnion::Third),
                )
                .chain(mem_table.into_iter().map(HummockIteratorUnion::Fourth)),
        );

        let watermark = watermark
            .into_iter()
            .map(|watermark| (read_options.table_id, watermark))
            .collect();

        let skip_watermark_iter = SkipWatermarkIterator::new(merge_iter, watermark);

        let user_key_range = (
            user_key_range.0.map(|key| key.cloned()),
            user_key_range.1.map(|key| key.cloned()),
        );

        // the epoch_range left bound for iterator read
        let min_epoch = gen_min_epoch(epoch, read_options.retention_seconds.as_ref());
        let mut user_iter = UserIterator::new(
            skip_watermark_iter,
            user_key_range,
            epoch,
            min_epoch,
            Some(committed),
            delete_range_iter,
        );
        user_iter
            .rewind()
            .verbose_instrument_await("rewind")
            .await?;
        local_stats.found_key = user_iter.is_valid();
        local_stats.sub_iter_count = local_stats.staging_imm_iter_count
            + local_stats.staging_sst_iter_count
            + local_stats.overlapping_iter_count
            + local_stats.non_overlapping_iter_count;

        Ok(HummockStorageIteratorInner::new(
            user_iter,
            self.state_store_metrics.clone(),
            read_options.table_id,
            local_stats,
        )
        .into_stream())
    }

    // Note: this method will not check the kv tomestones and delete range tomestones
    pub async fn may_exist(
        &self,
        table_key_range: TableKeyRange,
        read_options: ReadOptions,
        read_version_tuple: ReadVersionTuple,
    ) -> StorageResult<bool> {
        if is_empty_key_range(&table_key_range) {
            return Ok(false);
        }

        let table_id = read_options.table_id;
        let (imms, uncommitted_ssts, committed_version, _) = read_version_tuple;
        let mut stats_guard =
            MayExistLocalMetricsGuard::new(self.state_store_metrics.clone(), table_id);

        // 1. check staging data
        for imm in &imms {
            if imm.range_exists(&table_key_range) {
                return Ok(true);
            }
        }

        let user_key_range = bound_table_key_range(read_options.table_id, &table_key_range);
        let user_key_range_ref = (
            user_key_range.0.as_ref().map(UserKey::as_ref),
            user_key_range.1.as_ref().map(UserKey::as_ref),
        );
        let bloom_filter_prefix_hash = if let Some(prefix_hint) = read_options.prefix_hint {
            Sstable::hash_for_bloom_filter(&prefix_hint, table_id.table_id)
        } else {
            // only use `table_key_range` to see whether all SSTs are filtered out
            // without looking at bloom filter because prefix_hint is not provided
            if !uncommitted_ssts.is_empty() {
                // uncommitted_ssts is already pruned by `table_key_range` so no extra check is
                // needed.
                return Ok(true);
            }
            for level in committed_version.levels(table_id) {
                match level.level_type() {
                    LevelType::Overlapping | LevelType::Unspecified => {
                        if prune_overlapping_ssts(&level.table_infos, table_id, &table_key_range)
                            .next()
                            .is_some()
                        {
                            return Ok(true);
                        }
                    }
                    LevelType::Nonoverlapping => {
                        if prune_nonoverlapping_ssts(&level.table_infos, user_key_range_ref)
                            .next()
                            .is_some()
                        {
                            return Ok(true);
                        }
                    }
                }
            }
            return Ok(false);
        };

        // 2. order guarantee: imm -> sst
        for local_sst in &uncommitted_ssts {
            stats_guard.local_stats.may_exist_check_sstable_count += 1;
            if hit_sstable_bloom_filter(
                self.sstable_store
                    .sstable(local_sst, &mut stats_guard.local_stats)
                    .await?
                    .as_ref(),
                &user_key_range_ref,
                bloom_filter_prefix_hash,
                &mut stats_guard.local_stats,
            ) {
                return Ok(true);
            }
        }

        // 3. read from committed_version sst file
        // Because SST meta records encoded key range,
        // the filter key needs to be encoded as well.
        assert!(committed_version.is_valid());
        for level in committed_version.levels(table_id) {
            if level.table_infos.is_empty() {
                continue;
            }
            match level.level_type() {
                LevelType::Overlapping | LevelType::Unspecified => {
                    let sstable_infos =
                        prune_overlapping_ssts(&level.table_infos, table_id, &table_key_range);
                    for sstable_info in sstable_infos {
                        stats_guard.local_stats.may_exist_check_sstable_count += 1;
                        if hit_sstable_bloom_filter(
                            self.sstable_store
                                .sstable(sstable_info, &mut stats_guard.local_stats)
                                .await?
                                .as_ref(),
                            &user_key_range_ref,
                            bloom_filter_prefix_hash,
                            &mut stats_guard.local_stats,
                        ) {
                            return Ok(true);
                        }
                    }
                }
                LevelType::Nonoverlapping => {
                    let table_infos =
                        prune_nonoverlapping_ssts(&level.table_infos, user_key_range_ref);

                    for table_info in table_infos {
                        stats_guard.local_stats.may_exist_check_sstable_count += 1;
                        if hit_sstable_bloom_filter(
                            self.sstable_store
                                .sstable(table_info, &mut stats_guard.local_stats)
                                .await?
                                .as_ref(),
                            &user_key_range_ref,
                            bloom_filter_prefix_hash,
                            &mut stats_guard.local_stats,
                        ) {
                            return Ok(true);
                        }
                    }
                }
            }
        }

        Ok(false)
    }
}
