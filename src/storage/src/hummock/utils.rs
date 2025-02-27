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
use std::fmt::{Debug, Formatter};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use risingwave_common::cache::CachePriority;
use risingwave_common::catalog::{TableId, TableOption};
use risingwave_hummock_sdk::key::{
    bound_table_key_range, EmptySliceRef, FullKey, TableKey, UserKey,
};
use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_hummock_sdk::{can_concat, HummockEpoch};
use risingwave_pb::hummock::SstableInfo;
use tokio::sync::watch::Sender;
use tokio::sync::Notify;

use super::{HummockError, HummockResult};
use crate::error::StorageResult;
use crate::hummock::CachePolicy;
use crate::mem_table::{KeyOp, MemTableError};
use crate::store::{OpConsistencyLevel, ReadOptions, StateStoreRead};

pub fn range_overlap<R, B>(
    search_key_range: &R,
    inclusive_start_key: &B,
    end_key: Bound<&B>,
) -> bool
where
    R: RangeBounds<B>,
    B: Ord,
{
    let (start_bound, end_bound) = (search_key_range.start_bound(), search_key_range.end_bound());

    //        RANGE
    // TABLE
    let too_left = match (start_bound, end_key) {
        (Included(range_start), Included(inclusive_end_key)) => range_start > inclusive_end_key,
        (Included(range_start), Excluded(end_key))
        | (Excluded(range_start), Included(end_key))
        | (Excluded(range_start), Excluded(end_key)) => range_start >= end_key,
        (Unbounded, _) | (_, Unbounded) => false,
    };
    // RANGE
    //        TABLE
    let too_right = match end_bound {
        Included(range_end) => range_end < inclusive_start_key,
        Excluded(range_end) => range_end <= inclusive_start_key,
        Unbounded => false,
    };

    !too_left && !too_right
}

pub fn validate_safe_epoch(safe_epoch: u64, epoch: u64) -> HummockResult<()> {
    if epoch < safe_epoch {
        return Err(HummockError::expired_epoch(safe_epoch, epoch));
    }

    Ok(())
}

pub fn validate_table_key_range(version: &HummockVersion) {
    for l in version.levels.values().flat_map(|levels| {
        levels
            .l0
            .as_ref()
            .unwrap()
            .sub_levels
            .iter()
            .chain(levels.levels.iter())
    }) {
        for t in &l.table_infos {
            assert!(
                t.key_range.is_some(),
                "key_range in table [{}] is none",
                t.get_object_id()
            );
        }
    }
}

pub fn filter_single_sst<R, B>(info: &SstableInfo, table_id: TableId, table_key_range: &R) -> bool
where
    R: RangeBounds<TableKey<B>>,
    B: AsRef<[u8]> + EmptySliceRef,
{
    let table_range = info.key_range.as_ref().unwrap();
    let table_start = FullKey::decode(table_range.left.as_slice()).user_key;
    let table_end = FullKey::decode(table_range.right.as_slice()).user_key;
    let (left, right) = bound_table_key_range(table_id, table_key_range);
    let left: Bound<UserKey<&[u8]>> = left.as_ref().map(|key| key.as_ref());
    let right: Bound<UserKey<&[u8]>> = right.as_ref().map(|key| key.as_ref());
    range_overlap(
        &(left, right),
        &table_start,
        if table_range.right_exclusive {
            Bound::Excluded(&table_end)
        } else {
            Bound::Included(&table_end)
        },
    ) && info
        .get_table_ids()
        .binary_search(&table_id.table_id())
        .is_ok()
}

/// Search the SST containing the specified key within a level, using binary search.
pub(crate) fn search_sst_idx(ssts: &[SstableInfo], key: UserKey<&[u8]>) -> usize {
    ssts.partition_point(|table| {
        let ord = FullKey::decode(&table.key_range.as_ref().unwrap().left)
            .user_key
            .cmp(&key);
        ord == Ordering::Less || ord == Ordering::Equal
    })
}

/// Prune overlapping SSTs that does not overlap with a specific key range or does not overlap with
/// a specific table id. Returns the sst ids after pruning.
pub fn prune_overlapping_ssts<'a, R, B>(
    ssts: &'a [SstableInfo],
    table_id: TableId,
    table_key_range: &'a R,
) -> impl DoubleEndedIterator<Item = &'a SstableInfo>
where
    R: RangeBounds<TableKey<B>>,
    B: AsRef<[u8]> + EmptySliceRef,
{
    ssts.iter()
        .filter(move |info| filter_single_sst(info, table_id, table_key_range))
}

/// Prune non-overlapping SSTs that does not overlap with a specific key range or does not overlap
/// with a specific table id. Returns the sst ids after pruning.
#[allow(clippy::type_complexity)]
pub fn prune_nonoverlapping_ssts<'a>(
    ssts: &'a [SstableInfo],
    user_key_range: (Bound<UserKey<&'a [u8]>>, Bound<UserKey<&'a [u8]>>),
) -> impl DoubleEndedIterator<Item = &'a SstableInfo> {
    debug_assert!(can_concat(ssts));
    let start_table_idx = match user_key_range.0 {
        Included(key) | Excluded(key) => search_sst_idx(ssts, key).saturating_sub(1),
        _ => 0,
    };
    let end_table_idx = match user_key_range.1 {
        Included(key) | Excluded(key) => search_sst_idx(ssts, key).saturating_sub(1),
        _ => ssts.len().saturating_sub(1),
    };
    ssts[start_table_idx..=end_table_idx].iter()
}

#[derive(Debug)]
struct MemoryLimiterInner {
    total_size: AtomicU64,
    notify: Notify,
    quota: u64,
}

impl MemoryLimiterInner {
    fn release_quota(&self, quota: u64) {
        self.total_size.fetch_sub(quota, AtomicOrdering::Release);
        self.notify.notify_waiters();
    }

    fn add_memory(&self, quota: u64) {
        self.total_size.fetch_add(quota, AtomicOrdering::SeqCst);
    }

    fn try_require_memory(&self, quota: u64) -> bool {
        let mut current_quota = self.total_size.load(AtomicOrdering::Acquire);
        while self.permit_quota(current_quota, quota) {
            match self.total_size.compare_exchange(
                current_quota,
                current_quota + quota,
                AtomicOrdering::SeqCst,
                AtomicOrdering::SeqCst,
            ) {
                Ok(_) => {
                    return true;
                }
                Err(old_quota) => {
                    current_quota = old_quota;
                }
            }
        }
        false
    }

    async fn require_memory(&self, quota: u64) {
        let current_quota = self.total_size.load(AtomicOrdering::Acquire);
        if self.permit_quota(current_quota, quota)
            && self
                .total_size
                .compare_exchange(
                    current_quota,
                    current_quota + quota,
                    AtomicOrdering::SeqCst,
                    AtomicOrdering::SeqCst,
                )
                .is_ok()
        {
            // fast path.
            return;
        }
        loop {
            let notified = self.notify.notified();
            let current_quota = self.total_size.load(AtomicOrdering::Acquire);
            if self.permit_quota(current_quota, quota) {
                match self.total_size.compare_exchange(
                    current_quota,
                    current_quota + quota,
                    AtomicOrdering::SeqCst,
                    AtomicOrdering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(old_quota) => {
                        // The quota is enough but just changed by other threads. So just try to
                        // update again without waiting notify.
                        if self.permit_quota(old_quota, quota) {
                            continue;
                        }
                    }
                }
            }
            notified.await;
        }
    }

    fn permit_quota(&self, current_quota: u64, _request_quota: u64) -> bool {
        current_quota <= self.quota
    }
}

#[derive(Debug)]
pub struct MemoryLimiter {
    inner: Arc<MemoryLimiterInner>,
}

pub struct MemoryTracker {
    limiter: Arc<MemoryLimiterInner>,
    quota: u64,
}

impl Debug for MemoryTracker {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("quota").field("quota", &self.quota).finish()
    }
}

impl MemoryLimiter {
    pub fn unlimit() -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(MemoryLimiterInner {
                total_size: AtomicU64::new(0),
                notify: Notify::new(),
                quota: u64::MAX - 1,
            }),
        })
    }

    pub fn new(quota: u64) -> Self {
        Self {
            inner: Arc::new(MemoryLimiterInner {
                total_size: AtomicU64::new(0),
                notify: Notify::new(),
                quota,
            }),
        }
    }

    pub fn try_require_memory(&self, quota: u64) -> Option<MemoryTracker> {
        if self.inner.try_require_memory(quota) {
            Some(MemoryTracker {
                limiter: self.inner.clone(),
                quota,
            })
        } else {
            None
        }
    }

    pub fn get_memory_usage(&self) -> u64 {
        self.inner.total_size.load(AtomicOrdering::Acquire)
    }

    pub fn quota(&self) -> u64 {
        self.inner.quota
    }

    pub fn must_require_memory(&self, quota: u64) -> MemoryTracker {
        if !self.inner.try_require_memory(quota) {
            self.inner.add_memory(quota);
        }

        MemoryTracker {
            limiter: self.inner.clone(),
            quota,
        }
    }
}

impl MemoryLimiter {
    pub async fn require_memory(&self, quota: u64) -> MemoryTracker {
        // Since the over provision limiter gets blocked only when the current usage exceeds the
        // memory quota, it is allowed to apply for more than the memory quota.
        self.inner.require_memory(quota).await;
        MemoryTracker {
            limiter: self.inner.clone(),
            quota,
        }
    }
}

impl MemoryTracker {
    pub fn try_increase_memory(&mut self, target: u64) -> bool {
        if self.quota >= target {
            return true;
        }
        if self.limiter.try_require_memory(target - self.quota) {
            self.quota = target;
            true
        } else {
            false
        }
    }
}

impl Drop for MemoryTracker {
    fn drop(&mut self) {
        self.limiter.release_quota(self.quota);
    }
}

/// Check whether the items in `sub_iter` is a subset of the items in `full_iter`, and meanwhile
/// preserve the order.
pub fn check_subset_preserve_order<T: Eq>(
    sub_iter: impl Iterator<Item = T>,
    mut full_iter: impl Iterator<Item = T>,
) -> bool {
    for sub_iter_item in sub_iter {
        let mut found = false;
        for full_iter_item in full_iter.by_ref() {
            if sub_iter_item == full_iter_item {
                found = true;
                break;
            }
        }
        if !found {
            return false;
        }
    }
    true
}

pub(crate) const ENABLE_SANITY_CHECK: bool = cfg!(debug_assertions);

/// Make sure the key to insert should not exist in storage.
pub(crate) async fn do_insert_sanity_check(
    key: &TableKey<Bytes>,
    value: &Bytes,
    inner: &impl StateStoreRead,
    epoch: u64,
    table_id: TableId,
    table_option: TableOption,
    op_consistency_level: &OpConsistencyLevel,
) -> StorageResult<()> {
    if let OpConsistencyLevel::Inconsistent = op_consistency_level {
        return Ok(());
    }
    let read_options = ReadOptions {
        retention_seconds: table_option.retention_seconds,
        table_id,
        cache_policy: CachePolicy::Fill(CachePriority::High),
        ..Default::default()
    };
    let stored_value = inner.get(key.clone(), epoch, read_options).await?;

    if let Some(stored_value) = stored_value {
        return Err(Box::new(MemTableError::InconsistentOperation {
            key: key.clone(),
            prev: KeyOp::Insert(stored_value),
            new: KeyOp::Insert(value.clone()),
        })
        .into());
    }
    Ok(())
}

/// Make sure that the key to delete should exist in storage and the value should be matched.
pub(crate) async fn do_delete_sanity_check(
    key: &TableKey<Bytes>,
    old_value: &Bytes,
    inner: &impl StateStoreRead,
    epoch: u64,
    table_id: TableId,
    table_option: TableOption,
    op_consistency_level: &OpConsistencyLevel,
) -> StorageResult<()> {
    let OpConsistencyLevel::ConsistentOldValue(old_value_checker) = op_consistency_level else {
        return Ok(());
    };
    let read_options = ReadOptions {
        retention_seconds: table_option.retention_seconds,
        table_id,
        cache_policy: CachePolicy::Fill(CachePriority::High),
        ..Default::default()
    };
    match inner.get(key.clone(), epoch, read_options).await? {
        None => Err(Box::new(MemTableError::InconsistentOperation {
            key: key.clone(),
            prev: KeyOp::Delete(Bytes::default()),
            new: KeyOp::Delete(old_value.clone()),
        })
        .into()),
        Some(stored_value) => {
            if !old_value_checker(&stored_value, old_value) {
                Err(Box::new(MemTableError::InconsistentOperation {
                    key: key.clone(),
                    prev: KeyOp::Insert(stored_value),
                    new: KeyOp::Delete(old_value.clone()),
                })
                .into())
            } else {
                Ok(())
            }
        }
    }
}

/// Make sure that the key to update should exist in storage and the value should be matched
pub(crate) async fn do_update_sanity_check(
    key: &TableKey<Bytes>,
    old_value: &Bytes,
    new_value: &Bytes,
    inner: &impl StateStoreRead,
    epoch: u64,
    table_id: TableId,
    table_option: TableOption,
    op_consistency_level: &OpConsistencyLevel,
) -> StorageResult<()> {
    let OpConsistencyLevel::ConsistentOldValue(old_value_checker) = op_consistency_level else {
        return Ok(());
    };
    let read_options = ReadOptions {
        retention_seconds: table_option.retention_seconds,
        table_id,
        cache_policy: CachePolicy::Fill(CachePriority::High),
        ..Default::default()
    };

    match inner.get(key.clone(), epoch, read_options).await? {
        None => Err(Box::new(MemTableError::InconsistentOperation {
            key: key.clone(),
            prev: KeyOp::Delete(Bytes::default()),
            new: KeyOp::Update((old_value.clone(), new_value.clone())),
        })
        .into()),
        Some(stored_value) => {
            if !old_value_checker(&stored_value, old_value) {
                Err(Box::new(MemTableError::InconsistentOperation {
                    key: key.clone(),
                    prev: KeyOp::Insert(stored_value),
                    new: KeyOp::Update((old_value.clone(), new_value.clone())),
                })
                .into())
            } else {
                Ok(())
            }
        }
    }
}

pub fn cmp_delete_range_left_bounds(a: Bound<&Bytes>, b: Bound<&Bytes>) -> Ordering {
    match (a, b) {
        // only right bound of delete range can be `Unbounded`
        (Unbounded, _) | (_, Unbounded) => unreachable!(),
        (Included(x), Included(y)) | (Excluded(x), Excluded(y)) => x.cmp(y),
        (Included(x), Excluded(y)) => x.cmp(y).then(Ordering::Less),
        (Excluded(x), Included(y)) => x.cmp(y).then(Ordering::Greater),
    }
}

pub(crate) fn validate_delete_range(left: &Bound<Bytes>, right: &Bound<Bytes>) -> bool {
    match (left, right) {
        // only right bound of delete range can be `Unbounded`
        (Unbounded, _) => unreachable!(),
        (_, Unbounded) => true,
        (Included(x), Included(y)) => x <= y,
        (Included(x), Excluded(y)) | (Excluded(x), Included(y)) | (Excluded(x), Excluded(y)) => {
            x < y
        }
    }
}

#[expect(dead_code)]
pub(crate) fn filter_with_delete_range<'a>(
    kv_iter: impl Iterator<Item = (TableKey<Bytes>, KeyOp)> + 'a,
    mut delete_ranges_iter: impl Iterator<Item = &'a (Bound<Bytes>, Bound<Bytes>)> + 'a,
) -> impl Iterator<Item = (TableKey<Bytes>, KeyOp)> + 'a {
    let mut range = delete_ranges_iter.next();
    if let Some((range_start, range_end)) = range {
        assert!(
            validate_delete_range(range_start, range_end),
            "range_end {:?} smaller than range_start {:?}",
            range_start,
            range_end
        );
    }
    kv_iter.filter(move |(ref key, _)| {
        if let Some(range_bound) = range {
            if cmp_delete_range_left_bounds(Included(&key.0), range_bound.0.as_ref())
                == Ordering::Less
            {
                true
            } else if range_bound.contains(key.as_ref()) {
                false
            } else {
                // Key has exceeded the current key range. Advance to the next range.
                loop {
                    range = delete_ranges_iter.next();
                    if let Some(range_bound) = range {
                        assert!(
                            validate_delete_range(&range_bound.0, &range_bound.1),
                            "range_end {:?} smaller than range_start {:?}",
                            range_bound.0,
                            range_bound.1
                        );
                        if cmp_delete_range_left_bounds(Included(key), range_bound.0.as_ref())
                            == Ordering::Less
                        {
                            // Not fall in the next delete range
                            break true;
                        } else if range_bound.contains(key.as_ref()) {
                            // Fall in the next delete range
                            break false;
                        } else {
                            // Exceed the next delete range. Go to the next delete range if there is
                            // any in the next loop
                            continue;
                        }
                    } else {
                        // No more delete range.
                        break true;
                    }
                }
            }
        } else {
            true
        }
    })
}

pub(crate) async fn wait_for_epoch(
    notifier: &Sender<HummockEpoch>,
    wait_epoch: u64,
) -> StorageResult<()> {
    let mut receiver = notifier.subscribe();
    // avoid unnecessary check in the loop if the value does not change
    let max_committed_epoch = *receiver.borrow_and_update();
    if max_committed_epoch >= wait_epoch {
        return Ok(());
    }
    loop {
        match tokio::time::timeout(Duration::from_secs(30), receiver.changed()).await {
            Err(_) => {
                // The reason that we need to retry here is batch scan in
                // chain/rearrange_chain is waiting for an
                // uncommitted epoch carried by the CreateMV barrier, which
                // can take unbounded time to become committed and propagate
                // to the CN. We should consider removing the retry as well as wait_epoch
                // for chain/rearrange_chain if we enforce
                // chain/rearrange_chain to be scheduled on the same
                // CN with the same distribution as the upstream MV.
                // See #3845 for more details.
                tracing::warn!(
                    epoch = wait_epoch,
                    "wait_epoch timeout when waiting for version update",
                );
                continue;
            }
            Ok(Err(_)) => {
                return Err(HummockError::wait_epoch("tx dropped").into());
            }
            Ok(Ok(_)) => {
                let max_committed_epoch = *receiver.borrow();
                if max_committed_epoch >= wait_epoch {
                    return Ok(());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::{poll_fn, Future};
    use std::task::Poll;

    use futures::FutureExt;

    use crate::hummock::utils::MemoryLimiter;

    async fn assert_pending(future: &mut (impl Future + Unpin)) {
        for _ in 0..10 {
            assert!(poll_fn(|cx| Poll::Ready(future.poll_unpin(cx)))
                .await
                .is_pending());
        }
    }

    #[tokio::test]
    async fn test_loose_memory_limiter() {
        let quota = 5;
        let memory_limiter = MemoryLimiter::new(quota);
        drop(memory_limiter.require_memory(6).await);
        let tracker1 = memory_limiter.require_memory(3).await;
        assert_eq!(3, memory_limiter.get_memory_usage());
        let tracker2 = memory_limiter.require_memory(4).await;
        assert_eq!(7, memory_limiter.get_memory_usage());
        let mut future = memory_limiter.require_memory(5).boxed();
        assert_pending(&mut future).await;
        assert_eq!(7, memory_limiter.get_memory_usage());
        drop(tracker1);
        let tracker3 = future.await;
        assert_eq!(9, memory_limiter.get_memory_usage());
        drop(tracker2);
        assert_eq!(5, memory_limiter.get_memory_usage());
        drop(tracker3);
        assert_eq!(0, memory_limiter.get_memory_usage());
    }
}
