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

use risingwave_common::config::{StorageConfig, StorageMemoryConfig};
use risingwave_common::util::pretty_bytes::convert;

/// The minimal memory requirement of computing tasks in megabytes.
pub const MIN_COMPUTE_MEMORY_MB: usize = 512;
/// The memory reserved for system usage (stack and code segment of processes, allocation
/// overhead, network buffer, etc.) in megabytes.
pub const MIN_SYSTEM_RESERVED_MEMORY_MB: usize = 512;

const SYSTEM_RESERVED_MEMORY_PROPORTION: f64 = 0.3;

const STORAGE_MEMORY_PROPORTION: f64 = 0.3;

const COMPACTOR_MEMORY_PROPORTION: f64 = 0.1;

const STORAGE_BLOCK_CACHE_MEMORY_PROPORTION: f64 = 0.3;

const STORAGE_META_CACHE_MAX_MEMORY_MB: usize = 4096;
const STORAGE_META_CACHE_MEMORY_PROPORTION: f64 = 0.35;
const STORAGE_SHARED_BUFFER_MEMORY_PROPORTION: f64 = 0.3;
const STORAGE_DEFAULT_HIGH_PRIORITY_BLOCK_CACHE_RATIO: usize = 50;

/// Each compute node reserves some memory for stack and code segment of processes, allocation
/// overhead, network buffer, etc. based on `SYSTEM_RESERVED_MEMORY_PROPORTION`. The reserve memory
/// size must be larger than `MIN_SYSTEM_RESERVED_MEMORY_MB`
pub fn reserve_memory_bytes(total_memory_bytes: usize) -> (usize, usize) {
    if total_memory_bytes < MIN_COMPUTE_MEMORY_MB << 20 {
        panic!(
            "The total memory size ({}) is too small. It must be at least {} MB.",
            convert(total_memory_bytes as _),
            MIN_COMPUTE_MEMORY_MB
        );
    }

    let reserved = std::cmp::max(
        (total_memory_bytes as f64 * SYSTEM_RESERVED_MEMORY_PROPORTION).ceil() as usize,
        MIN_SYSTEM_RESERVED_MEMORY_MB << 20,
    );
    (reserved, total_memory_bytes - reserved)
}

/// Decide the memory limit for each storage cache. If not specified in `StorageConfig`, memory
/// limits are calculated based on the proportions to total `non_reserved_memory_bytes`.
pub fn storage_memory_config(
    non_reserved_memory_bytes: usize,
    embedded_compactor_enabled: bool,
    storage_config: &StorageConfig,
) -> StorageMemoryConfig {
    let (storage_memory_proportion, compactor_memory_proportion) = if embedded_compactor_enabled {
        (STORAGE_MEMORY_PROPORTION, COMPACTOR_MEMORY_PROPORTION)
    } else {
        (STORAGE_MEMORY_PROPORTION + COMPACTOR_MEMORY_PROPORTION, 0.0)
    };
    let mut block_cache_capacity_mb = storage_config.block_cache_capacity_mb.unwrap_or(
        ((non_reserved_memory_bytes as f64
            * storage_memory_proportion
            * STORAGE_BLOCK_CACHE_MEMORY_PROPORTION)
            .ceil() as usize)
            >> 20,
    );
    let high_priority_ratio_in_percent = storage_config
        .high_priority_ratio_in_percent
        .unwrap_or(STORAGE_DEFAULT_HIGH_PRIORITY_BLOCK_CACHE_RATIO);
    let default_meta_cache_capacity = (non_reserved_memory_bytes as f64
        * storage_memory_proportion
        * STORAGE_META_CACHE_MEMORY_PROPORTION)
        .ceil() as usize;
    let meta_cache_capacity_mb = storage_config
        .meta_cache_capacity_mb
        .unwrap_or(std::cmp::min(
            default_meta_cache_capacity >> 20,
            STORAGE_META_CACHE_MAX_MEMORY_MB,
        ));

    let prefetch_buffer_capacity_mb = storage_config
        .prefetch_buffer_capacity_mb
        .unwrap_or(block_cache_capacity_mb);

    if meta_cache_capacity_mb == STORAGE_META_CACHE_MAX_MEMORY_MB {
        block_cache_capacity_mb += (default_meta_cache_capacity >> 20) - meta_cache_capacity_mb;
    }
    let shared_buffer_capacity_mb = storage_config.shared_buffer_capacity_mb.unwrap_or(
        ((non_reserved_memory_bytes as f64
            * storage_memory_proportion
            * STORAGE_SHARED_BUFFER_MEMORY_PROPORTION)
            .ceil() as usize)
            >> 20,
    );

    let data_file_cache_ring_buffer_capacity_mb = if storage_config.data_file_cache.dir.is_empty() {
        0
    } else {
        storage_config.data_file_cache.ring_buffer_capacity_mb
    };
    let meta_file_cache_ring_buffer_capacity_mb = if storage_config.meta_file_cache.dir.is_empty() {
        0
    } else {
        storage_config.meta_file_cache.ring_buffer_capacity_mb
    };

    let compactor_memory_limit_mb = storage_config.compactor_memory_limit_mb.unwrap_or(
        ((non_reserved_memory_bytes as f64 * compactor_memory_proportion).ceil() as usize) >> 20,
    );

    let total_calculated_mb = block_cache_capacity_mb
        + meta_cache_capacity_mb
        + shared_buffer_capacity_mb
        + data_file_cache_ring_buffer_capacity_mb
        + meta_file_cache_ring_buffer_capacity_mb
        + compactor_memory_limit_mb;
    let soft_limit_mb = (non_reserved_memory_bytes as f64
        * (storage_memory_proportion + compactor_memory_proportion).ceil())
        as usize
        >> 20;
    // + 5 because ceil is used when calculating `total_bytes`.
    if total_calculated_mb > soft_limit_mb + 5 {
        tracing::warn!(
            "The storage memory ({}) exceeds soft limit ({}).",
            convert((total_calculated_mb << 20) as _),
            convert((soft_limit_mb << 20) as _)
        );
    }

    StorageMemoryConfig {
        block_cache_capacity_mb,
        meta_cache_capacity_mb,
        shared_buffer_capacity_mb,
        data_file_cache_ring_buffer_capacity_mb,
        meta_file_cache_ring_buffer_capacity_mb,
        compactor_memory_limit_mb,
        prefetch_buffer_capacity_mb,
        high_priority_ratio_in_percent,
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::config::StorageConfig;

    use super::{reserve_memory_bytes, storage_memory_config};

    #[test]
    fn test_reserve_memory_bytes() {
        // at least 512 MB
        let (reserved, non_reserved) = reserve_memory_bytes(1536 << 20);
        assert_eq!(reserved, 512 << 20);
        assert_eq!(non_reserved, 1024 << 20);

        // reserve based on proportion
        let (reserved, non_reserved) = reserve_memory_bytes(10 << 30);
        assert_eq!(reserved, 3 << 30);
        assert_eq!(non_reserved, 7 << 30);
    }

    #[test]
    fn test_storage_memory_config() {
        let mut storage_config = StorageConfig::default();

        let total_non_reserved_memory_bytes = 8 << 30;

        let memory_config =
            storage_memory_config(total_non_reserved_memory_bytes, true, &storage_config);
        assert_eq!(memory_config.block_cache_capacity_mb, 737);
        assert_eq!(memory_config.meta_cache_capacity_mb, 860);
        assert_eq!(memory_config.shared_buffer_capacity_mb, 737);
        assert_eq!(memory_config.data_file_cache_ring_buffer_capacity_mb, 0);
        assert_eq!(memory_config.meta_file_cache_ring_buffer_capacity_mb, 0);
        assert_eq!(memory_config.compactor_memory_limit_mb, 819);

        storage_config.data_file_cache.dir = "data".to_string();
        storage_config.meta_file_cache.dir = "meta".to_string();
        let memory_config =
            storage_memory_config(total_non_reserved_memory_bytes, true, &storage_config);
        assert_eq!(memory_config.block_cache_capacity_mb, 737);
        assert_eq!(memory_config.meta_cache_capacity_mb, 860);
        assert_eq!(memory_config.shared_buffer_capacity_mb, 737);
        assert_eq!(memory_config.data_file_cache_ring_buffer_capacity_mb, 256);
        assert_eq!(memory_config.meta_file_cache_ring_buffer_capacity_mb, 256);
        assert_eq!(memory_config.compactor_memory_limit_mb, 819);

        storage_config.block_cache_capacity_mb = Some(512);
        storage_config.meta_cache_capacity_mb = Some(128);
        storage_config.shared_buffer_capacity_mb = Some(1024);
        storage_config.compactor_memory_limit_mb = Some(512);
        let memory_config = storage_memory_config(0, true, &storage_config);
        assert_eq!(memory_config.block_cache_capacity_mb, 512);
        assert_eq!(memory_config.meta_cache_capacity_mb, 128);
        assert_eq!(memory_config.shared_buffer_capacity_mb, 1024);
        assert_eq!(memory_config.data_file_cache_ring_buffer_capacity_mb, 256);
        assert_eq!(memory_config.meta_file_cache_ring_buffer_capacity_mb, 256);
        assert_eq!(memory_config.compactor_memory_limit_mb, 512);
    }
}
