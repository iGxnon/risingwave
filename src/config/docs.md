# RisingWave System Configurations

This page is automatically generated by `./risedev generate-example-config`

## batch

| Config | Description | Default |
|--------|-------------|---------|
| distributed_query_limit |  |  |
| enable_barrier_read |  | false |
| frontend_compute_runtime_worker_threads |  frontend compute runtime worker threads | 4 |
| statement_timeout_in_sec |  Timeout for a batch query in seconds. | 3600 |
| worker_threads_num |  The thread number of the batch task runtime in the compute node. The default value is  decided by `tokio`. |  |

## meta

| Config | Description | Default |
|--------|-------------|---------|
| backend |  | "Mem" |
| collect_gc_watermark_spin_interval_sec |  The spin interval when collecting global GC watermark in hummock. | 5 |
| compaction_task_max_heartbeat_interval_secs |  | 30 |
| compaction_task_max_progress_interval_secs |  | 600 |
| cut_table_size_limit |  | 1073741824 |
| dangerous_max_idle_secs |  After specified seconds of idle (no mview or flush), the process will be exited.  It is mainly useful for playgrounds. |  |
| default_parallelism |  The default global parallelism for all streaming jobs, if user doesn't specify the  parallelism, this value will be used. `FULL` means use all available parallelism units,  otherwise it's a number. | "Full" |
| disable_automatic_parallelism_control |  Whether to disable adaptive-scaling feature. | false |
| disable_recovery |  Whether to enable fail-on-recovery. Should only be used in e2e tests. | false |
| do_not_config_object_storage_lifecycle |  Whether config object storage bucket lifecycle to purge stale data. | false |
| enable_committed_sst_sanity_check |  Enable sanity check when SSTs are committed. | false |
| enable_compaction_deterministic |  Whether to enable deterministic compaction scheduling, which  will disable all auto scheduling of compaction tasks.  Should only be used in e2e tests. | false |
| enable_hummock_data_archive |  If enabled, SSTable object file and version delta will be retained.   SSTable object file need to be deleted via full GC.   version delta need to be manually deleted. | false |
| event_log_channel_max_size |  Keeps the latest N events per channel. | 10 |
| event_log_enabled |  | true |
| full_gc_interval_sec |  Interval of automatic hummock full GC. | 86400 |
| hummock_version_checkpoint_interval_sec |  Interval of hummock version checkpoint. | 30 |
| hybird_partition_vnode_count |  | 4 |
| max_heartbeat_interval_secs |  Maximum allowed heartbeat interval in seconds. | 300 |
| meta_leader_lease_secs |  | 30 |
| min_delta_log_num_for_hummock_version_checkpoint |  The minimum delta log number a new checkpoint should compact, otherwise the checkpoint  attempt is rejected. | 10 |
| min_sst_retention_time_sec |  Objects within `min_sst_retention_time_sec` won't be deleted by hummock full GC, even they  are dangling. | 86400 |
| min_table_split_write_throughput |  If the size of one table is smaller than `min_table_split_write_throughput`, we would not  split it to an single group. | 4194304 |
| move_table_size_limit |  | 10737418240 |
| node_num_monitor_interval_sec |  | 10 |
| partition_vnode_count |  | 16 |
| periodic_compaction_interval_sec |  Schedule compaction for all compaction groups with this interval. | 60 |
| periodic_space_reclaim_compaction_interval_sec |  Schedule space_reclaim compaction for all compaction groups with this interval. | 3600 |
| periodic_split_compact_group_interval_sec |  | 10 |
| periodic_tombstone_reclaim_compaction_interval_sec |  | 600 |
| periodic_ttl_reclaim_compaction_interval_sec |  Schedule ttl_reclaim compaction for all compaction groups with this interval. | 1800 |
| split_group_size_limit |  | 68719476736 |
| table_write_throughput_threshold |  | 16777216 |
| unrecognized |  |  |
| vacuum_interval_sec |  Interval of invoking a vacuum job, to remove stale metadata from meta store and objects  from object store. | 30 |
| vacuum_spin_interval_ms |  The spin interval inside a vacuum job. It avoids the vacuum job monopolizing resources of  meta node. | 10 |

## meta.compaction_config

| Config | Description | Default |
|--------|-------------|---------|
| compaction_filter_mask |  | 6 |
| enable_emergency_picker |  | true |
| level0_max_compact_file_number |  | 100 |
| level0_overlapping_sub_level_compact_level_count |  | 12 |
| level0_stop_write_threshold_sub_level_number |  | 300 |
| level0_sub_level_compact_level_count |  | 3 |
| level0_tier_compact_file_number |  | 12 |
| max_bytes_for_level_base |  | 536870912 |
| max_bytes_for_level_multiplier |  | 5 |
| max_compaction_bytes |  | 2147483648 |
| max_space_reclaim_bytes |  | 536870912 |
| max_sub_compaction |  | 4 |
| sub_level_max_compaction_bytes |  | 134217728 |
| target_file_size_base |  | 33554432 |
| tombstone_reclaim_ratio |  | 40 |

## server

| Config | Description | Default |
|--------|-------------|---------|
| connection_pool_size |  | 16 |
| grpc_max_reset_stream |  | 200 |
| heap_profiling |  Enable heap profile dump when memory usage is high. |  |
| heartbeat_interval_ms |  The interval for periodic heartbeat from worker to the meta service. | 1000 |
| metrics_level |  Used for control the metrics level, similar to log level. | "Info" |
| telemetry_enabled |  | true |

## storage

| Config | Description | Default |
|--------|-------------|---------|
| block_cache_capacity_mb |  Capacity of sstable block cache. |  |
| cache_refill |  |  |
| check_compaction_result |  | false |
| compact_iter_recreate_timeout_ms |  | 600000 |
| compactor_fast_max_compact_delete_ratio |  | 40 |
| compactor_fast_max_compact_task_size |  | 2147483648 |
| compactor_max_sst_key_count |  | 2097152 |
| compactor_max_sst_size |  | 536870912 |
| compactor_max_task_multiplier |  Compactor calculates the maximum number of tasks that can be executed on the node based on  worker_num and compactor_max_task_multiplier.  max_pull_task_count = worker_num * compactor_max_task_multiplier | 2.5 |
| compactor_memory_available_proportion |  The percentage of memory available when compactor is deployed separately.  non_reserved_memory_bytes = system_memory_available_bytes * compactor_memory_available_proportion | 0.8 |
| compactor_memory_limit_mb |  |  |
| data_file_cache |  |  |
| disable_remote_compactor |  | false |
| enable_fast_compaction |  | false |
| high_priority_ratio_in_percent |  |  |
| imm_merge_threshold |  The threshold for the number of immutable memtables to merge to a new imm. | 0 |
| max_concurrent_compaction_task_number |  | 16 |
| max_prefetch_block_number |  max prefetch block number | 16 |
| max_preload_io_retry_times |  | 3 |
| max_preload_wait_time_mill |  | 0 |
| max_sub_compaction |  Max sub compaction task numbers | 4 |
| max_version_pinning_duration_sec |  | 10800 |
| mem_table_spill_threshold |  The spill threshold for mem table. | 0 |
| meta_cache_capacity_mb |  Capacity of sstable meta cache. |  |
| meta_file_cache |  |  |
| min_sst_size_for_streaming_upload |  Whether to enable streaming upload for sstable. | 33554432 |
| object_store |  |  |
| prefetch_buffer_capacity_mb |  max memory usage for large query |  |
| share_buffer_compaction_worker_threads_number |  Worker threads number of dedicated tokio runtime for share buffer compaction. 0 means use  tokio's default value (number of CPU core). | 4 |
| share_buffer_upload_concurrency |  Number of tasks shared buffer can upload in parallel. | 8 |
| share_buffers_sync_parallelism |  parallelism while syncing share buffers into L0 SST. Should NOT be 0. | 1 |
| shared_buffer_capacity_mb |  Maximum shared buffer size, writes attempting to exceed the capacity will stall until there  is enough space. |  |
| shared_buffer_flush_ratio |  The shared buffer will start flushing data to object when the ratio of memory usage to the  shared buffer capacity exceed such ratio. | 0.800000011920929 |
| sstable_id_remote_fetch_number |  Number of SST ids fetched from meta per RPC | 10 |
| write_conflict_detection_enabled |  Whether to enable write conflict detection | true |

## streaming

| Config | Description | Default |
|--------|-------------|---------|
| actor_runtime_worker_threads_num |  The thread number of the streaming actor runtime in the compute node. The default value is  decided by `tokio`. |  |
| async_stack_trace |  Enable async stack tracing through `await-tree` for risectl. | "ReleaseVerbose" |
| in_flight_barrier_nums |  The maximum number of barriers in-flight in the compute nodes. | 10000 |
| unique_user_stream_errors |  Max unique user stream errors per actor | 10 |

## system

| Config | Description | Default |
|--------|-------------|---------|
| backup_storage_directory | Remote directory for storing snapshots. |  |
| backup_storage_url | Remote storage url for storing snapshots. |  |
| barrier_interval_ms | The interval of periodic barrier. | 1000 |
| block_size_kb | Size of each block in bytes in SST. | 64 |
| bloom_false_positive | False positive probability of bloom filter. | 0.001 |
| checkpoint_frequency | There will be a checkpoint for every n barriers. | 1 |
| data_directory | Remote directory for storing data and metadata objects. |  |
| enable_tracing | Whether to enable distributed tracing. | false |
| max_concurrent_creating_streaming_jobs | Max number of concurrent creating streaming jobs. | 1 |
| parallel_compact_size_mb |  | 512 |
| pause_on_next_bootstrap | Whether to pause all data sources on next bootstrap. | false |
| sstable_size_mb | Target size of the Sstable. | 256 |
| state_store |  |  |
| wasm_storage_url |  | "fs://.risingwave/data" |
