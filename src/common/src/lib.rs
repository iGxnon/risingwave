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

#![expect(
    refining_impl_trait,
    reason = "Some of the Row::iter() implementations returns ExactSizeIterator. Is this reasonable?"
)]
#![feature(extract_if)]
#![feature(trait_alias)]
#![feature(is_sorted)]
#![feature(type_alias_impl_trait)]
#![feature(test)]
#![feature(trusted_len)]
#![feature(allocator_api)]
#![feature(lint_reasons)]
#![feature(coroutines)]
#![feature(map_try_insert)]
#![feature(lazy_cell)]
#![feature(error_generic_member_access)]
#![feature(let_chains)]
#![feature(portable_simd)]
#![feature(array_chunks)]
#![feature(inline_const_pat)]
#![allow(incomplete_features)]
#![feature(iterator_try_collect)]
#![feature(round_ties_even)]
#![feature(iter_order_by)]
#![feature(exclusive_range_pattern)]
#![feature(binary_heap_into_iter_sorted)]
#![feature(impl_trait_in_assoc_type)]
#![feature(map_entry_replace)]
#![feature(negative_impls)]
#![feature(bound_map)]
#![feature(array_methods)]
#![feature(btree_cursors)]

#[cfg_attr(not(test), expect(unused_extern_crates))]
extern crate self as risingwave_common;

#[macro_use]
pub mod jemalloc;
#[macro_use]
pub mod error;
#[macro_use]
pub mod array;
#[macro_use]
pub mod util;
pub mod acl;
pub mod buffer;
pub mod cache;
pub mod cast;
pub mod catalog;
pub mod config;
pub mod constants;
pub mod estimate_size;
pub mod field_generator;
pub mod hash;
pub mod log;
pub mod memory;
pub mod metrics;
pub mod monitor;
pub mod opts;
pub mod range;
pub mod row;
pub mod session_config;
pub mod system_param;
pub mod telemetry;
pub mod test_utils;
pub mod transaction;
pub mod types;
pub mod vnode_mapping;
pub mod test_prelude {
    pub use super::array::{DataChunkTestExt, StreamChunkTestExt};
    pub use super::catalog::test_utils::ColumnDescTestExt;
}

pub const RW_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Placeholder for unknown git sha.
pub const UNKNOWN_GIT_SHA: &str = "unknown";

// The single source of truth of the pg parameters, Used in ConfigMap and current_cluster_version.
// The version of PostgreSQL that Risingwave claims to be.
pub const PG_VERSION: &str = "9.5.0";
/// The version of PostgreSQL that Risingwave claims to be.
pub const SERVER_VERSION_NUM: i32 = 90500;
/// Shows the server-side character set encoding. At present, this parameter can be shown but not set, because the encoding is determined at database creation time. It is also the default value of `client_encoding`.
pub const SERVER_ENCODING: &str = "UTF8";
/// see <https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-STANDARD-CONFORMING-STRINGS>
pub const STANDARD_CONFORMING_STRINGS: &str = "on";

#[macro_export]
macro_rules! git_sha {
    ($env:literal) => {
        match option_env!($env) {
            Some(v) if !v.is_empty() => v,
            _ => $crate::UNKNOWN_GIT_SHA,
        }
    };
}

// FIXME: We expand `unwrap_or` since it's unavailable in const context now.
// `const_option_ext` was broken by https://github.com/rust-lang/rust/pull/110393
// Tracking issue: https://github.com/rust-lang/rust/issues/91930
pub const GIT_SHA: &str = git_sha!("GIT_SHA");

pub fn current_cluster_version() -> String {
    format!(
        "PostgreSQL {}-RisingWave-{} ({})",
        PG_VERSION, RW_VERSION, GIT_SHA
    )
}
