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

use risingwave_common::types::{DataType, Fields};
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::expr::cast_map_array;

/// The catalog `pg_cast` stores data type conversion paths.
/// Ref: [`https://www.postgresql.org/docs/current/catalog-pg-cast.html`]
#[derive(Fields)]
struct PgCast {
    oid: i32,
    castsource: i32,
    casttarget: i32,
    castcontext: String,
}

#[system_catalog(table, "pg_catalog.pg_cast")]
fn read_pg_cast(_: &SysCatalogReaderImpl) -> Vec<PgCast> {
    let mut cast_array = cast_map_array();
    cast_array.sort();
    cast_array
        .iter()
        .enumerate()
        .map(|(idx, (src, target, ctx))| PgCast {
            oid: idx as i32,
            castsource: DataType::from(*src).to_oid(),
            casttarget: DataType::from(*target).to_oid(),
            castcontext: ctx.to_string(),
        })
        .collect()
}
