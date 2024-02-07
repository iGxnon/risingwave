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

use std::iter;
use std::path::Path;

use anyhow::Context;
use itertools::Itertools;
use protobuf_native::compiler::{
    SimpleErrorCollector, SourceTreeDescriptorDatabase, VirtualSourceTree,
};
use protobuf_native::MessageLite;

use crate::error::ConnectorResult;
use crate::schema::schema_registry::Client;

macro_rules! embed_wkts {
    [$( $path:literal ),+ $(,)?] => {
        &[$(
            (
                concat!("google/protobuf/", $path),
                include_bytes!(concat!(env!("PROTO_INCLUDE"), "/google/protobuf/", $path)).as_slice(),
            )
        ),+]
    };
}
const WELL_KNOWN_TYPES: &[(&str, &[u8])] = embed_wkts![
    "any.proto",
    "api.proto",
    "compiler/plugin.proto",
    "descriptor.proto",
    "duration.proto",
    "empty.proto",
    "field_mask.proto",
    "source_context.proto",
    "struct.proto",
    "timestamp.proto",
    "type.proto",
    "wrappers.proto",
];

// Pull protobuf schema and all it's deps from the confluent schema registry,
// and compile then into one file descriptor
pub(super) async fn compile_file_descriptor_from_schema_registry(
    subject_name: &str,
    client: &Client,
) -> ConnectorResult<Vec<u8>> {
    let (primary_subject, dependency_subjects) = client
        .get_subject_and_references(subject_name)
        .await
        .with_context(|| format!("failed to resolve subject `{subject_name}`"))?;

    // Compile .proto files into a file descriptor set.
    let mut source_tree = VirtualSourceTree::new();
    for subject in iter::once(&primary_subject).chain(dependency_subjects.iter()) {
        source_tree.as_mut().add_file(
            Path::new(&subject.name),
            subject.schema.content.as_bytes().to_vec(),
        );
    }
    for (path, bytes) in WELL_KNOWN_TYPES {
        source_tree
            .as_mut()
            .add_file(Path::new(path), bytes.to_vec());
    }

    let mut error_collector = SimpleErrorCollector::new();
    // `db` needs to be dropped before we can iterate on `error_collector`.
    let fds = {
        let mut db = SourceTreeDescriptorDatabase::new(source_tree.as_mut());
        db.as_mut().record_errors_to(error_collector.as_mut());
        db.as_mut()
            .build_file_descriptor_set(&[Path::new(&primary_subject.name)])
    }
    .with_context(|| {
        format!(
            "build_file_descriptor_set failed. Errors:\n{}",
            error_collector.as_mut().join("\n")
        )
    })?;

    let serialized = fds.serialize().context("serialize descriptor set failed")?;
    Ok(serialized)
}
