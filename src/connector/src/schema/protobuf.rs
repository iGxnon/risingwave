// Copyright 2023 RisingWave Labs
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

use prost_reflect::MessageDescriptor;

use super::schema_registry::{Client, handle_sr_list};
use super::{SchemaFetchError, MESSAGE_NAME_KEY, SCHEMA_LOCATION_KEY, SCHEMA_REGISTRY_KEY};
use crate::common::AwsAuthProps;
use crate::parser::{EncodingProperties, ProtobufParserConfig, ProtobufProperties};
use crate::schema::NAME_STRATEGY_KEY;
use crate::schema::schema_registry::{name_strategy_from_str, get_subject_by_strategy};

/// `aws_auth_props` is only required when reading `s3://` URL.
pub async fn fetch_descriptor(
    format_options: &BTreeMap<String, String>,
    aws_auth_props: Option<&AwsAuthProps>,
) -> Result<MessageDescriptor, SchemaFetchError> {
    let message_name = format_options
        .get(MESSAGE_NAME_KEY)
        .ok_or_else(|| SchemaFetchError(format!("{MESSAGE_NAME_KEY} required")))?
        .clone();
    let schema_location = format_options
        .get(SCHEMA_LOCATION_KEY);
    let schema_registry = format_options
        .get(SCHEMA_REGISTRY_KEY);
    let row_schema_location = match (schema_location, schema_registry) {
        (Some(_), Some(_)) => return Err(SchemaFetchError(format!("cannot use {SCHEMA_LOCATION_KEY} and {SCHEMA_REGISTRY_KEY} together"))),
        (None, None) => return Err(SchemaFetchError(format!("requires one of {SCHEMA_LOCATION_KEY} or {SCHEMA_REGISTRY_KEY}"))),
        (None, Some(url)) => {
            return fetch_from_registry(url, &message_name, format_options).await
        }
        (Some(url), None) => url.clone(),
    };

    if row_schema_location.starts_with("s3") && aws_auth_props.is_none() {
        return Err(SchemaFetchError("s3 URL not supported yet".into()));
    }

    let enc = EncodingProperties::Protobuf(ProtobufProperties {
        use_schema_registry: false,
        row_schema_location,
        message_name,
        aws_auth_props: aws_auth_props.cloned(),
        // name_strategy, topic, key_message_name, enable_upsert, client_config
        ..Default::default()
    });
    // Ideally, we should extract the schema loading logic from source parser to this place,
    // and call this in both source and sink.
    // But right now this function calls into source parser for its schema loading functionality.
    // This reversed dependency will be fixed when we support schema registry.
    let conf = ProtobufParserConfig::new(enc)
        .await
        .map_err(|e| SchemaFetchError(e.to_string()))?;
    Ok(conf.message_descriptor)
}

pub async fn fetch_from_registry(
    url: &str,
    message_name: &str,
    format_options: &BTreeMap<String, String>,
) -> Result<MessageDescriptor, SchemaFetchError> {
    let client_config = format_options.into();
    let name_strategy = format_options
        .get(NAME_STRATEGY_KEY)
        .map(|s| {
            name_strategy_from_str(s)
                .ok_or_else(|| SchemaFetchError(format!("unrecognized strategy {s}")))
        })
        .transpose()?
        .unwrap_or_default();
    let topic = ""; // TODO

    let val_subject = get_subject_by_strategy(&name_strategy, topic, Some(message_name), false)?;

    let urls = handle_sr_list(url)?;
    let client = Client::new(urls, &client_config)?;
    compile_file_descriptor_from_schema_registry(&val_subject, &client).await?
}
