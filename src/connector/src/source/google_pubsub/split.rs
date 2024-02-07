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

use risingwave_common::types::JsonbVal;
use serde::{Deserialize, Serialize};

use crate::error::ConnectorResult;
use crate::source::{SplitId, SplitMetaData};

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct PubsubSplit {
    pub(crate) index: u32,
    pub(crate) subscription: String,

    /// `start_offset` is a numeric timestamp.
    /// When not `None`, the PubsubReader seeks to the timestamp described by the start_offset.
    /// These offsets are taken from the `offset` property of the SourceMessage yielded by the
    /// pubsub reader.
    pub(crate) start_offset: Option<String>,

    /// `stop_offset` is a numeric timestamp.
    /// When not `None`, the PubsubReader stops reading messages when the `offset` property of
    /// the SourceMessage is greater than or equal to the stop_offset.
    pub(crate) stop_offset: Option<String>,
}

impl SplitMetaData for PubsubSplit {
    fn restore_from_json(value: JsonbVal) -> ConnectorResult<Self> {
        serde_json::from_value(value.take()).map_err(Into::into)
    }

    fn encode_to_json(&self) -> JsonbVal {
        serde_json::to_value(self.clone()).unwrap().into()
    }

    fn id(&self) -> SplitId {
        format!("{}-{}", self.subscription, self.index).into()
    }

    fn update_with_offset(&mut self, start_offset: String) -> ConnectorResult<()> {
        self.start_offset = Some(start_offset);
        Ok(())
    }
}
