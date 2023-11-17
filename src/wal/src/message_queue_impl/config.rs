// Copyright 2023 The CeresDB Authors
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

//! Config for wal on message queue

use message_queue::kafka::config::Config as KafkaConfig;
use serde::{Deserialize, Serialize};
use time_ext::ReadableDuration;

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct KafkaStorageConfig {
    /// Kafka client config
    pub kafka: KafkaConfig,
    /// If true, data wal will return Ok directly, without any IO operations.
    pub disable_data: bool,
    /// Namespace config for data.
    pub data_namespace: KafkaWalConfig,
    /// Namespace config for meta data
    pub meta_namespace: KafkaWalConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct KafkaWalConfig {
    pub clean_period: ReadableDuration,
}

impl Default for KafkaWalConfig {
    fn default() -> Self {
        Self {
            clean_period: ReadableDuration::millis(3600 * 1000),
        }
    }
}
