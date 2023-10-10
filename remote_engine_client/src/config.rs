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

//! Config for [Client]

use arrow_ext::ipc::CompressOptions;
use serde::{Deserialize, Serialize};
use time_ext::ReadableDuration;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    pub connect_timeout: ReadableDuration,
    pub channel_pool_max_size_per_partition: usize,
    pub channel_pool_partition_num: usize,
    pub channel_keep_alive_while_idle: bool,
    pub channel_keep_alive_timeout: ReadableDuration,
    pub channel_keep_alive_interval: ReadableDuration,
    pub route_cache_max_size_per_partition: usize,
    pub route_cache_partition_num: usize,
    pub compression: CompressOptions,
    pub max_retry: usize,
    pub retry_interval: ReadableDuration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            connect_timeout: ReadableDuration::secs(3),
            channel_pool_max_size_per_partition: 16,
            channel_pool_partition_num: 16,
            channel_keep_alive_interval: ReadableDuration::secs(600),
            channel_keep_alive_timeout: ReadableDuration::secs(3),
            channel_keep_alive_while_idle: true,
            route_cache_max_size_per_partition: 16,
            route_cache_partition_num: 16,
            compression: CompressOptions::default(),
            max_retry: 5,
            retry_interval: ReadableDuration::secs(5),
        }
    }
}
