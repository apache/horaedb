// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use size_ext::ReadableSize;
use time_ext::ReadableDuration;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
/// Options for storage backend
pub struct StorageOptions {
    // 0 means disable mem cache
    pub mem_cache_capacity: ReadableSize,
    pub mem_cache_partition_bits: usize,
    // 0 means disable disk cache
    // Note: disk_cache_capacity % (disk_cache_page_size * (1 << disk_cache_partition_bits)) should
    // be 0
    pub disk_cache_capacity: ReadableSize,
    pub disk_cache_page_size: ReadableSize,
    pub disk_cache_partition_bits: usize,
    pub disk_cache_dir: String,
    pub object_store: ObjectStoreOptions,
}

impl Default for StorageOptions {
    fn default() -> Self {
        let root_path = "/tmp/horaedb".to_string();

        StorageOptions {
            mem_cache_capacity: ReadableSize::mb(512),
            mem_cache_partition_bits: 6,
            disk_cache_dir: root_path.clone(),
            disk_cache_capacity: ReadableSize::gb(0),
            disk_cache_page_size: ReadableSize::mb(2),
            disk_cache_partition_bits: 4,
            object_store: ObjectStoreOptions::Local(LocalOptions::new_with_default(root_path)),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[allow(clippy::large_enum_variant)]
pub enum ObjectStoreOptions {
    Local(LocalOptions),
    Aliyun(AliyunOptions),
    S3(S3Options),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LocalOptions {
    pub data_dir: String,
    #[serde(default = "default_max_retries")]
    pub max_retries: usize,
    #[serde(default)]
    pub timeout: TimeoutOptions,
}

impl LocalOptions {
    pub fn new_with_default(data_dir: String) -> Self {
        Self {
            data_dir,
            max_retries: default_max_retries(),
            timeout: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AliyunOptions {
    pub key_id: String,
    pub key_secret: String,
    pub endpoint: String,
    pub bucket: String,
    pub prefix: String,
    #[serde(default = "default_max_retries")]
    pub max_retries: usize,
    #[serde(default)]
    pub http: HttpOptions,
    #[serde(default)]
    pub timeout: TimeoutOptions,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct S3Options {
    pub region: String,
    pub key_id: String,
    pub key_secret: String,
    pub endpoint: String,
    pub bucket: String,
    pub prefix: String,
    #[serde(default = "default_max_retries")]
    pub max_retries: usize,
    #[serde(default)]
    pub http: HttpOptions,
    #[serde(default)]
    pub timeout: TimeoutOptions,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HttpOptions {
    pub pool_max_idle_per_host: usize,
    pub timeout: ReadableDuration,
    pub keep_alive_timeout: ReadableDuration,
    pub keep_alive_interval: ReadableDuration,
}

impl Default for HttpOptions {
    fn default() -> Self {
        Self {
            pool_max_idle_per_host: 1024,
            timeout: ReadableDuration::from(Duration::from_secs(60)),
            keep_alive_timeout: ReadableDuration::from(Duration::from_secs(60)),
            keep_alive_interval: ReadableDuration::from(Duration::from_secs(2)),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TimeoutOptions {
    // Non IO Operation like stat and delete, they operate on a single file, we control them by
    // setting timeout.
    pub timeout: ReadableDuration,
    // IO Operation like read and write, they operate on data directly, we control them by setting
    // io_timeout.
    pub io_timeout: ReadableDuration,
}

impl Default for TimeoutOptions {
    fn default() -> Self {
        Self {
            timeout: ReadableDuration::from(Duration::from_secs(10)),
            io_timeout: ReadableDuration::from(Duration::from_secs(10)),
        }
    }
}

#[inline]
fn default_max_retries() -> usize {
    3
}
