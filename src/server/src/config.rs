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

use common::ReadableDuration;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub port: u16,
    pub test: TestConfig, // for test
    pub metric_engine: MetricEngineConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            port: 5000,
            test: TestConfig::default(),
            metric_engine: MetricEngineConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct TestConfig {
    pub enable_write: bool,
    pub write_worker_num: usize,
    pub write_interval: ReadableDuration,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            enable_write: true,
            write_worker_num: 1,
            write_interval: ReadableDuration::millis(500),
        }
    }
}

#[derive(Default, Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct MetricEngineConfig {
    pub manifest: ManifestConfig,
    pub sst: SstConfig,
    pub storage: StorageConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ManifestConfig {
    pub background_thread_num: usize,
}

impl Default for ManifestConfig {
    fn default() -> Self {
        Self {
            background_thread_num: 2,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SstConfig {
    pub background_thread_num: usize,
}

impl Default for SstConfig {
    fn default() -> Self {
        Self {
            background_thread_num: 2,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", deny_unknown_fields)]
#[allow(clippy::large_enum_variant)]
pub enum StorageConfig {
    Local(LocalStorageConfig),
    S3Like(S3LikeStorageConfig),
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self::Local(LocalStorageConfig::default())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LocalStorageConfig {
    pub data_dir: String,
}

impl Default for LocalStorageConfig {
    fn default() -> Self {
        Self {
            data_dir: "/tmp/horaedb".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct S3LikeStorageConfig {
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
#[serde(default, deny_unknown_fields)]
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
            timeout: ReadableDuration::secs(15),
            keep_alive_timeout: ReadableDuration::secs(10),
            keep_alive_interval: ReadableDuration::secs(2),
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
            timeout: ReadableDuration::secs(10),
            io_timeout: ReadableDuration::secs(10),
        }
    }
}

#[inline]
fn default_max_retries() -> usize {
    3
}
