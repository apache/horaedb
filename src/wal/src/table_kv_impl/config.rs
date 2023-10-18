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

use serde::{Deserialize, Serialize};
use table_kv::config::ObkvConfig;
use time_ext::ReadableDuration;

use crate::table_kv_impl::model::NamespaceConfig;

/// Config of wal based on obkv
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ObkvStorageConfig {
    /// Obkv client config
    pub obkv: ObkvConfig,
    /// Namespace config for data.
    pub data_namespace: WalNamespaceConfig,
    /// Namespace config for meta data
    pub meta_namespace: ManifestNamespaceConfig,
}

/// Config of obkv wal based manifest
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ManifestNamespaceConfig {
    /// Decide how many wal data shards will be created
    ///
    /// NOTICE: it can just be set once, the later setting makes no effect.
    pub shard_num: usize,

    /// Decide how many wal meta shards will be created
    ///
    /// NOTICE: it can just be set once, the later setting makes no effect.
    pub meta_shard_num: usize,

    pub init_scan_timeout: ReadableDuration,
    pub init_scan_batch_size: i32,
    pub clean_scan_timeout: ReadableDuration,
    pub clean_scan_batch_size: usize,
    pub bucket_create_parallelism: usize,
}

impl Default for ManifestNamespaceConfig {
    fn default() -> Self {
        let namespace_config = NamespaceConfig::default();

        Self {
            shard_num: namespace_config.wal_shard_num,
            meta_shard_num: namespace_config.table_unit_meta_shard_num,
            init_scan_timeout: namespace_config.init_scan_timeout,
            init_scan_batch_size: namespace_config.init_scan_batch_size,
            clean_scan_timeout: namespace_config.clean_scan_timeout,
            clean_scan_batch_size: namespace_config.clean_scan_batch_size,
            bucket_create_parallelism: namespace_config.bucket_create_parallelism,
        }
    }
}

impl From<ManifestNamespaceConfig> for NamespaceConfig {
    fn from(manifest_config: ManifestNamespaceConfig) -> Self {
        NamespaceConfig {
            wal_shard_num: manifest_config.shard_num,
            table_unit_meta_shard_num: manifest_config.meta_shard_num,
            ttl: None,
            init_scan_timeout: manifest_config.init_scan_timeout,
            init_scan_batch_size: manifest_config.init_scan_batch_size,
            clean_scan_timeout: manifest_config.clean_scan_timeout,
            clean_scan_batch_size: manifest_config.clean_scan_batch_size,
            bucket_create_parallelism: manifest_config.bucket_create_parallelism,
        }
    }
}

/// Config of obkv wal based wal module
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WalNamespaceConfig {
    /// Decide how many wal data shards will be created
    ///
    /// NOTICE: it can just be set once, the later setting makes no effect.
    pub shard_num: usize,

    /// Decide how many wal meta shards will be created
    ///
    /// NOTICE: it can just be set once, the later setting makes no effect.
    pub meta_shard_num: usize,

    pub ttl: ReadableDuration,
    pub init_scan_timeout: ReadableDuration,
    pub init_scan_batch_size: i32,
    pub bucket_create_parallelism: usize,
}

impl Default for WalNamespaceConfig {
    fn default() -> Self {
        let namespace_config = NamespaceConfig::default();

        Self {
            shard_num: namespace_config.wal_shard_num,
            meta_shard_num: namespace_config.table_unit_meta_shard_num,
            ttl: namespace_config.ttl.unwrap(),
            init_scan_timeout: namespace_config.init_scan_timeout,
            init_scan_batch_size: namespace_config.init_scan_batch_size,
            bucket_create_parallelism: namespace_config.bucket_create_parallelism,
        }
    }
}

impl From<WalNamespaceConfig> for NamespaceConfig {
    fn from(wal_config: WalNamespaceConfig) -> Self {
        Self {
            wal_shard_num: wal_config.shard_num,
            table_unit_meta_shard_num: wal_config.meta_shard_num,
            ttl: Some(wal_config.ttl),
            init_scan_timeout: wal_config.init_scan_timeout,
            init_scan_batch_size: wal_config.init_scan_batch_size,
            bucket_create_parallelism: wal_config.bucket_create_parallelism,
            ..Default::default()
        }
    }
}
