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

#[cfg(feature = "wal-rocksdb")]
pub type RocksDBStorageConfig = crate::rocksdb_impl::config::RocksDBStorageConfig;
#[cfg(not(feature = "wal-rocksdb"))]
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct RocksDBStorageConfig;

#[cfg(feature = "wal-table-kv")]
pub type ObkvStorageConfig = crate::table_kv_impl::config::ObkvStorageConfig;
#[cfg(not(feature = "wal-table-kv"))]
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct ObkvStorageConfig;

#[cfg(feature = "wal-message-queue")]
pub type KafkaStorageConfig = crate::message_queue_impl::config::KafkaStorageConfig;
#[cfg(not(feature = "wal-message-queue"))]
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct KafkaStorageConfig;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    // The flatten attribute inlines keys from a field into the parent struct.
    // That's to say `storage` has no real usage, it's just a placeholder.
    #[serde(flatten)]
    pub storage: StorageConfig,
    /// If true, data wal will return Ok directly, without any IO operations.
    // Note: this is only used for test, we shouldn't enable this in production.
    #[serde(default)]
    pub disable_data: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            storage: StorageConfig::RocksDB(Box::default()),
            disable_data: false,
        }
    }
}

/// Options for wal storage backend
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum StorageConfig {
    RocksDB(Box<RocksDBStorageConfig>),
    Obkv(Box<ObkvStorageConfig>),
    Kafka(Box<KafkaStorageConfig>),
}
