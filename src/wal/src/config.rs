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

use crate::{
    message_queue_impl::config::KafkaStorageConfig, rocksdb_impl::config::RocksDBStorageConfig,
    table_kv_impl::config::ObkvStorageConfig,
};

/// Options for wal storage backend
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum StorageConfig {
    RocksDB(Box<RocksDBStorageConfig>),
    Obkv(Box<ObkvStorageConfig>),
    Kafka(Box<KafkaStorageConfig>),
}
