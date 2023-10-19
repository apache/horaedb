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

//! utilities for testing wal module.

use std::{
    collections::{HashMap, VecDeque},
    path::Path,
    str::FromStr,
    sync::Arc,
};

use async_trait::async_trait;
use common_types::{table::TableId, SequenceNumber};
use message_queue::kafka::{config::Config as KafkaConfig, kafka_impl::KafkaImpl};
use runtime::{self, Runtime};
use table_kv::memory::MemoryImpl;
use tempfile::TempDir;
use time_ext::ReadableDuration;
use wal::{
    kv_encoder::LogBatchEncoder,
    log_batch::{LogWriteBatch, MemoryPayload, MemoryPayloadDecoder},
    manager::{
        BatchLogIteratorAdapter, ReadContext, WalLocation, WalManager, WalManagerRef, WalRuntimes,
        WriteContext,
    },
    message_queue_impl::{config::KafkaWalConfig, wal::MessageQueueImpl},
    rocksdb_impl::manager::RocksImpl,
    table_kv_impl::{model::NamespaceConfig, wal::WalNamespaceImpl},
};
