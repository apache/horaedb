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
use bytes_ext::{BufMut, SafeBuf, SafeBufMut};
use common_types::{table::TableId, SequenceNumber};
use message_queue::kafka::{config::Config as KafkaConfig, kafka_impl::KafkaImpl};
use runtime::{self, Runtime};
use snafu::Snafu;
use table_kv::memory::MemoryImpl;
use tempfile::TempDir;
use time_ext::ReadableDuration;

use crate::{
    kv_encoder::LogBatchEncoder,
    log_batch::{LogWriteBatch, Payload, PayloadDecoder},
    manager::{
        BatchLogIteratorAdapter, ReadContext, WalLocation, WalManager, WalManagerRef, WriteContext,
    },
    message_queue_impl::{config::Config, wal::MessageQueueImpl},
    table_kv_impl::{model::NamespaceConfig, wal::WalNamespaceImpl, WalRuntimes},
    tests::read_write::{
        test_complex_read_write, test_move_from_nodes, test_read_with_boundary, test_reopen,
        test_sequence_increase_monotonically_delete_reopen_write,
        test_sequence_increase_monotonically_delete_write,
        test_sequence_increase_monotonically_multiple_writes, test_simple_read_write_default_batch,
        test_simple_read_write_different_batch_size, test_simple_write_delete,
        test_write_delete_half, test_write_delete_multiple_regions, test_write_multiple_regions,
        test_write_scan,
    },
};

pub fn test_all<B: WalBuilder>(builder: B, is_distributed: bool) {
    test_simple_read_write_default_batch(builder.clone());
    test_simple_read_write_different_batch_size(builder.clone());
    test_read_with_boundary(builder.clone());
    test_write_multiple_regions(builder.clone());
    test_reopen(builder.clone());
    test_complex_read_write(builder.clone());
    test_simple_write_delete(builder.clone());
    test_write_delete_half(builder.clone());
    test_write_delete_multiple_regions(builder.clone());
    test_sequence_increase_monotonically_multiple_writes(builder.clone());
    test_sequence_increase_monotonically_delete_write(builder.clone());
    test_sequence_increase_monotonically_delete_reopen_write(builder.clone());
    test_write_scan(builder.clone());
    if is_distributed {
        test_move_from_nodes(builder);
    }
}

#[derive(Debug, Snafu)]
pub enum Error {}

#[async_trait]
pub trait WalBuilder: Clone + Send + Sync + 'static {
    type Wal: WalManager + Send + Sync;

    async fn build(&self, data_path: &Path, runtime: Arc<Runtime>) -> Arc<Self::Wal>;
}

const WAL_NAMESPACE: &str = "wal";

#[derive(Default)]
pub struct MemoryTableWalBuilder {
    table_kv: MemoryImpl,
    ttl: Option<ReadableDuration>,
}

#[async_trait]
impl WalBuilder for MemoryTableWalBuilder {
    type Wal = WalNamespaceImpl<MemoryImpl>;

    async fn build(&self, _data_path: &Path, runtime: Arc<Runtime>) -> Arc<Self::Wal> {
        let config = NamespaceConfig {
            wal_shard_num: 2,
            table_unit_meta_shard_num: 2,
            ttl: self.ttl,
            ..Default::default()
        };

        let wal_runtimes = WalRuntimes {
            read_runtime: runtime.clone(),
            write_runtime: runtime.clone(),
            default_runtime: runtime.clone(),
        };
        let namespace_wal =
            WalNamespaceImpl::open(self.table_kv.clone(), wal_runtimes, WAL_NAMESPACE, config)
                .await
                .unwrap();

        Arc::new(namespace_wal)
    }
}

impl Clone for MemoryTableWalBuilder {
    fn clone(&self) -> Self {
        Self {
            table_kv: MemoryImpl::default(),
            ttl: self.ttl,
        }
    }
}

impl MemoryTableWalBuilder {
    pub fn with_ttl(ttl: &str) -> Self {
        Self {
            table_kv: MemoryImpl::default(),
            ttl: Some(ReadableDuration::from_str(ttl).unwrap()),
        }
    }
}

pub type TableKvTestEnv = TestEnv<MemoryTableWalBuilder>;

pub struct KafkaWalBuilder {
    namespace: String,
}

impl KafkaWalBuilder {
    pub fn new() -> Self {
        Self {
            namespace: format!("test-namespace-{}", uuid::Uuid::new_v4()),
        }
    }
}

impl Default for KafkaWalBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl WalBuilder for KafkaWalBuilder {
    type Wal = MessageQueueImpl<KafkaImpl>;

    async fn build(&self, _data_path: &Path, runtime: Arc<Runtime>) -> Arc<Self::Wal> {
        let mut config = KafkaConfig::default();
        config.client.boost_brokers = Some(vec!["127.0.0.1:9011".to_string()]);
        let kafka_impl = KafkaImpl::new(config).await.unwrap();
        let message_queue_impl = MessageQueueImpl::new(
            self.namespace.clone(),
            kafka_impl,
            runtime.clone(),
            Config::default(),
        );

        Arc::new(message_queue_impl)
    }
}

impl Clone for KafkaWalBuilder {
    fn clone(&self) -> Self {
        Self {
            namespace: format!("test-namespace-{}", uuid::Uuid::new_v4()),
        }
    }
}

/// The environment for testing wal.
pub struct TestEnv<B> {
    pub dir: TempDir,
    pub runtime: Arc<Runtime>,
    pub write_ctx: WriteContext,
    pub read_ctx: ReadContext,
    /// Builder for a specific wal.
    builder: B,
}

impl<B: WalBuilder> TestEnv<B> {
    pub fn new(num_workers: usize, builder: B) -> Self {
        let runtime = runtime::Builder::default()
            .worker_threads(num_workers)
            .enable_all()
            .build()
            .unwrap();

        Self {
            dir: tempfile::tempdir().unwrap(),
            runtime: Arc::new(runtime),
            write_ctx: WriteContext::default(),
            read_ctx: ReadContext::default(),
            builder,
        }
    }

    pub async fn build_wal(&self) -> WalManagerRef {
        self.builder
            .build(self.dir.path(), self.runtime.clone())
            .await
    }

    pub fn build_payload_batch(&self, start: u32, end: u32) -> Vec<TestPayload> {
        (start..end).map(|val| TestPayload { val }).collect()
    }

    /// Build the log batch with [TestPayload].val range [start, end).
    pub async fn build_log_batch(
        &self,
        location: WalLocation,
        start: u32,
        end: u32,
    ) -> (Vec<TestPayload>, LogWriteBatch) {
        let log_entries = (start..end).collect::<Vec<_>>();

        let log_batch_encoder = LogBatchEncoder::create(location);
        let log_batch = log_batch_encoder
            .encode_batch::<TestPayload, u32>(&log_entries)
            .expect("should succeed to encode payloads");

        let payload_batch = self.build_payload_batch(start, end);
        (payload_batch, log_batch)
    }

    // pub async fn check_multiple_log_entries

    /// Check whether the log entries from the iterator equals the
    /// `write_batch`.
    pub async fn check_log_entries(
        &self,
        test_table_datas: Vec<TestTableData>,
        mut iter: BatchLogIteratorAdapter,
    ) {
        let mut table_log_entries: HashMap<TableId, VecDeque<_>> =
            HashMap::with_capacity(test_table_datas.len());

        loop {
            let dec = TestPayloadDecoder;
            let log_entries = iter
                .next_log_entries(dec, VecDeque::new())
                .await
                .expect("should succeed to fetch next log entry");
            if log_entries.is_empty() {
                break;
            }

            for log_entry in log_entries {
                let log_entries = table_log_entries
                    .entry(log_entry.table_id)
                    .or_insert_with(VecDeque::default);
                log_entries.push_back(log_entry);
            }
        }

        for test_table_data in test_table_datas {
            let empty_log_entries = VecDeque::new();
            let log_entries = table_log_entries
                .get(&test_table_data.table_id)
                .unwrap_or(&empty_log_entries);

            assert_eq!(test_table_data.payload_batch.len(), log_entries.len());
            for (idx, (expect_log_write_entry, log_entry)) in test_table_data
                .payload_batch
                .iter()
                .zip(log_entries.iter())
                .rev()
                .enumerate()
            {
                // sequence
                assert_eq!(test_table_data.max_seq - idx as u64, log_entry.sequence);

                // payload
                assert_eq!(expect_log_write_entry, &log_entry.payload);
            }
        }
    }
}

/// The payload for Wal log entry for testing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestPayload {
    pub val: u32,
}

impl Payload for TestPayload {
    type Error = Error;

    fn encode_size(&self) -> usize {
        4
    }

    fn encode_to<B: BufMut>(&self, buf: &mut B) -> Result<(), Self::Error> {
        buf.try_put_u32(self.val).expect("must write");
        Ok(())
    }
}

impl From<&u32> for TestPayload {
    fn from(v: &u32) -> Self {
        Self { val: *v }
    }
}

pub struct TestPayloadDecoder;

impl PayloadDecoder for TestPayloadDecoder {
    type Error = Error;
    type Target = TestPayload;

    fn decode<B: SafeBuf>(&self, buf: &mut B) -> Result<Self::Target, Self::Error> {
        let val = buf.try_get_u32().expect("should succeed to read u32");
        Ok(TestPayload { val })
    }
}

pub struct TestTableData {
    table_id: TableId,
    payload_batch: Vec<TestPayload>,
    max_seq: SequenceNumber,
}

impl TestTableData {
    pub fn new(
        table_id: TableId,
        payload_batch: Vec<TestPayload>,
        max_seq: SequenceNumber,
    ) -> Self {
        Self {
            table_id,
            payload_batch,
            max_seq,
        }
    }
}
