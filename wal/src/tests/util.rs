// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! utilities for testing wal module.

use std::{collections::VecDeque, path::Path, str::FromStr, sync::Arc};

use async_trait::async_trait;
use common_types::{
    bytes::{MemBuf, MemBufMut},
    SequenceNumber,
};
use common_util::{
    config::ReadableDuration,
    runtime::{self, Runtime},
};
use snafu::Snafu;
use table_kv::memory::MemoryImpl;
use tempfile::TempDir;

use crate::{
    log_batch::{LogWriteBatch, Payload, PayloadDecoder},
    manager::{
        BatchLogIterator, BatchLogIteratorAdapter, ReadContext, WalLocation, WalManager,
        WalManagerRef, WriteContext,
    },
    rocks_impl::{self, manager::RocksImpl},
    table_kv_impl::{model::NamespaceConfig, wal::WalNamespaceImpl, WalRuntimes},
};

#[derive(Debug, Snafu)]
pub enum Error {}

#[async_trait]
pub trait WalBuilder: Send + Sync + 'static {
    type Wal: WalManager + Send + Sync;

    async fn build(&self, data_path: &Path, runtime: Arc<Runtime>) -> Arc<Self::Wal>;
}

#[derive(Default)]
pub struct RocksWalBuilder;

#[async_trait]
impl WalBuilder for RocksWalBuilder {
    type Wal = RocksImpl;

    async fn build(&self, data_path: &Path, runtime: Arc<Runtime>) -> Arc<Self::Wal> {
        let wal_builder =
            rocks_impl::manager::Builder::with_default_rocksdb_config(data_path, runtime);

        Arc::new(
            wal_builder
                .build()
                .expect("should succeed to build rocksimpl wal"),
        )
    }
}

pub type RocksTestEnv = TestEnv<RocksWalBuilder>;

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
            region_meta_shard_num: 2,
            ttl: self.ttl,
            ..Default::default()
        };

        let wal_runtimes = WalRuntimes {
            read_runtime: runtime.clone(),
            write_runtime: runtime.clone(),
            bg_runtime: runtime.clone(),
        };
        let namespace_wal =
            WalNamespaceImpl::open(self.table_kv.clone(), wal_runtimes, WAL_NAMESPACE, config)
                .await
                .unwrap();

        Arc::new(namespace_wal)
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
        wal: WalManagerRef,
        wal_location: WalLocation,
        start: u32,
        end: u32,
    ) -> (Vec<TestPayload>, LogWriteBatch) {
        let log_entries = (start..end).collect::<Vec<_>>();

        let log_batch_encoder = wal
            .encoder(wal_location)
            .expect("should succeed to create log batch encoder");

        let log_batch = log_batch_encoder
            .encode_batch::<TestPayload, u32>(&log_entries)
            .expect("should succeed to encode payloads");

        let payload_batch = self.build_payload_batch(start, end);
        (payload_batch, log_batch)
    }

    /// Check whether the log entries from the iterator equals the
    /// `write_batch`.
    pub async fn check_log_entries(
        &self,
        max_seq: SequenceNumber,
        payload_batch: &[TestPayload],
        mut iter: BatchLogIteratorAdapter,
    ) {
        let mut log_entries = VecDeque::with_capacity(payload_batch.len());
        let mut buffer = VecDeque::new();
        loop {
            let dec = TestPayloadDecoder;
            buffer = iter
                .next_log_entries(dec, buffer)
                .await
                .expect("should succeed to fetch next log entry");
            if buffer.is_empty() {
                break;
            }

            log_entries.append(&mut buffer);
        }

        assert_eq!(payload_batch.len(), log_entries.len());
        for (idx, (expect_log_write_entry, log_entry)) in payload_batch
            .iter()
            .zip(log_entries.iter())
            .rev()
            .enumerate()
        {
            // sequence
            assert_eq!(max_seq - idx as u64, log_entry.sequence);

            // payload
            assert_eq!(expect_log_write_entry, &log_entry.payload);
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

    fn encode_to<B: MemBufMut>(&self, buf: &mut B) -> Result<(), Self::Error> {
        buf.write_u32(self.val).expect("must write");
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

    fn decode<B: MemBuf>(&self, buf: &mut B) -> Result<Self::Target, Self::Error> {
        let val = buf.read_u32().expect("should succeed to read u32");
        Ok(TestPayload { val })
    }
}
