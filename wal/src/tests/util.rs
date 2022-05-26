// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! utilities for testing wal module.

use std::{path::Path, sync::Arc};

use common_types::bytes::{MemBuf, MemBufMut};
use common_util::runtime::{self, Runtime};
use tempfile::TempDir;

use crate::{
    log_batch::{LogWriteBatch, LogWriteEntry, Payload, PayloadDecoder},
    manager::{LogIterator, LogReader, ReadContext, RegionId, WalManager, WriteContext},
    rocks_impl::{self, manager::RocksImpl},
};

pub trait WalBuilder: Default + Send + Sync {
    type Wal: WalManager + Send + Sync;
    fn build(&self, data_path: &Path, runtime: Arc<Runtime>) -> Arc<Self::Wal>;
}
use common_types::SequenceNumber;
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {}

#[derive(Default)]
pub struct RocksWalBuilder;

impl WalBuilder for RocksWalBuilder {
    type Wal = RocksImpl;

    fn build(&self, data_path: &Path, runtime: Arc<Runtime>) -> Arc<Self::Wal> {
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
    pub fn new(num_workers: usize) -> Self {
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
            builder: B::default(),
        }
    }

    pub fn build_wal(&self) -> Arc<B::Wal> {
        self.builder.build(self.dir.path(), self.runtime.clone())
    }

    /// Build the log batch with [TestPayload].val range [start, end).
    pub fn build_log_batch(
        &self,
        region_id: RegionId,
        start: u32,
        end: u32,
    ) -> LogWriteBatch<TestPayload> {
        let mut write_batch = LogWriteBatch::new(region_id);
        for val in start..end {
            let payload = TestPayload { val };
            write_batch.entries.push(LogWriteEntry { payload });
        }

        write_batch
    }

    /// Check whether the log entries from the iterator equals the
    /// `write_batch`.
    pub fn check_log_entries(
        &self,
        max_seq: SequenceNumber,
        write_batch: &LogWriteBatch<TestPayload>,
        mut iter: <B::Wal as LogReader>::Iterator,
    ) {
        let dec = TestPayloadDecoder;
        let mut log_entries = Vec::with_capacity(write_batch.entries.len());
        loop {
            let log_entry = iter
                .next_log_entry(&dec)
                .expect("should succeed to fetch next log entry");
            if log_entry.is_none() {
                break;
            }

            log_entries.push(log_entry.unwrap());
        }

        assert_eq!(write_batch.entries.len(), log_entries.len());
        for (idx, (expect_log_write_entry, log_entry)) in write_batch
            .entries
            .iter()
            .zip(log_entries.iter())
            .rev()
            .enumerate()
        {
            assert_eq!(max_seq - idx as u64, log_entry.sequence);
            assert_eq!(expect_log_write_entry.payload, log_entry.payload);
        }
    }
}

/// The payload for Wal log entry for testing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestPayload {
    val: u32,
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

pub struct TestPayloadDecoder;

impl PayloadDecoder for TestPayloadDecoder {
    type Error = Error;
    type Target = TestPayload;

    fn decode<B: MemBuf>(&self, buf: &mut B) -> Result<Self::Target, Self::Error> {
        let val = buf.read_u32().expect("should succeed to read u32");
        Ok(TestPayload { val })
    }
}
