// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! MemTable

pub mod columnar;
pub mod factory;
mod iter;
pub mod key;
pub mod skiplist;

use std::{ops::Bound, sync::Arc, time::Instant};

use common_types::{
    bytes::{ByteVec, Bytes},
    projected_schema::ProjectedSchema,
    record_batch::RecordBatchWithKey,
    row::RowGroupSplitter,
    schema::{IndexInWriterSchema, Schema},
    SequenceNumber,
};
use common_util::{define_result, error::GenericError};
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, Snafu};
use trace_metric::MetricsCollector;

use crate::memtable::key::KeySequence;

const DEFAULT_SCAN_BATCH_SIZE: usize = 500;
const MEMTABLE_TYPE_SKIPLIST: &str = "skiplist";
const MEMTABLE_TYPE_COLUMNAR: &str = "columnar";

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
pub enum MemtableType {
    SkipList,
    Columnar,
}

impl MemtableType {
    pub fn parse_from(s: &str) -> Self {
        if s.eq_ignore_ascii_case(MEMTABLE_TYPE_SKIPLIST) {
            MemtableType::Columnar
        } else {
            MemtableType::SkipList
        }
    }
}

impl ToString for MemtableType {
    fn to_string(&self) -> String {
        match self {
            MemtableType::SkipList => MEMTABLE_TYPE_SKIPLIST.to_string(),
            MemtableType::Columnar => MEMTABLE_TYPE_COLUMNAR.to_string(),
        }
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Failed to encode internal key, err:{}", source))]
    EncodeInternalKey { source: crate::memtable::key::Error },

    #[snafu(display("Failed to decode internal key, err:{}", source))]
    DecodeInternalKey { source: crate::memtable::key::Error },

    #[snafu(display("Failed to decode row, err:{}", source))]
    DecodeRow {
        source: common_util::codec::row::Error,
    },

    #[snafu(display("Failed to append row to batch builder, err:{}", source))]
    AppendRow {
        source: common_types::record_batch::Error,
    },

    #[snafu(display("Failed to build record batch, err:{}", source,))]
    BuildRecordBatch {
        source: common_types::record_batch::Error,
    },

    #[snafu(display("Failed to project memtable schema, err:{}", source))]
    ProjectSchema {
        source: common_types::projected_schema::Error,
    },

    #[snafu(display(
        "Invalid sequence number to put, given:{}, last:{}.\nBacktrace:\n{}",
        given,
        last,
        backtrace
    ))]
    InvalidPutSequence {
        given: SequenceNumber,
        last: SequenceNumber,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid row, err:{}", source))]
    InvalidRow { source: GenericError },

    #[snafu(display("Fail to iter in reverse order, err:{}", source))]
    IterReverse { source: GenericError },

    #[snafu(display("Timeout when iter memtable.\nBacktrace:\n{}", backtrace))]
    IterTimeout { backtrace: Backtrace },

    #[snafu(display("msg:{msg}, err:{source}"))]
    Internal { msg: String, source: GenericError },

    #[snafu(display("msg:{msg}"))]
    InternalNoCause { msg: String },
}

define_result!(Error);

/// Options for put and context for tracing
pub struct PutContext {
    /// Buffer for encoding key, can reuse during put
    pub key_buf: ByteVec,
    /// Buffer for encoding value, can reuse during put
    pub value_buf: ByteVec,
    /// Used to encode row.
    pub index_in_writer: IndexInWriterSchema,
}

impl PutContext {
    pub fn new(index_in_writer: IndexInWriterSchema) -> Self {
        Self {
            key_buf: ByteVec::new(),
            value_buf: ByteVec::new(),
            index_in_writer,
        }
    }
}

/// Options for scan and context for tracing
#[derive(Debug, Clone)]
pub struct ScanContext {
    /// Suggested row number per batch
    pub batch_size: usize,
    pub deadline: Option<Instant>,
}

impl Default for ScanContext {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_SCAN_BATCH_SIZE,
            deadline: None,
        }
    }
}

/// Scan request
///
/// Now we only support forward scan.
#[derive(Debug, Clone)]
pub struct ScanRequest {
    /// The start key of the encoded user key (without sequence).
    pub start_user_key: Bound<Bytes>,
    /// The end key of the encoded user key (without sequence).
    pub end_user_key: Bound<Bytes>,
    /// Max visible sequence (inclusive), row key with sequence <= this can be
    /// visible.
    pub sequence: SequenceNumber,
    /// Schema and projection to read.
    pub projected_schema: ProjectedSchema,
    pub need_dedup: bool,
    pub reverse: bool,
    /// Collector for scan metrics.
    pub metrics_collector: Option<MetricsCollector>,
}

/// In memory storage for table's data.
///
/// # Concurrency
/// The memtable is designed for single-writer and multiple-reader usage, so
/// not all function supports concurrent writer, the caller should guarantee not
/// writing to the memtable concurrently.
// All operation is done in memory, no need to use async trait
pub trait MemTable {
    /// Schema of this memtable
    ///
    /// The schema of a memtable is not allowed to change now. Modifying the
    /// schema of a table requires a memtable switch and external
    /// synchronization
    fn schema(&self) -> &Schema;

    /// Peek the min key of this memtable.
    fn min_key(&self) -> Option<Bytes>;

    /// Peek the max key of this memtable.
    fn max_key(&self) -> Option<Bytes>;

    /// Insert one row into the memtable.
    ///
    ///.- ctx: The put context
    /// - sequence: The sequence of the row
    /// - row: The row to insert
    /// - schema: The schema of the row
    ///
    /// REQUIRE:
    /// - The schema of RowGroup must equal to the schema of memtable. How to
    /// handle duplicate entries is implementation specific.
    fn put(
        &self,
        ctx: &mut PutContext,
        sequence: KeySequence,
        row_group: &RowGroupSplitter,
        schema: &Schema,
    ) -> Result<()>;

    /// Scan the memtable.
    ///
    /// Returns the data in columnar format. The returned rows is guaranteed
    /// to be ordered by the primary key.
    fn scan(&self, ctx: ScanContext, request: ScanRequest) -> Result<ColumnarIterPtr>;

    /// Returns an estimate of the number of bytes of data in used
    fn approximate_memory_usage(&self) -> usize;

    /// Set last sequence of the memtable, returns error if the given `sequence`
    /// is less than existing last sequence.
    ///
    /// REQUIRE:
    /// - External synchronization is required.
    fn set_last_sequence(&self, sequence: SequenceNumber) -> Result<()>;

    /// Returns the last sequence of the memtable.
    ///
    /// If the memtable is empty, then the last sequence is 0.
    fn last_sequence(&self) -> SequenceNumber;
}

/// A reference to memtable
pub type MemTableRef = Arc<dyn MemTable + Send + Sync>;

/// A pointer to columnar iterator
pub type ColumnarIterPtr = Box<dyn Iterator<Item = Result<RecordBatchWithKey>> + Send + Sync>;
