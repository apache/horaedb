// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! WalManager abstraction

use std::{fmt, time::Duration};

use async_trait::async_trait;
pub use common_types::SequenceNumber;

use crate::log_batch::{LogEntry, LogWriteBatch, Payload, PayloadDecoder};

pub mod error {
    use common_util::define_result;
    use snafu::{Backtrace, Snafu};

    use crate::manager::RegionId;

    // Now most error from manage implementation don't have backtrace, so we add
    // backtrace here.
    #[derive(Debug, Snafu)]
    #[snafu(visibility(pub))]
    pub enum Error {
        #[snafu(display(
            "Failed to open wal, path:{}, err:{}.\nBacktrace:\n{}",
            wal_path,
            source,
            backtrace
        ))]
        Open {
            wal_path: String,
            source: Box<dyn std::error::Error + Send + Sync>,
            backtrace: Backtrace,
        },

        #[snafu(display("Failed to initialize wal, err:{}.\nBacktrace:\n{}", source, backtrace))]
        Initialization {
            source: Box<dyn std::error::Error + Send + Sync>,
            backtrace: Backtrace,
        },

        #[snafu(display(
            "Region is not found, region_id:{}.\nBacktrace:\n{}",
            region_id,
            backtrace
        ))]
        RegionNotFound {
            region_id: RegionId,
            backtrace: Backtrace,
        },

        #[snafu(display(
            "Failed to write log entries, err:{}.\nBacktrace:\n{}",
            source,
            backtrace
        ))]
        Write {
            source: Box<dyn std::error::Error + Send + Sync>,
            backtrace: Backtrace,
        },

        #[snafu(display(
            "Failed to read log entries, err:{}.\nBacktrace:\n{}",
            source,
            backtrace
        ))]
        Read {
            source: Box<dyn std::error::Error + Send + Sync>,
            backtrace: Backtrace,
        },

        #[snafu(display(
            "Failed to delete log entries, err:{}.\nBacktrace:\n{}",
            source,
            backtrace
        ))]
        Delete {
            source: Box<dyn std::error::Error + Send + Sync>,
            backtrace: Backtrace,
        },

        #[snafu(display("Failed to encode, err:{}.\nBacktrace:\n{}", source, backtrace))]
        Encoding {
            source: Box<dyn std::error::Error + Send + Sync>,
            backtrace: Backtrace,
        },

        #[snafu(display("Failed to decode, err:{}.\nBacktrace:\n{}", source, backtrace))]
        Decoding {
            source: Box<dyn std::error::Error + Send + Sync>,
            backtrace: Backtrace,
        },
    }

    define_result!(Error);
}

use common_types::{MAX_SEQUENCE_NUMBER, MIN_SEQUENCE_NUMBER};
pub use error::*;

pub type RegionId = u64;
pub const MAX_REGION_ID: RegionId = u64::MAX;

#[derive(Debug, Clone)]
pub struct WriteContext {
    /// Timeout to write wal and it only takes effect when writing to a Wal on a
    /// remote machine (writing to the local disk does not have timeout).
    pub timeout: Duration,
}

impl Default for WriteContext {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(1),
        }
    }
}

/// Write abstraction for log entries in Wal.
#[async_trait]
pub trait LogWriter {
    /// Write a batch of log entries to log.
    ///
    /// Returns the max sequence number for the batch of log entries.
    async fn write<P: Payload>(
        &self,
        ctx: &WriteContext,
        batch: &LogWriteBatch<P>,
    ) -> Result<SequenceNumber>;
}

#[derive(Debug, Clone)]
pub struct ReadContext {
    /// Timeout to read log entries and it only takes effect when reading from a
    /// Wal on a remote machine (reading from the local disk does not have
    /// timeout).
    pub timeout: Duration,
}

impl Default for ReadContext {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ReadBoundary {
    Max,
    Min,
    Included(SequenceNumber),
    Excluded(SequenceNumber),
}

impl ReadBoundary {
    /// Convert the boundary to start sequence number.
    ///
    /// Returns `None` if the boundary is `Excluded(MAX_SEQUENCE_NUM)`
    pub fn as_start_sequence_number(&self) -> Option<SequenceNumber> {
        match *self {
            ReadBoundary::Max => Some(MAX_SEQUENCE_NUMBER),
            ReadBoundary::Min => Some(MIN_SEQUENCE_NUMBER),
            ReadBoundary::Included(n) => Some(n),
            ReadBoundary::Excluded(n) => {
                if n == MAX_SEQUENCE_NUMBER {
                    None
                } else {
                    Some(n + 1)
                }
            }
        }
    }

    /// Convert the boundary to start sequence number.
    ///
    /// Returns `None` if the boundary is `Excluded(MIN_SEQUENCE_NUM)`
    pub fn as_end_sequence_number(&self) -> Option<SequenceNumber> {
        match *self {
            ReadBoundary::Max => Some(MAX_SEQUENCE_NUMBER),
            ReadBoundary::Min => Some(MIN_SEQUENCE_NUMBER),
            ReadBoundary::Included(n) => Some(n),
            ReadBoundary::Excluded(n) => {
                if n == MIN_SEQUENCE_NUMBER {
                    None
                } else {
                    Some(n - 1)
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReadRequest {
    /// Region id of the wal to read
    pub region_id: RegionId,
    // TODO(yingwen): Or just rename to ReadBound?
    /// Start bound
    pub start: ReadBoundary,
    /// End bound
    pub end: ReadBoundary,
}

/// Iterator abstraction for log entry.
pub trait LogIterator {
    fn next_log_entry<D: PayloadDecoder>(
        &mut self,
        decoder: &D,
    ) -> Result<Option<LogEntry<D::Target>>>;
}

/// Read abstraction for log entries in the Wal.
pub trait LogReader {
    /// Iterator over log entries.
    type Iterator: LogIterator + Send;
    /// Provide iterator on necessary entries according to `ReadRequest`.
    fn read(&self, ctx: &ReadContext, req: &ReadRequest) -> Result<Self::Iterator>;
}

// TODO(xikai): define Error as associate type.
/// Management of multi-region Wals.
///
/// Every region has its own increasing (and maybe hallow) sequence number
/// space.
#[async_trait]
pub trait WalManager: LogWriter + LogReader + fmt::Debug {
    /// Get current sequence number.
    fn sequence_num(&self, region_id: RegionId) -> Result<SequenceNumber>;

    /// Mark the entries whose sequence number is in [0, `sequence_number`] to
    /// be deleted in the future.
    async fn mark_delete_entries_up_to(
        &self,
        region_id: RegionId,
        sequence_num: SequenceNumber,
    ) -> Result<()>;
}
