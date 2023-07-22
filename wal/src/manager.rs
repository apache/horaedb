// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! WalManager abstraction

use std::{collections::VecDeque, fmt, sync::Arc, time::Duration};

use async_trait::async_trait;
pub use common_types::SequenceNumber;
use common_types::{
    table::{TableId, DEFAULT_SHARD_ID},
    MAX_SEQUENCE_NUMBER, MIN_SEQUENCE_NUMBER,
};
pub use error::*;
use generic_error::BoxError;
use runtime::Runtime;
use snafu::ResultExt;

use crate::log_batch::{LogEntry, LogWriteBatch, PayloadDecoder};

pub mod error {
    use generic_error::GenericError;
    use macros::define_result;
    use snafu::{Backtrace, Snafu};

    use crate::manager::{RegionId, WalLocation};

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
            source: GenericError,
            backtrace: Backtrace,
        },

        #[snafu(display("Failed to initialize wal, err:{}.\nBacktrace:\n{}", source, backtrace))]
        Initialization {
            source: GenericError,
            backtrace: Backtrace,
        },

        #[snafu(display(
            "Region is not found, wal location:{:?}.\nBacktrace:\n{}",
            location,
            backtrace
        ))]
        RegionNotFound {
            location: WalLocation,
            backtrace: Backtrace,
        },

        #[snafu(display(
            "Failed to create wal encoder, err:{}.\nBacktrace:\n{}",
            source,
            backtrace
        ))]
        CreateWalEncoder {
            source: GenericError,
            backtrace: Backtrace,
        },

        #[snafu(display(
            "Failed to write log entries, err:{}.\nBacktrace:\n{}",
            source,
            backtrace
        ))]
        Write {
            source: GenericError,
            backtrace: Backtrace,
        },

        #[snafu(display(
            "Failed to read log entries, err:{}.\nBacktrace:\n{}",
            source,
            backtrace
        ))]
        Read {
            source: GenericError,
            backtrace: Backtrace,
        },

        #[snafu(display(
            "Failed to delete log entries, err:{}.\nBacktrace:\n{}",
            source,
            backtrace
        ))]
        Delete {
            source: GenericError,
            backtrace: Backtrace,
        },

        #[snafu(display("Failed to encode, err:{}.\nBacktrace:\n{}", source, backtrace))]
        Encoding {
            source: GenericError,
            backtrace: Backtrace,
        },

        #[snafu(display("Failed to decode, err:{}.\nBacktrace:\n{}", source, backtrace))]
        Decoding {
            source: GenericError,
            backtrace: Backtrace,
        },

        #[snafu(display(
            "Failed to close wal region, region_id:{}, err:{}.\nBacktrace:\n{}",
            source,
            region,
            backtrace
        ))]
        CloseRegion {
            source: GenericError,
            region: RegionId,
            backtrace: Backtrace,
        },

        #[snafu(display("Failed to close wal, err:{}.\nBacktrace:\n{}", source, backtrace))]
        Close {
            source: GenericError,
            backtrace: Backtrace,
        },

        #[snafu(display("Failed to execute in runtime, err:{}", source))]
        RuntimeExec { source: runtime::Error },

        #[snafu(display("Encountered unknown error, msg:{}.\nBacktrace:\n{}", msg, backtrace))]
        Unknown { msg: String, backtrace: Backtrace },
    }

    define_result!(Error);
}

pub type RegionId = u64;

/// Decide where to write logs
#[derive(Debug, Default, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct WalLocation {
    pub region_id: RegionId,
    pub table_id: TableId,
}

impl WalLocation {
    pub fn new(region_id: RegionId, table_id: TableId) -> Self {
        WalLocation {
            region_id,
            table_id,
        }
    }
}

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

#[derive(Debug, Clone)]
pub struct ReadContext {
    /// Timeout to read log entries and it only takes effect when reading from a
    /// Wal on a remote machine (reading from the local disk does not have
    /// timeout).
    pub timeout: Duration,
    /// Batch size to read log entries.
    pub batch_size: usize,
}

impl Default for ReadContext {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(5),
            batch_size: 500,
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
    /// Location of the wal to read
    pub location: WalLocation,
    // TODO(yingwen): Or just rename to ReadBound?
    /// Start bound
    pub start: ReadBoundary,
    /// End bound
    pub end: ReadBoundary,
}

impl Default for ReadRequest {
    fn default() -> Self {
        Self {
            location: WalLocation::new(DEFAULT_SHARD_ID as u64, TableId::MIN),
            start: ReadBoundary::Min,
            end: ReadBoundary::Min,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ScanRequest {
    /// Id of the region to scan
    pub region_id: RegionId,
}

pub type ScanContext = ReadContext;

/// Sync Iterator abstraction for log entry.
pub trait SyncLogIterator: Send + fmt::Debug {
    /// Fetch next log entry from the iterator.
    ///
    /// NOTE that this operation may **SYNC** caller thread now.
    fn next_log_entry(&mut self) -> Result<Option<LogEntry<&'_ [u8]>>>;
}

/// Vectorwise log entry iterator.
#[async_trait]
pub trait AsyncLogIterator: Send + fmt::Debug {
    /// Async fetch next log entry from the iterator.
    async fn next_log_entry(&mut self) -> Result<Option<LogEntry<&'_ [u8]>>>;
}

/// Management of multi-region Wals.
///
/// Every region has its own increasing (and maybe hallow) sequence number
/// space.
#[async_trait]
pub trait WalManager: Send + Sync + fmt::Debug + 'static {
    /// Get current sequence number.
    async fn sequence_num(&self, location: WalLocation) -> Result<SequenceNumber>;

    /// Mark the entries whose sequence number is in [0, `sequence_number`] to
    /// be deleted in the future.
    async fn mark_delete_entries_up_to(
        &self,
        location: WalLocation,
        sequence_num: SequenceNumber,
    ) -> Result<()>;

    /// Close a region.
    async fn close_region(&self, region: RegionId) -> Result<()>;

    /// Close the wal gracefully.
    async fn close_gracefully(&self) -> Result<()>;

    /// Provide iterator on necessary entries according to `ReadRequest`.
    async fn read_batch(
        &self,
        ctx: &ReadContext,
        req: &ReadRequest,
    ) -> Result<BatchLogIteratorAdapter>;

    /// Write a batch of log entries to log.
    ///
    /// Returns the max sequence number for the batch of log entries.
    async fn write(&self, ctx: &WriteContext, batch: &LogWriteBatch) -> Result<SequenceNumber>;

    /// Scan all logs from a `Region`.
    async fn scan(&self, ctx: &ScanContext, req: &ScanRequest) -> Result<BatchLogIteratorAdapter>;

    /// Get statistics
    async fn get_statistics(&self) -> Option<String> {
        None
    }
}

#[derive(Debug)]
enum LogIterator {
    Sync {
        iter: Box<dyn SyncLogIterator>,
        runtime: Arc<Runtime>,
    },
    Async(Box<dyn AsyncLogIterator>),
}

/// Adapter to convert a sync/async iterator to a batch async iterator.
#[derive(Debug)]
pub struct BatchLogIteratorAdapter {
    /// When nothing can be returned while calling `next_log_entry` of
    /// both sync and async `iter`, it will be set to None to represent
    /// the termination of polling [BatchLogIteratorAdapter].
    iter: Option<LogIterator>,
    batch_size: usize,
}

impl BatchLogIteratorAdapter {
    pub fn new_with_sync(
        iter: Box<dyn SyncLogIterator>,
        runtime: Arc<Runtime>,
        batch_size: usize,
    ) -> Self {
        Self {
            iter: Some(LogIterator::Sync { iter, runtime }),
            batch_size,
        }
    }

    pub fn new_with_async(iter: Box<dyn AsyncLogIterator>, batch_size: usize) -> Self {
        Self {
            iter: Some(LogIterator::Async(iter)),
            batch_size,
        }
    }

    async fn simulated_async_next<D: PayloadDecoder + Send + 'static>(
        &mut self,
        decoder: D,
        runtime: Arc<Runtime>,
        sync_iter: Box<dyn SyncLogIterator>,
        mut buffer: VecDeque<LogEntry<D::Target>>,
    ) -> Result<(VecDeque<LogEntry<D::Target>>, Option<LogIterator>)> {
        buffer.clear();

        let mut iter = sync_iter;
        let batch_size = self.batch_size;
        let (log_entries, iter_opt) = runtime
            .spawn_blocking(move || {
                for _ in 0..batch_size {
                    if let Some(raw_log_entry) = iter.next_log_entry()? {
                        let mut raw_payload = raw_log_entry.payload;
                        let payload = decoder
                            .decode(&mut raw_payload)
                            .box_err()
                            .context(error::Decoding)?;
                        let log_entry = LogEntry {
                            table_id: raw_log_entry.table_id,
                            sequence: raw_log_entry.sequence,
                            payload,
                        };
                        buffer.push_back(log_entry);
                    } else {
                        return Ok((buffer, None));
                    }
                }

                Ok((buffer, Some(iter)))
            })
            .await
            .context(RuntimeExec)??;

        match iter_opt {
            Some(iter) => Ok((log_entries, Some(LogIterator::Sync { iter, runtime }))),
            None => Ok((log_entries, None)),
        }
    }

    async fn async_next<D: PayloadDecoder + Send + 'static>(
        &mut self,
        decoder: D,
        async_iter: Box<dyn AsyncLogIterator>,
        mut buffer: VecDeque<LogEntry<D::Target>>,
    ) -> Result<(VecDeque<LogEntry<D::Target>>, Option<LogIterator>)> {
        buffer.clear();

        let mut async_iter = async_iter;
        for _ in 0..self.batch_size {
            if let Some(raw_log_entry) = async_iter.next_log_entry().await? {
                let mut raw_payload = raw_log_entry.payload;
                let payload = decoder
                    .decode(&mut raw_payload)
                    .box_err()
                    .context(error::Decoding)?;
                let log_entry = LogEntry {
                    table_id: raw_log_entry.table_id,
                    sequence: raw_log_entry.sequence,
                    payload,
                };
                buffer.push_back(log_entry);
            } else {
                return Ok((buffer, None));
            }
        }

        Ok((buffer, Some(LogIterator::Async(async_iter))))
    }

    pub async fn next_log_entries<D: PayloadDecoder + Send + 'static>(
        &mut self,
        decoder: D,
        buffer: VecDeque<LogEntry<D::Target>>,
    ) -> Result<VecDeque<LogEntry<D::Target>>> {
        if self.iter.is_none() {
            return Ok(VecDeque::new());
        }

        let iter = self.iter.take().unwrap();
        let (log_entries, iter) = match iter {
            LogIterator::Sync { iter, runtime } => {
                self.simulated_async_next(decoder, runtime, iter, buffer)
                    .await?
            }
            LogIterator::Async(iter) => self.async_next(decoder, iter, buffer).await?,
        };

        self.iter = iter;
        Ok(log_entries)
    }
}

pub type WalManagerRef = Arc<dyn WalManager>;

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, sync::Arc};

    use async_trait::async_trait;
    use runtime::{self, Runtime};

    use super::*;
    use crate::{log_batch::LogEntry, tests::util::TestPayloadDecoder};

    #[derive(Debug, Clone)]
    struct TestIterator {
        test_logs: Vec<Vec<u8>>,
        cursor: usize,
        terminate: usize,
    }

    impl SyncLogIterator for TestIterator {
        fn next_log_entry(
            &mut self,
        ) -> super::Result<Option<crate::log_batch::LogEntry<&'_ [u8]>>> {
            if self.cursor == self.terminate {
                return Ok(None);
            }

            let log_entry = LogEntry {
                table_id: 0,
                sequence: 0,
                payload: self.test_logs[self.cursor].as_slice(),
            };
            self.cursor += 1;

            Ok(Some(log_entry))
        }
    }

    #[async_trait]
    impl AsyncLogIterator for TestIterator {
        async fn next_log_entry(
            &mut self,
        ) -> super::Result<Option<crate::log_batch::LogEntry<&'_ [u8]>>> {
            if self.cursor == self.terminate {
                return Ok(None);
            }

            let log_entry = LogEntry {
                table_id: 0,
                sequence: 0,
                payload: self.test_logs[self.cursor].as_slice(),
            };
            self.cursor += 1;

            Ok(Some(log_entry))
        }
    }

    #[test]
    fn test_iterator_adapting() {
        let test_data = vec![1_u32, 2, 3, 4, 5, 6];
        let test_iterator = TestIterator {
            test_logs: test_data.iter().map(|u| u.to_be_bytes().to_vec()).collect(),
            cursor: 0,
            terminate: test_data.len(),
        };
        let runtime = Arc::new(
            runtime::Builder::default()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap(),
        );

        runtime.block_on(async {
            let res1 = test_async_iterator_adapting(test_iterator.clone()).await;
            assert_eq!(test_data, res1);
        });

        runtime.block_on(async {
            let res2 = test_sync_iterator_adapting(test_iterator.clone(), runtime.clone()).await;
            assert_eq!(test_data, res2);
        });
    }

    async fn test_async_iterator_adapting(test_iterator: TestIterator) -> Vec<u32> {
        let mut res = Vec::new();
        let mut iter = BatchLogIteratorAdapter::new_with_async(Box::new(test_iterator), 3);
        let mut buffer = VecDeque::with_capacity(3);

        loop {
            buffer = iter
                .next_log_entries(TestPayloadDecoder, buffer)
                .await
                .unwrap();
            for entry in buffer.iter() {
                res.push(entry.payload.val);
            }

            if buffer.is_empty() {
                break;
            }
        }

        res
    }

    async fn test_sync_iterator_adapting(
        test_iterator: TestIterator,
        runtime: Arc<Runtime>,
    ) -> Vec<u32> {
        let mut res = Vec::new();
        let mut iter = BatchLogIteratorAdapter::new_with_sync(Box::new(test_iterator), runtime, 3);
        let mut buffer = VecDeque::with_capacity(3);
        loop {
            buffer = iter
                .next_log_entries(TestPayloadDecoder, buffer)
                .await
                .unwrap();
            for entry in buffer.iter() {
                res.push(entry.payload.val);
            }

            if buffer.is_empty() {
                break;
            }
        }

        res
    }
}
