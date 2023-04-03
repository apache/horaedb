// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Sst writer trait definition

use std::cmp;

use async_trait::async_trait;
use bytes::Bytes;
use common_types::{
    record_batch::RecordBatchWithKey, request_id::RequestId, schema::Schema, time::TimeRange,
    SequenceNumber,
};
use common_util::error::GenericError;
use futures::Stream;

use crate::table_options::StorageFormat;

pub mod error {
    use common_util::{define_result, error::GenericError};
    use snafu::{Backtrace, Snafu};

    #[derive(Debug, Snafu)]
    #[snafu(visibility(pub))]
    pub enum Error {
        #[snafu(display(
            "Failed to perform storage operation, err:{}.\nBacktrace:\n{}",
            source,
            backtrace
        ))]
        Storage {
            source: object_store::ObjectStoreError,
            backtrace: Backtrace,
        },

        #[snafu(display("Failed to encode meta data, err:{}", source))]
        EncodeMetaData { source: GenericError },

        #[snafu(display(
            "Failed to encode record batch into sst, err:{}.\nBacktrace:\n{}",
            source,
            backtrace
        ))]
        EncodeRecordBatch {
            source: GenericError,
            backtrace: Backtrace,
        },

        #[snafu(display("Failed to build parquet filter, err:{}", source))]
        BuildParquetFilter { source: GenericError },

        #[snafu(display("Failed to poll record batch, err:{}", source))]
        PollRecordBatch { source: GenericError },

        #[snafu(display("Failed to read data, err:{}", source))]
        ReadData { source: GenericError },

        #[snafu(display("Other kind of error, msg:{}.\nBacktrace:\n{}", msg, backtrace))]
        OtherNoCause { msg: String, backtrace: Backtrace },
    }

    define_result!(Error);
}

pub use error::*;

pub type RecordBatchStreamItem = std::result::Result<RecordBatchWithKey, GenericError>;
// TODO(yingwen): SstReader also has a RecordBatchStream, can we use same type?
pub type RecordBatchStream = Box<dyn Stream<Item = RecordBatchStreamItem> + Send + Unpin>;

#[derive(Debug, Copy, Clone)]
pub struct SstInfo {
    pub file_size: usize,
    pub row_num: usize,
    pub storage_format: StorageFormat,
}

#[derive(Debug, Clone)]
pub struct MetaData {
    /// Min key of the sst.
    pub min_key: Bytes,
    /// Max key of the sst.
    pub max_key: Bytes,
    /// Time Range of the sst.
    pub time_range: TimeRange,
    /// Max sequence number in the sst.
    pub max_sequence: SequenceNumber,
    /// The schema of the sst.
    pub schema: Schema,
}

/// The writer for sst.
///
/// The caller provides a stream of [RecordBatch] and the writer takes
/// responsibilities for persisting the records.
#[async_trait]
pub trait SstWriter {
    async fn write(
        &mut self,
        request_id: RequestId,
        meta: &MetaData,
        record_stream: RecordBatchStream,
    ) -> Result<SstInfo>;
}

impl MetaData {
    /// Merge multiple meta datas into the one.
    ///
    /// Panic if the metas is empty.
    pub fn merge<I>(mut metas: I, schema: Schema) -> Self
    where
        I: Iterator<Item = MetaData>,
    {
        let first_meta = metas.next().unwrap();
        let mut min_key = first_meta.min_key;
        let mut max_key = first_meta.max_key;
        let mut time_range_start = first_meta.time_range.inclusive_start();
        let mut time_range_end = first_meta.time_range.exclusive_end();
        let mut max_sequence = first_meta.max_sequence;

        for file in metas {
            min_key = cmp::min(file.min_key, min_key);
            max_key = cmp::max(file.max_key, max_key);
            time_range_start = cmp::min(file.time_range.inclusive_start(), time_range_start);
            time_range_end = cmp::max(file.time_range.exclusive_end(), time_range_end);
            max_sequence = cmp::max(file.max_sequence, max_sequence);
        }

        MetaData {
            min_key,
            max_key,
            time_range: TimeRange::new(time_range_start, time_range_end).unwrap(),
            max_sequence,
            schema,
        }
    }
}
