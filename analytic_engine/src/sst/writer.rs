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

//! Sst writer trait definition

use std::cmp;

use async_trait::async_trait;
use bytes_ext::Bytes;
use common_types::{
    record_batch::RecordBatchWithKey, request_id::RequestId, schema::Schema, time::TimeRange,
    SequenceNumber,
};
use futures::Stream;
use generic_error::GenericError;

use crate::table_options::StorageFormat;

pub mod error {
    use common_types::datum::DatumKind;
    use generic_error::GenericError;
    use macros::define_result;
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

        #[snafu(display("Failed to encode pb data, err:{}", source))]
        EncodePbData {
            source: crate::sst::parquet::encoding::Error,
        },

        #[snafu(display("IO failed, file:{file}, source:{source}.\nbacktrace:\n{backtrace}",))]
        Io {
            file: String,
            source: std::io::Error,
            backtrace: Backtrace,
        },

        #[snafu(display(
            "Failed to encode record batch into sst, err:{}.\nBacktrace:\n{}",
            source,
            backtrace
        ))]
        EncodeRecordBatch {
            source: GenericError,
            backtrace: Backtrace,
        },

        #[snafu(display(
            "Require column to be timestamp, actual:{datum_kind}.\nBacktrace:\n{backtrace}"
        ))]
        RequireTimestampColumn {
            datum_kind: DatumKind,
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

#[derive(Debug, Clone)]
pub struct SstInfo {
    pub file_size: usize,
    pub row_num: usize,
    pub storage_format: StorageFormat,
    pub meta_path: String,
    /// Real time range, not aligned to segment.
    pub time_range: TimeRange,
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
