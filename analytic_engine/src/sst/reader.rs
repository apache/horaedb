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

//! Sst reader trait definition.

use async_trait::async_trait;
use common_types::record_batch::RecordBatchWithKey;

use crate::{prefetchable_stream::PrefetchableStream, sst::meta_data::SstMetaData};

pub mod error {
    use generic_error::GenericError;
    use macros::define_result;
    use snafu::{Backtrace, Snafu};

    #[derive(Debug, Snafu)]
    #[snafu(visibility(pub))]
    pub enum Error {
        #[snafu(display("Try to read again, path:{path}.\nBacktrace:\n{backtrace}"))]
        ReadAgain { backtrace: Backtrace, path: String },

        #[snafu(display("Fail to read persisted file, path:{path}, err:{source}"))]
        ReadPersist { path: String, source: GenericError },

        #[snafu(display("Failed to decode record batch, err:{source}"))]
        DecodeRecordBatch { source: GenericError },

        #[snafu(display(
            "Failed to decode sst meta data, file_path:{file_path}, err:{source}.\nBacktrace:\n{backtrace:?}",
        ))]
        FetchAndDecodeSstMeta {
            file_path: String,
            source: parquet::errors::ParquetError,
            backtrace: Backtrace,
        },

        #[snafu(display(
            "Failed to decode page indexes for meta data, file_path:{file_path}, err:{source}.\nBacktrace:\n{backtrace:?}",
        ))]
        DecodePageIndexes {
            file_path: String,
            source: parquet::errors::ParquetError,
            backtrace: Backtrace,
        },

        #[snafu(display("Failed to decode sst meta data, err:{source}"))]
        DecodeSstMeta { source: GenericError },

        #[snafu(display("Sst meta data is not found.\nBacktrace:\n{backtrace}"))]
        SstMetaNotFound { backtrace: Backtrace },

        #[snafu(display("Fail to projection, err:{source}"))]
        Projection { source: GenericError },

        #[snafu(display("Sst meta data is empty.\nBacktrace:\n{backtrace}"))]
        EmptySstMeta { backtrace: Backtrace },

        #[snafu(display("Invalid schema, err:{source}"))]
        InvalidSchema { source: common_types::schema::Error },

        #[snafu(display("Meet a datafusion error, err:{source}\nBacktrace:\n{backtrace}"))]
        DataFusionError {
            source: datafusion::error::DataFusionError,
            backtrace: Backtrace,
        },

        #[snafu(display("Meet a object store error, err:{source}\nBacktrace:\n{backtrace}"))]
        ObjectStoreError {
            source: object_store::ObjectStoreError,
            backtrace: Backtrace,
        },

        #[snafu(display("Meet a parquet error, err:{source}\nBacktrace:\n{backtrace}"))]
        ParquetError {
            source: parquet::errors::ParquetError,
            backtrace: Backtrace,
        },

        #[snafu(display("Other kind of error:{source}"))]
        Other { source: GenericError },

        #[snafu(display("Other kind of error, msg:{msg}.\nBacktrace:\n{backtrace}"))]
        OtherNoCause { msg: String, backtrace: Backtrace },
    }

    define_result!(Error);
}

pub use error::*;

#[async_trait]
pub trait SstReader {
    async fn meta_data(&mut self) -> Result<SstMetaData>;

    async fn read(
        &mut self,
    ) -> Result<Box<dyn PrefetchableStream<Item = Result<RecordBatchWithKey>>>>;
}

#[cfg(test)]
pub mod tests {
    use common_types::row::Row;

    use super::*;
    use crate::prefetchable_stream::PrefetchableStream;

    pub async fn check_stream<S>(stream: &mut S, expected_rows: Vec<Row>)
    where
        S: PrefetchableStream<Item = Result<RecordBatchWithKey>> + Unpin,
    {
        let mut visited_rows = 0;
        while let Some(batch) = stream.fetch_next().await {
            let batch = batch.unwrap();
            for row_idx in 0..batch.num_rows() {
                assert_eq!(batch.clone_row_at(row_idx), expected_rows[visited_rows]);
                visited_rows += 1;
            }
        }

        assert_eq!(visited_rows, expected_rows.len());
    }
}
