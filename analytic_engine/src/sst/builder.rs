// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Sst builder trait definition

use async_trait::async_trait;
use common_types::{record_batch::RecordBatchWithKey, request_id::RequestId};
use futures::Stream;

use crate::{sst::file::SstMetaData, table_options::StorageFormat};

pub mod error {
    use common_util::define_result;
    use snafu::{Backtrace, Snafu};

    #[derive(Debug, Snafu)]
    #[snafu(visibility(pub))]
    pub enum Error {
        #[snafu(display("Failed to perform storage operation, err:{}", source))]
        Storage {
            source: object_store::ObjectStoreError,
        },

        #[snafu(display("Failed to encode meta data, err:{}", source))]
        EncodeMetaData {
            source: Box<dyn std::error::Error + Send + Sync>,
        },

        #[snafu(display(
            "Failed to encode record batch into sst, err:{}.\nBacktrace:\n{}",
            source,
            backtrace
        ))]
        EncodeRecordBatch {
            source: Box<dyn std::error::Error + Send + Sync>,
            backtrace: Backtrace,
        },

        #[snafu(display("Failed to poll record batch, err:{}", source))]
        PollRecordBatch {
            source: Box<dyn std::error::Error + Send + Sync>,
        },

        #[snafu(display("Failed to read data, err:{}", source))]
        ReadData {
            source: Box<dyn std::error::Error + Send + Sync>,
        },
    }

    define_result!(Error);
}

pub use error::*;

pub type RecordBatchStreamItem =
    std::result::Result<RecordBatchWithKey, Box<dyn std::error::Error + Send + Sync>>;
// TODO(yingwen): SstReader also has a RecordBatchStream, can we use same type?
pub type RecordBatchStream = Box<dyn Stream<Item = RecordBatchStreamItem> + Send + Unpin>;

#[derive(Debug, Copy, Clone)]
pub struct SstInfo {
    pub file_size: usize,
    pub row_num: usize,
    pub storage_format: StorageFormat,
}

/// The builder for sst.
///
/// The caller provides a stream of [RecordBatch] and the builder takes
/// responsibilities for persisting the records.
#[async_trait]
pub trait SstBuilder {
    async fn build(
        &mut self,
        request_id: RequestId,
        meta: &SstMetaData,
        record_stream: RecordBatchStream,
    ) -> Result<SstInfo>;
}
