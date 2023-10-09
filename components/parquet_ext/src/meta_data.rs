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

use std::{ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder;
use generic_error::GenericResult;
use parquet::{
    arrow::arrow_reader::ArrowReaderOptions,
    errors::{ParquetError, Result},
    file::{footer, metadata::ParquetMetaData},
};

use crate::reader::ObjectStoreReader;

#[async_trait]
pub trait ChunkReader: Sync + Send {
    async fn get_bytes(&self, range: Range<usize>) -> GenericResult<Bytes>;
}

/// Fetch and parse [`ParquetMetadata`] from the file reader.
///
/// Referring to: https://github.com/apache/arrow-datafusion/blob/ac2e5d15e5452e83c835d793a95335e87bf35569/datafusion/core/src/datasource/file_format/parquet.rs#L390-L449
pub async fn fetch_parquet_metadata(
    file_size: usize,
    file_reader: &dyn ChunkReader,
) -> Result<(ParquetMetaData, usize)> {
    const FOOTER_LEN: usize = 8;

    if file_size < FOOTER_LEN {
        let err_msg = format!("file size of {file_size} is less than footer");
        return Err(ParquetError::General(err_msg));
    }

    let footer_start = file_size - FOOTER_LEN;

    let footer_bytes = file_reader
        .get_bytes(footer_start..file_size)
        .await
        .map_err(|e| {
            let err_msg = format!("failed to get footer bytes, err:{e}");
            ParquetError::General(err_msg)
        })?;

    assert_eq!(footer_bytes.len(), FOOTER_LEN);
    let mut footer = [0; FOOTER_LEN];
    footer.copy_from_slice(&footer_bytes);

    let metadata_len = footer::decode_footer(&footer)?;

    if file_size < metadata_len + FOOTER_LEN {
        let err_msg = format!(
            "file size of {} is smaller than footer + metadata {}",
            file_size,
            metadata_len + FOOTER_LEN
        );
        return Err(ParquetError::General(err_msg));
    }

    let metadata_start = file_size - metadata_len - FOOTER_LEN;
    let metadata_bytes = file_reader
        .get_bytes(metadata_start..footer_start)
        .await
        .map_err(|e| {
            let err_msg = format!("failed to get metadata bytes, err:{e}");
            ParquetError::General(err_msg)
        })?;

    footer::decode_metadata(&metadata_bytes).map(|v| (v, metadata_len))
}

/// Build page indexes for meta data
///
/// TODO: Currently there is no method to build page indexes for meta data in
/// `parquet`, maybe we can write a issue in `arrow-rs` .
pub async fn meta_with_page_indexes(
    object_store_reader: ObjectStoreReader,
) -> Result<Arc<ParquetMetaData>> {
    let read_options = ArrowReaderOptions::new().with_page_index(true);
    let builder =
        ParquetRecordBatchStreamBuilder::new_with_options(object_store_reader, read_options)
            .await
            .map_err(|e| {
                let err_msg = format!("failed to build page indexes in metadata, err:{e}");
                ParquetError::General(err_msg)
            })?;
    Ok(builder.metadata().clone())
}
