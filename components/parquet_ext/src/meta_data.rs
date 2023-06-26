// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::ops::Range;

use async_trait::async_trait;
use bytes::Bytes;
use common_util::error::GenericResult;
use parquet::{
    errors::{ParquetError, Result},
    file::{footer, metadata::ParquetMetaData},
};

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
