// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{ops::Range, sync::Arc, time::Instant};

use bytes::Bytes;
use futures::{
    future::{BoxFuture, FutureExt},
    TryFutureExt,
};
use log::debug;
use object_store::{ObjectStoreRef, Path};
use parquet::{arrow::async_reader::AsyncFileReader, file::metadata::ParquetMetaData};

/// Implemention AsyncFileReader based on `ObjectStore`
///
/// TODO: Perhaps we should avoid importing `object_store` in `parquet_ext` to
/// keep the crate `parquet_ext` more pure.
#[derive(Clone)]
pub struct ObjectStoreReader {
    storage: ObjectStoreRef,
    path: Path,
    meta_data: Arc<ParquetMetaData>,
    begin: Instant,
}

impl ObjectStoreReader {
    pub fn new(storage: ObjectStoreRef, path: Path, meta_data: Arc<ParquetMetaData>) -> Self {
        Self {
            storage,
            path,
            meta_data,
            begin: Instant::now(),
        }
    }
}

impl Drop for ObjectStoreReader {
    fn drop(&mut self) {
        debug!(
            "ObjectStoreReader dropped, path:{}, elapsed:{:?}",
            &self.path,
            self.begin.elapsed()
        );
    }
}

impl AsyncFileReader for ObjectStoreReader {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.storage
            .get_range(&self.path, range)
            .map_err(|e| {
                parquet::errors::ParquetError::General(format!(
                    "Failed to fetch range from object store, err:{e}"
                ))
            })
            .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        async move {
            self.storage
                .get_ranges(&self.path, &ranges)
                .map_err(|e| {
                    parquet::errors::ParquetError::General(format!(
                        "Failed to fetch ranges from object store, err:{e}"
                    ))
                })
                .await
        }
        .boxed()
    }

    fn get_metadata(
        &mut self,
    ) -> BoxFuture<'_, parquet::errors::Result<Arc<parquet::file::metadata::ParquetMetaData>>> {
        Box::pin(async move { Ok(self.meta_data.clone()) })
    }
}
