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

use std::{ops::Range, sync::Arc, time::Instant};

use bytes::Bytes;
use futures::{
    future::{BoxFuture, FutureExt},
    TryFutureExt,
};
use logger::debug;
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
