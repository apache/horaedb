// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{
    ops::Range,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures::{
    future::{BoxFuture, FutureExt},
    TryFutureExt,
};
use object_store::{ObjectStoreRef, Path};
use parquet::{arrow::async_reader::AsyncFileReader, file::metadata::ParquetMetaData};

/// The observer for metrics of [ObjectStoreReader].
pub trait MetricsObserver: Send {
    fn elapsed(&self, path: &Path, elapsed: Duration);
    fn num_bytes_fetched(&self, path: &Path, num_bytes: usize);
}

#[derive(Debug, Clone)]
pub struct NoopMetricsObserver;

impl MetricsObserver for NoopMetricsObserver {
    fn elapsed(&self, _: &Path, _: Duration) {}

    fn num_bytes_fetched(&self, _: &Path, _: usize) {}
}

/// The implementation based on `ObjectStore` for [`AsyncFileReader`].
#[derive(Clone)]
pub struct ObjectStoreReader<T: MetricsObserver> {
    storage: ObjectStoreRef,
    path: Path,
    meta_data: Arc<ParquetMetaData>,
    begin: Instant,
    metrics: T,
}

impl ObjectStoreReader<NoopMetricsObserver> {
    pub fn new(storage: ObjectStoreRef, path: Path, meta_data: Arc<ParquetMetaData>) -> Self {
        Self::with_metrics(storage, path, meta_data, NoopMetricsObserver)
    }
}

impl<T: MetricsObserver> ObjectStoreReader<T> {
    pub fn with_metrics(
        storage: ObjectStoreRef,
        path: Path,
        meta_data: Arc<ParquetMetaData>,
        metrics: T,
    ) -> Self {
        Self {
            storage,
            path,
            meta_data,
            begin: Instant::now(),
            metrics,
        }
    }
}

impl<T: MetricsObserver> Drop for ObjectStoreReader<T> {
    fn drop(&mut self) {
        self.metrics.elapsed(&self.path, self.begin.elapsed())
    }
}

impl<T: MetricsObserver> AsyncFileReader for ObjectStoreReader<T> {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        async move {
            let get_res = self
                .storage
                .get_range(&self.path, range)
                .map_err(|e| {
                    parquet::errors::ParquetError::General(format!(
                        "Failed to fetch range from object store, err:{e}"
                    ))
                })
                .await;

            if let Ok(bytes) = &get_res {
                self.metrics.num_bytes_fetched(&self.path, bytes.len());
            }

            get_res
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        async move {
            let get_res = self
                .storage
                .get_ranges(&self.path, &ranges)
                .map_err(|e| {
                    parquet::errors::ParquetError::General(format!(
                        "Failed to fetch ranges from object store, err:{e}"
                    ))
                })
                .await;

            if let Ok(bytes) = &get_res {
                let num_bytes: usize = bytes.iter().map(|v| v.len()).sum();
                self.metrics.num_bytes_fetched(&self.path, num_bytes);
            }

            get_res
        }
        .boxed()
    }

    fn get_metadata(
        &mut self,
    ) -> BoxFuture<'_, parquet::errors::Result<Arc<parquet::file::metadata::ParquetMetaData>>> {
        Box::pin(async move { Ok(self.meta_data.clone()) })
    }
}
