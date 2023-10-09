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

use std::{fmt::Display, ops::Range, sync::Arc, thread, time::Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use lazy_static::lazy_static;
use log::trace;
use prometheus::{
    exponential_buckets, register_histogram_vec, register_int_counter, HistogramVec, IntCounter,
};
use prometheus_static_metric::make_static_metric;
use runtime::Runtime;
use tokio::io::AsyncWrite;
use upstream::{
    path::Path, Error as StoreError, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore,
    Result,
};

use crate::ObjectStoreRef;

make_static_metric! {
    pub struct ObjectStoreDurationHistogram: Histogram {
        "op" => {
            put,
            put_multipart,
            abort_multipart,
            get,
            get_range,
            get_ranges,
            head,
            delete,
            list,
            list_with_delimiter,
            copy,
            rename,
            copy_if_not_exists,
            rename_if_not_exists
        },
    }

    pub struct ObjectStoreThroughputHistogram: Histogram {
        "op" => {
            put,
            get_range,
            get_ranges,
        },
    }
}

lazy_static! {
    static ref OBJECT_STORE_DURATION_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "object_store_latency",
        "latency of object store's operation",
        &["op"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    static ref OBJECT_STORE_THROUGHPUT_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "object_store_throughput",
        "throughput of object store's operation",
        &["op"],
        // The max bound value is 64 * 2^24 = 1GB
        exponential_buckets(64.0, 4.0, 12).unwrap()
    )
    .unwrap();
    pub static ref OBJECT_STORE_MEMORY_CACHE_HIT: IntCounter = register_int_counter!(
        "object_store_memory_cache_hit",
        "object store memory cache hit"
    )
    .unwrap();
    pub static ref OBJECT_STORE_MEMORY_CACHE_MISS: IntCounter = register_int_counter!(
        "object_store_memory_cache_miss",
        "object store memory cache miss"
    )
    .unwrap();
    pub static ref OBJECT_STORE_DISK_CACHE_HIT: IntCounter = register_int_counter!(
        "object_store_disk_cache_hit",
        "object store disk cache hit"
    )
    .unwrap();
    pub static ref OBJECT_STORE_DISK_CACHE_MISS: IntCounter = register_int_counter!(
        "object_store_disk_cache_miss",
        "object store disk cache miss"
    )
    .unwrap();
}

lazy_static! {
    pub static ref DISK_CACHE_DEDUP_COUNT: IntCounter = register_int_counter!(
        "disk_cache_dedup_counter",
        "Dedup disk cache fetch request counts"
    )
    .unwrap();
}

lazy_static! {
    pub static ref OBJECT_STORE_DURATION_HISTOGRAM: ObjectStoreDurationHistogram =
        ObjectStoreDurationHistogram::from(&OBJECT_STORE_DURATION_HISTOGRAM_VEC);
    pub static ref OBJECT_STORE_THROUGHPUT_HISTOGRAM: ObjectStoreThroughputHistogram =
        ObjectStoreThroughputHistogram::from(&OBJECT_STORE_THROUGHPUT_HISTOGRAM_VEC);
}

pub const METRICS: &str = "METRICS";
/// A object store wrapper for collecting statistics about the underlying store.
#[derive(Debug)]
pub struct StoreWithMetrics {
    store: ObjectStoreRef,
    /// Use a separate runtime to execute object store methods;
    /// Prevent computationally intensive tasks from occupying the runtime for a
    /// long time and causing an increase in access time.
    runtime: Arc<Runtime>,
}

impl StoreWithMetrics {
    pub fn new(store: ObjectStoreRef, runtime: Arc<Runtime>) -> Self {
        Self { store, runtime }
    }
}

impl Display for StoreWithMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Store with metrics, underlying store:{}", self.store)
    }
}

#[async_trait]
impl ObjectStore for StoreWithMetrics {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let _timer = OBJECT_STORE_DURATION_HISTOGRAM.put.start_timer();
        OBJECT_STORE_THROUGHPUT_HISTOGRAM
            .put
            .observe(bytes.len() as f64);
        self.store.put(location, bytes).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        let _timer = OBJECT_STORE_DURATION_HISTOGRAM.put_multipart.start_timer();

        let instant = Instant::now();
        let loc = location.clone();
        let store = self.store.clone();
        let res = self
            .runtime
            .spawn(async move { store.put_multipart(&loc).await })
            .await
            .map_err(|source| StoreError::Generic {
                store: METRICS,
                source: Box::new(source),
            })?;

        trace!(
            "Object store with metrics put_multipart cost:{}ms, location:{}, thread:{}-{:?}",
            instant.elapsed().as_millis(),
            location,
            thread::current().name().unwrap_or("noname").to_string(),
            thread::current().id()
        );
        res
    }

    async fn abort_multipart(&self, location: &Path, multipart_id: &MultipartId) -> Result<()> {
        let _timer = OBJECT_STORE_DURATION_HISTOGRAM
            .abort_multipart
            .start_timer();
        self.store.abort_multipart(location, multipart_id).await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let _timer = OBJECT_STORE_DURATION_HISTOGRAM.get.start_timer();
        let store = self.store.clone();
        let loc = location.clone();
        self.runtime
            .spawn(async move { store.get(&loc).await })
            .await
            .map_err(|source| StoreError::Generic {
                store: METRICS,
                source: Box::new(source),
            })?
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let _timer = OBJECT_STORE_DURATION_HISTOGRAM.get_range.start_timer();

        let instant = Instant::now();
        let store = self.store.clone();
        let loc = location.clone();
        let result = self
            .runtime
            .spawn(async move { store.get_range(&loc, range.clone()).await })
            .await
            .map_err(|source| StoreError::Generic {
                store: METRICS,
                source: Box::new(source),
            })??;
        trace!(
            "Object store with metrics get_range cost:{}ms, location:{}, thread:{}-{:?}",
            instant.elapsed().as_millis(),
            location,
            thread::current().name().unwrap_or("noname").to_string(),
            thread::current().id()
        );

        OBJECT_STORE_THROUGHPUT_HISTOGRAM
            .get_range
            .observe(result.len() as f64);
        Ok(result)
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        let _timer = OBJECT_STORE_DURATION_HISTOGRAM.get_ranges.start_timer();
        let result = self.store.get_ranges(location, ranges).await?;
        let len: usize = result.iter().map(|v| v.len()).sum();
        OBJECT_STORE_THROUGHPUT_HISTOGRAM
            .get_ranges
            .observe(len as f64);
        Ok(result)
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let _timer = OBJECT_STORE_DURATION_HISTOGRAM.head.start_timer();

        let instant = Instant::now();
        let store = self.store.clone();
        let loc = location.clone();
        let response = self
            .runtime
            .spawn(async move { store.head(&loc).await })
            .await
            .map_err(|source| StoreError::Generic {
                store: METRICS,
                source: Box::new(source),
            })?;

        trace!(
            "Object store with metrics head cost:{}ms, location:{}",
            instant.elapsed().as_millis(),
            location
        );
        response
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let _timer = OBJECT_STORE_DURATION_HISTOGRAM.delete.start_timer();
        let store = self.store.clone();
        let loc = location.clone();
        self.runtime
            .spawn(async move { store.delete(&loc).await })
            .await
            .map_err(|source| StoreError::Generic {
                store: METRICS,
                source: Box::new(source),
            })?
    }

    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        let _timer = OBJECT_STORE_DURATION_HISTOGRAM.list.start_timer();
        self.store.list(prefix).await
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let _timer = OBJECT_STORE_DURATION_HISTOGRAM
            .list_with_delimiter
            .start_timer();
        self.store.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let _timer = OBJECT_STORE_DURATION_HISTOGRAM.copy.start_timer();
        self.store.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let _timer = OBJECT_STORE_DURATION_HISTOGRAM.rename.start_timer();
        self.store.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let _timer = OBJECT_STORE_DURATION_HISTOGRAM
            .copy_if_not_exists
            .start_timer();
        self.store.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let _timer = OBJECT_STORE_DURATION_HISTOGRAM
            .rename_if_not_exists
            .start_timer();
        self.store.rename_if_not_exists(from, to).await
    }
}
