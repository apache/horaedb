// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{fmt::Display, ops::Range, time::Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use lazy_static::lazy_static;
use log::info;
use prometheus::{exponential_buckets, register_histogram_vec, HistogramVec};
use prometheus_static_metric::make_static_metric;
use tokio::io::AsyncWrite;
use upstream::{path::Path, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Result};

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
}

lazy_static! {
    pub static ref OBJECT_STORE_DURATION_HISTOGRAM: ObjectStoreDurationHistogram =
        ObjectStoreDurationHistogram::from(&OBJECT_STORE_DURATION_HISTOGRAM_VEC);
    pub static ref OBJECT_STORE_THROUGHPUT_HISTOGRAM: ObjectStoreThroughputHistogram =
        ObjectStoreThroughputHistogram::from(&OBJECT_STORE_THROUGHPUT_HISTOGRAM_VEC);
}

/// A object store wrapper for collecting statistics about the underlying store.
#[derive(Debug)]
pub struct StoreWithMetrics {
    store: ObjectStoreRef,
}

impl StoreWithMetrics {
    pub fn new(store: ObjectStoreRef) -> Self {
        Self { store }
    }
}

impl Display for StoreWithMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Store with metrics, underlying store:{}", self.store)
    }
}
use tokio::runtime::Handle;

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
        self.store.put_multipart(location).await
    }

    async fn abort_multipart(&self, location: &Path, multipart_id: &MultipartId) -> Result<()> {
        let _timer = OBJECT_STORE_DURATION_HISTOGRAM
            .abort_multipart
            .start_timer();
        self.store.abort_multipart(location, multipart_id).await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let _timer = OBJECT_STORE_DURATION_HISTOGRAM.get.start_timer();
        self.store.get(location).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let handle = Handle::current();
        let instant = Instant::now();
        let _timer = OBJECT_STORE_DURATION_HISTOGRAM.get_range.start_timer();
        let result = self.store.get_range(location, range).await?;
        info!(
            "object store metrics get_range cost:{}, location:{}, handle:{:?}",
            instant.elapsed().as_millis(),
            location,
            handle
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
            .get_range
            .observe(len as f64);
        Ok(result)
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let instant = Instant::now();
        let _timer = OBJECT_STORE_DURATION_HISTOGRAM.head.start_timer();
        let response = self.store.head(location).await;
        info!(
            "object store metrics head cost:{}, location:{}",
            instant.elapsed().as_millis(),
            location
        );
        response
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let _timer = OBJECT_STORE_DURATION_HISTOGRAM.delete.start_timer();
        self.store.delete(location).await
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
