// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{fmt::Display, ops::Range};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt};
use lazy_static::lazy_static;
use prometheus::{
    exponential_buckets, register_histogram_vec, register_int_gauge_vec, HistogramVec, IntGauge,
    IntGaugeVec,
};
use tokio::io::AsyncWrite;
use upstream::{
    path::{self, Path},
    GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Result,
};

use crate::ObjectStoreRef;

lazy_static! {
    static ref LATENCY_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "object_store_latency",
        "latency of object store's operation",
        &["name", "op"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    static ref THROUGHPUT_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "object_store_throughput",
        "throughput of object store's operation",
        &["name", "op"],
        // The max bound value is 64 * 2^24 = 1GB
        exponential_buckets(64.0, 4.0, 12).unwrap()
    )
    .unwrap();
}

/// A object store wrapper for collecting statistics about the underlying store.
#[derive(Debug)]
pub struct StoreWithMetrics {
    store: ObjectStoreRef,
}

impl Display for StoreWithMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Store with metrics, underlying store:{}", self.store)
    }
}

#[async_trait]
impl ObjectStore for StoreWithMetrics {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        self.store.put(location, bytes).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        self.store.put_multipart(location).await
    }

    async fn abort_multipart(&self, location: &Path, multipart_id: &MultipartId) -> Result<()> {
        self.store.abort_multipart(location, multipart_id).await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        self.store.get(location).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        self.store.get_range(location, range).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        self.store.get_ranges(location, ranges).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.store.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.store.delete(location).await
    }

    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        self.store.list(prefix).await
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.store.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.store.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.store.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.store.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.store.rename_if_not_exists(from, to).await
    }
}
