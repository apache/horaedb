// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! An ObjectStore implementation with disk as cache.
//! The disk cache is a read-through caching, with page as its minimal cache
//! unit.
//!
//! Page is used for reasons below:
//! - reduce file size in case of there are too many request with small range.

use std::{fmt::Display, ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use lru::LruCache;
use tokio::{io::AsyncWrite, sync::Mutex};
use upstream::{path::Path, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Result};

struct CachedBytes {
    bytes: Bytes,
}

#[derive(Debug)]
struct DiskCache {
    path: String,
    cache: Mutex<LruCache<String, Bytes>>,
}

impl DiskCache {
    fn new(path: String, cap: usize) -> Self {
        Self {
            path,
            cache: Mutex::new(LruCache::new(cap)),
        }
    }

    async fn insert(&self, key: String, value: Bytes) -> Result<()> {
        todo!()
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        todo!()
    }
}

impl Display for DiskCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskCache")
            .field("path", &self.path)
            .field("cache", &self.cache)
            .finish()
    }
}

#[derive(Debug)]
pub struct DiskStore {
    cache: DiskCache,
    // Max disk capacity cache use can
    cap: usize,
    page_size: usize,
    underlying_store: Arc<dyn ObjectStore>,
}

impl DiskStore {
    fn normalize_range(&self, range: &Range<usize>) -> Range<usize> {
        todo!()
    }

    fn cache_key(location: &Path, range: &Range<usize>) -> String {
        format!("{}-{}-{}", location, range.start, range.end)
    }
}

impl Display for DiskStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskStore")
            .field("page_size", &self.page_size)
            .field("cap", &self.cap)
            .field("cache", &self.cache)
            .finish()
    }
}

#[async_trait]
impl ObjectStore for DiskStore {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        self.underlying_store.put(location, bytes).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        self.underlying_store.put_multipart(location).await
    }

    async fn abort_multipart(&self, location: &Path, multipart_id: &MultipartId) -> Result<()> {
        self.underlying_store
            .abort_multipart(location, multipart_id)
            .await
    }

    // TODO(chenxiang): don't cache whole path for reasons below
    // 1. cache key don't support overlapping
    // 2. In sst module, we only use get_range, get is not used
    async fn get(&self, location: &Path) -> Result<GetResult> {
        self.underlying_store.get(location).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let cache_key = Self::cache_key(location, &range);
        if let Some(bytes) = self.cache.get(&cache_key).await? {
            return Ok(bytes);
        }

        let bytes = self.underlying_store.get_range(location, range).await;
        if let Ok(bytes) = &bytes {
            self.cache.insert(cache_key, bytes.clone()).await?;
        }

        bytes
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.underlying_store.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.underlying_store.delete(location).await
    }

    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        self.underlying_store.list(prefix).await
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.underlying_store.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.underlying_store.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.underlying_store.copy_if_not_exists(from, to).await
    }
}
