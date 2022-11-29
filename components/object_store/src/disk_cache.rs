// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! An ObjectStore implementation with disk as cache.
//! The disk cache is a read-through caching, with page as its minimal cache
//! unit.
//!
//! Page is used for reasons below:
//! - reduce file size in case of there are too many request with small range.

use std::{fmt::Display, fs, ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use log::error;
use lru::LruCache;
use prost::Message;
use snafu::{Backtrace, ResultExt, Snafu};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWrite},
    sync::Mutex,
};
use upstream::{
    path::Path, Error as ObjectStoreError, GetResult, ListResult, MultipartId, ObjectMeta,
    ObjectStore, Result,
};

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display(
        "IO failed, file:{}, source:{}.\nbacktrace:\n{}",
        file,
        source,
        backtrace
    ))]
    IoError {
        file: String,
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to decode prost, file:{}, source:{}.\nbacktrace:\n{}",
        file,
        source,
        backtrace
    ))]
    DecodeError {
        file: String,
        source: prost::DecodeError,
        backtrace: Backtrace,
    },
}

impl From<Error> for ObjectStoreError {
    fn from(source: Error) -> Self {
        Self::Generic {
            store: "DiskCacheStore",
            source: Box::new(source),
        }
    }
}

struct CachedBytes {
    file_path: String,
}

impl Drop for CachedBytes {
    fn drop(&mut self) {
        if let Err(e) = fs::remove_file(&self.file_path) {
            error!(
                "Remove disk cache failed, key:{}, err:{}",
                self.file_path, e
            );
        }
    }
}

impl CachedBytes {
    fn new(file_path: String) -> Self {
        Self { file_path }
    }

    async fn persist(&self, _key: &str, _value: Bytes) -> Result<()> {
        todo!()
    }

    async fn to_bytes(&self) -> Result<Bytes> {
        let mut f = File::open(&self.file_path).await.with_context(|| IoError {
            file: self.file_path.clone(),
        })?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).await.with_context(|| IoError {
            file: self.file_path.clone(),
        })?;

        let bytes = proto::cache::Bytes::decode(&*buf).with_context(|| DecodeError {
            file: self.file_path.clone(),
        })?;

        Ok(bytes.value.into())
    }
}

#[derive(Debug)]
struct DiskCache {
    root_dir: String,
    cache: Mutex<LruCache<String, CachedBytes>>,
}

impl DiskCache {
    fn new(root_dir: String, cap: usize) -> Self {
        Self {
            root_dir,
            cache: Mutex::new(LruCache::new(cap)),
        }
    }

    fn normalize_filename(key: &str) -> String {
        key.replace('/', "_")
    }

    async fn insert(&self, key: String, value: Bytes) -> Result<()> {
        let mut cache = self.cache.lock().await;
        let file_path = std::path::Path::new(&self.root_dir)
            .join(Self::normalize_filename(&key))
            .into_os_string()
            .into_string()
            .unwrap();
        let bytes = CachedBytes::new(file_path);
        bytes.persist(&key, value).await?;
        cache.push(key, bytes);

        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        let mut cache = self.cache.lock().await;
        if let Some(cached_bytes) = cache.get(key) {
            // TODO: release lock when doing IO
            return cached_bytes.to_bytes().await.map(Some);
        }

        Ok(None)
    }
}

impl Display for DiskCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskCache")
            .field("path", &self.root_dir)
            .field("cache", &self.cache)
            .finish()
    }
}

#[derive(Debug)]
pub struct DiskCacheStore {
    cache: DiskCache,
    // Max disk capacity cache use can
    cap: usize,
    page_size: usize,
    underlying_store: Arc<dyn ObjectStore>,
}

impl DiskCacheStore {
    pub fn new(
        root_dir: String,
        cap: usize,
        page_size: usize,
        underlying_store: Arc<dyn ObjectStore>,
    ) -> Self {
        let cache = DiskCache::new(root_dir, cap);

        Self {
            cache,
            cap,
            page_size,
            underlying_store,
        }
    }

    fn normalize_range(&self, range: &Range<usize>) -> Range<usize> {
        let start = range.start / self.page_size * self.page_size;
        let end = (range.end + self.page_size) / self.page_size * self.page_size;

        start..end
    }

    fn cache_key(location: &Path, range: &Range<usize>) -> String {
        format!("{}-{}-{}", location, range.start, range.end)
    }
}

impl Display for DiskCacheStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskCacheStore")
            .field("page_size", &self.page_size)
            .field("cap", &self.cap)
            .field("cache", &self.cache)
            .finish()
    }
}

#[async_trait]
impl ObjectStore for DiskCacheStore {
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

#[cfg(test)]
mod test {
    use tempfile::tempdir;
    use upstream::local::LocalFileSystem;

    use super::*;

    fn prepare_store(page_size: usize, cap: usize) -> DiskCacheStore {
        let local_path = tempdir().unwrap();
        let local_store = Arc::new(LocalFileSystem::new_with_prefix(local_path.path()).unwrap());

        DiskCacheStore::new("/tmp".to_string(), cap, page_size, local_store)
    }

    #[test]
    fn test_normalize_range() {
        let page_size = 4096;
        let testcases = vec![(0..1, 0..4096), (0..4096, 0..8192)];

        let store = prepare_store(page_size, 1000);
        for (input, expected) in testcases {
            assert_eq!(store.normalize_range(&input), expected);
        }
    }
}
