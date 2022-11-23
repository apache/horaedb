use std::{
    collections::hash_map::DefaultHasher,
    fmt::Display,
    hash::{Hash, Hasher},
    ops::Range,
    sync::Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use lru_weighted_cache::{LruWeightedCache, Weighted};
use tokio::{io::AsyncWrite, sync::Mutex};
use upstream::{path::Path, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Result};

struct CachedBytes(Bytes);

impl Weighted for CachedBytes {
    fn weight(&self) -> usize {
        self.0.len()
    }
}

#[derive(Debug)]
struct Partition {
    inner: Mutex<LruWeightedCache<String, CachedBytes>>,
}

impl Partition {
    fn new(mem_cap: usize) -> Self {
        Self {
            inner: Mutex::new(LruWeightedCache::new(1, mem_cap).expect("invalid params")),
        }
    }
}
impl Partition {
    async fn get(&self, key: &String) -> Option<Bytes> {
        let mut guard = self.inner.lock().await;
        guard.get(key).map(|v| v.0.clone())
    }

    async fn insert(&self, key: String, value: Bytes) {
        let mut guard = self.inner.lock().await;
        // don't care error now.
        _ = guard.insert(key, CachedBytes(value));
    }
}

#[derive(Debug)]
struct MemCache {
    /// Max memory this store can use
    mem_cap: usize,
    partitions: Vec<Arc<Partition>>,
    partition_mask: usize,
}

impl MemCache {
    fn new(partition_bits: usize, mem_cap: usize) -> Self {
        let partition_num = 1 << partition_bits;
        let cap_per_part = mem_cap / partition_num;
        let partitions = (0..partition_num)
            .map(|_| Arc::new(Partition::new(cap_per_part)))
            .collect::<Vec<_>>();

        Self {
            mem_cap,
            partitions,
            partition_mask: partition_num - 1,
        }
    }

    fn locate_partition(&self, key: &String) -> Arc<Partition> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        self.partitions[hasher.finish() as usize & self.partition_mask].clone()
    }

    async fn get(&self, key: &String) -> Option<Bytes> {
        let partition = self.locate_partition(key);
        partition.get(key).await
    }

    async fn insert(&self, key: String, value: Bytes) {
        let partition = self.locate_partition(&key);
        partition.insert(key, value).await;
    }
}

impl Display for MemCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemCache")
            .field("mem_cap", &self.mem_cap)
            .field("mask", &self.partition_mask)
            .finish()
    }
}

#[derive(Debug)]
pub struct CachedStore {
    cache: MemCache,
    inner: Arc<dyn ObjectStore>,
}

impl CachedStore {
    pub fn new(
        partition_bits: usize,
        mem_cap: usize,
        underlying_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self {
            cache: MemCache::new(partition_bits, mem_cap),
            inner: underlying_store,
        }
    }

    fn cache_key(location: &Path, range: &Range<usize>) -> String {
        format!("{}-{}-{}", location, range.start, range.end)
    }
}

impl Display for CachedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.cache.fmt(f)
    }
}

#[async_trait]
impl ObjectStore for CachedStore {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        self.inner.put(location, bytes).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        self.inner.put_multipart(location).await
    }

    async fn abort_multipart(&self, location: &Path, multipart_id: &MultipartId) -> Result<()> {
        self.inner.abort_multipart(location, multipart_id).await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        self.inner.get(location).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let cache_key = Self::cache_key(location, &range);
        if let Some(bytes) = self.cache.get(&cache_key).await {
            return Ok(bytes);
        }

        let bytes = self.inner.get_range(location, range).await;
        if let Ok(bytes) = &bytes {
            self.cache.insert(cache_key, bytes.clone()).await;
        }

        bytes
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.inner.delete(location).await
    }

    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        self.inner.list(prefix).await
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}
