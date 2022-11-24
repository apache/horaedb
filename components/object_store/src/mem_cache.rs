// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! An implementation of ObjectStore, which support
//! 1. Cache based on memory, and support evict based on memory usage
//! 2. Builtin Partition to reduce lock contention

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
            .field("partitons", &self.partitions)
            .finish()
    }
}

#[derive(Debug)]
pub struct CachedStore {
    cache: MemCache,
    underlying_store: Arc<dyn ObjectStore>,
}

impl CachedStore {
    pub fn new(
        partition_bits: usize,
        mem_cap: usize,
        underlying_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self {
            cache: MemCache::new(partition_bits, mem_cap),
            underlying_store,
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

    async fn get(&self, location: &Path) -> Result<GetResult> {
        self.underlying_store.get(location).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let cache_key = Self::cache_key(location, &range);
        if let Some(bytes) = self.cache.get(&cache_key).await {
            return Ok(bytes);
        }

        // TODO(chenxiang): What if two threads reach here? It's better to
        // pend one thread, and only let one to fetch data from underlying store.
        let bytes = self.underlying_store.get_range(location, range).await;
        if let Ok(bytes) = &bytes {
            self.cache.insert(cache_key, bytes.clone()).await;
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

    async fn prepare_store(bits: usize, mem_cap: usize) -> CachedStore {
        let local_path = tempdir().unwrap();
        let local_store = Arc::new(LocalFileSystem::new_with_prefix(local_path.path()).unwrap());

        CachedStore::new(bits, mem_cap, local_store)
    }

    #[tokio::test]
    async fn test_mem_cache_evict() {
        // single partition
        let store = prepare_store(0, 13).await;

        // write date
        let location = Path::from("1.sst");
        store
            .put(&location, Bytes::from_static(&[1; 1024]))
            .await
            .unwrap();

        // get bytes from [0, 5), insert to cache
        let range0_5 = 0..5;
        _ = store.get_range(&location, range0_5.clone()).await.unwrap();
        assert!(store
            .cache
            .get(&CachedStore::cache_key(&location, &range0_5))
            .await
            .is_some());
        assert_eq!(
            r#"MemCache { mem_cap: 13, mask: 0, partitons: [Partition { inner: Mutex { data: LruWeightedCache { max_item_weight: 13, max_total_weight: 13, current_weight: 5 } } }] }"#,
            format!("{}", store)
        );

        // get bytes from [5, 10), insert to cache
        let range5_10 = 5..10;
        _ = store.get_range(&location, range5_10.clone()).await.unwrap();
        assert!(store
            .cache
            .get(&CachedStore::cache_key(&location, &range0_5))
            .await
            .is_some());
        assert!(store
            .cache
            .get(&CachedStore::cache_key(&location, &range5_10))
            .await
            .is_some());
        assert_eq!(
            r#"MemCache { mem_cap: 13, mask: 0, partitons: [Partition { inner: Mutex { data: LruWeightedCache { max_item_weight: 13, max_total_weight: 13, current_weight: 10 } } }] }"#,
            format!("{}", store)
        );

        // get bytes from [5, 10), insert to cache
        // cache is full, evict [0, 5)
        let range10_15 = 5..10;
        _ = store
            .get_range(&location, range10_15.clone())
            .await
            .unwrap();
        assert!(store
            .cache
            .get(&CachedStore::cache_key(&location, &range5_10))
            .await
            .is_some());
        assert!(store
            .cache
            .get(&CachedStore::cache_key(&location, &range10_15))
            .await
            .is_some());
        assert_eq!(
            r#"MemCache { mem_cap: 13, mask: 0, partitons: [Partition { inner: Mutex { data: LruWeightedCache { max_item_weight: 13, max_total_weight: 13, current_weight: 10 } } }] }"#,
            format!("{}", store)
        );

        let range10_13 = 10..13;
        _ = store
            .get_range(&location, range10_13.clone())
            .await
            .unwrap();
        assert_eq!(
            r#"MemCache { mem_cap: 13, mask: 0, partitons: [Partition { inner: Mutex { data: LruWeightedCache { max_item_weight: 13, max_total_weight: 13, current_weight: 13 } } }] }"#,
            format!("{}", store)
        );
    }

    #[tokio::test]
    async fn test_mem_cache_partition() {
        // 4 partitions
        let store = prepare_store(2, 100).await;
        let location = Path::from("partition.sst");
        store
            .put(&location, Bytes::from_static(&[1; 1024]))
            .await
            .unwrap();

        let range0_5 = 0..5;
        let range100_105 = 100..105;
        _ = store.get_range(&location, range0_5.clone()).await.unwrap();
        _ = store
            .get_range(&location, range100_105.clone())
            .await
            .unwrap();

        assert_eq!(
            r#"MemCache { mem_cap: 100, mask: 3, partitons: [Partition { inner: Mutex { data: LruWeightedCache { max_item_weight: 25, max_total_weight: 25, current_weight: 0 } } }, Partition { inner: Mutex { data: LruWeightedCache { max_item_weight: 25, max_total_weight: 25, current_weight: 5 } } }, Partition { inner: Mutex { data: LruWeightedCache { max_item_weight: 25, max_total_weight: 25, current_weight: 0 } } }, Partition { inner: Mutex { data: LruWeightedCache { max_item_weight: 25, max_total_weight: 25, current_weight: 5 } } }] }"#,
            format!("{}", store)
        );

        assert!(store
            .cache
            .get(&CachedStore::cache_key(&location, &range0_5))
            .await
            .is_some());
        assert!(store
            .cache
            .get(&CachedStore::cache_key(&location, &range100_105))
            .await
            .is_some());
    }
}
