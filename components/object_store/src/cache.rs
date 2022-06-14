// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Cached object store wrapper.

use std::{fmt::Display, ops::Range};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::MIN_DATETIME;
use futures::{future::try_join_all, lock::Mutex, stream::BoxStream, TryStreamExt};
use lru::LruCache;
use upstream::{path::Path, GetResult, ListResult, ObjectMeta, ObjectStore, Result};

#[derive(Debug)]
pub struct CachedStoreConfig {
    pub max_cache_size: usize,
}

#[derive(Debug)]
pub struct CachedStore {
    local_store: Box<dyn ObjectStore>,
    remote_store: Box<dyn ObjectStore>,
    state: Mutex<CacheState>,
}

impl CachedStore {
    pub async fn init(
        local_store: Box<dyn ObjectStore>,
        remote_store: Box<dyn ObjectStore>,
        config: CachedStoreConfig,
    ) -> Result<Self> {
        let local_list: Vec<ObjectMeta> = local_store.list(None).await?.try_collect().await?;
        let mut state = CacheState::new(config.max_cache_size, local_list);
        let (removed, _) = state.reserve(0);
        if !removed.is_empty() {
            Self::remove_paths(local_store.as_ref(), removed).await?;
        }

        Ok(Self {
            local_store,
            remote_store,
            state: Mutex::new(state),
        })
    }

    /// Try putting object to local store. If local store cannot make enough
    /// space for the object, this function will skip putting it and return Ok.
    async fn put_local(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let required_size = bytes.len();

        let mut state = self.state.lock().await;
        let (removed, guard) = state.reserve(required_size);
        Self::remove_paths(self.local_store.as_ref(), removed).await?;
        drop(state);

        // cannot reserve enough space, skip to put local store
        if guard.is_none() {
            return Ok(());
        }

        let result = self.local_store.put(location, bytes.clone()).await;
        if result.is_err() {
            let _ = self.local_store.delete(location).await;
            self.state.lock().await.remove_guard(guard.unwrap());
        } else {
            self.state
                .lock()
                .await
                .consume_guard(guard.unwrap(), location);
        }
        result
    }

    async fn remove_paths(store: &dyn ObjectStore, paths: Vec<Path>) -> Result<()> {
        let tasks = paths
            .iter()
            .map(|path| store.delete(&path))
            .collect::<Vec<_>>();
        try_join_all(tasks).await?;
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for CachedStore {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let _ = self.put_local(location, bytes.clone()).await;

        self.remote_store.put(location, bytes).await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        if self.state.lock().await.contains(location) {
            self.local_store.get(location).await
        } else {
            let remote_obj = self.remote_store.get(location).await?;
            let bytes = remote_obj.bytes().await?;
            let _ = self.put_local(location, bytes).await?;
            self.local_store.get(location).await
        }
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        if self.state.lock().await.contains(location) {
            self.local_store.get_range(location, range).await
        } else {
            let remote_obj = self.remote_store.get(location).await?;
            let bytes = remote_obj.bytes().await?;
            let _ = self.put_local(location, bytes).await?;
            self.local_store.get_range(location, range).await
        }
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.remote_store.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let mut tasks = Vec::with_capacity(2);
        if self.state.lock().await.try_remove(location) {
            tasks.push(self.local_store.delete(location));
        }
        tasks.push(self.remote_store.delete(location));
        let result = futures::future::join_all(tasks.into_iter()).await;
        result.into_iter().collect::<Result<Vec<_>>>()?;

        Ok(())
    }

    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        self.remote_store.list(prefix).await
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.remote_store.list_with_delimiter(prefix).await
    }
}

impl Display for CachedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CachedStore with local storage {} and remote storage {}",
            self.local_store, self.remote_store
        )
    }
}

struct CacheState {
    max_size: usize,
    total_size: usize,
    cached_entries: LruCache<String, ObjectMeta>,
}

impl CacheState {
    fn new(max_size: usize, meta: Vec<ObjectMeta>) -> Self {
        let total_size = meta.iter().map(|m| m.size).sum();
        let mut cached_entries = LruCache::unbounded();
        for m in meta {
            cached_entries.put(m.location.to_string(), m);
        }

        Self {
            max_size,
            total_size,
            cached_entries,
        }
    }

    /// Try to remove a entries. Returns whether the entry was removed.
    fn try_remove(&mut self, location: &Path) -> bool {
        let removed = self.cached_entries.pop(&location.to_string());
        if let Some(removed) = &removed {
            self.total_size -= removed.size;
        }
        removed.is_some()
    }

    /// Check whether the local storage contains the given location. This will
    /// be treat as a cache read operation.
    fn contains(&mut self, location: &Path) -> bool {
        self.cached_entries.get(&location.to_string()).is_some()
    }

    /// Reserve space for a new entry. Returns a guard that stands for the
    /// space. It needs to be consumed by either [remove_guard] (cancel this
    /// reserve operation) or [consume_guard] (confirm this reserve operation).
    #[must_use]
    fn reserve(&mut self, size: usize) -> (Vec<Path>, Option<EntryGuard>) {
        if self.total_size + size <= self.max_size {
            self.total_size += size;
            return (Vec::new(), Some(EntryGuard { size }));
        }

        let mut removed = Vec::new();
        while self.total_size + size > self.max_size {
            let popped = self.cached_entries.pop_lru();
            if let Some(popped) = popped {
                self.total_size -= popped.1.size;
                removed.push(popped.1.location);
            } else {
                return (removed, None);
            }
        }
        self.total_size += size;
        return (removed, Some(EntryGuard { size }));
    }

    fn remove_guard(&mut self, guard: EntryGuard) {
        self.total_size -= guard.size;
    }

    fn consume_guard(&mut self, guard: EntryGuard, location: &Path) {
        let prev_cache = self.cached_entries.push(
            location.to_string(),
            ObjectMeta {
                location: location.to_owned(),
                last_modified: MIN_DATETIME,
                size: guard.size,
            },
        );
        if let Some(prev_cache) = prev_cache {
            self.total_size -= prev_cache.1.size;
        }
    }
}

struct EntryGuard {
    size: usize,
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use futures::StreamExt;
    use tempfile::tempdir;
    use tokio::sync::Barrier;
    use upstream::local::LocalFileSystem;

    use super::*;

    async fn prepare_cache(max_cache_size: usize) -> CachedStore {
        let local_path = tempdir().unwrap();
        let remote_path = tempdir().unwrap();

        let local_store = Box::new(LocalFileSystem::new_with_prefix(local_path.path()).unwrap());
        let remote_store = Box::new(LocalFileSystem::new_with_prefix(remote_path.path()).unwrap());
        let config = CachedStoreConfig { max_cache_size };

        CachedStore::init(local_store, remote_store, config)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn reserve_trigger_outdate() {
        let store = prepare_cache(4096).await;
        for i in 0..5 {
            let location = Path::from(format!("{}.bin", i));
            store
                .put(&location, Bytes::from_static(&[0; 1024]))
                .await
                .unwrap();
        }
        let result = store.local_store.get(&Path::from("0.bin")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn large_than_capacity() {
        let store = prepare_cache(4096).await;
        let location = Path::from("large.bin");
        let result = store.put(&location, Bytes::from_static(&[0; 10240])).await;
        assert!(result.is_ok());
        let result = store.local_store.get(&location).await;
        assert!(result.is_err());
        assert_eq!(store.state.lock().await.total_size, 0);
    }

    #[test]
    fn reserve_but_not_consumed() {
        let mut state = CacheState::new(40960, Vec::new());
        let mut guards = Vec::with_capacity(10);
        for _ in 0..10 {
            let (removed, guard) = state.reserve(4096);
            assert_eq!(removed.len(), 0);
            guards.push(guard.unwrap());
        }
        assert_eq!(state.total_size, 4096 * 10);
        for guard in guards {
            state.consume_guard(guard, &Path::from("object.bin"));
        }
        assert_eq!(state.total_size, 4096);
    }

    #[tokio::test]
    async fn concurrent_access_cache() {
        let store = Arc::new(prepare_cache(40960).await);
        store
            .put(&Path::from("object.bin"), Bytes::from_static(&[0; 1024]))
            .await
            .unwrap();
        let mut tasks = Vec::with_capacity(10);
        let barrier = Arc::new(Barrier::new(10));
        for _ in 0..10 {
            let c = barrier.clone();
            let s = store.clone();
            tasks.push(tokio::spawn(async move {
                c.wait().await;
                let result = s.get(&Path::from("object.bin")).await.unwrap();
                c.wait().await;
                assert_eq!(result.bytes().await.unwrap().len(), 1024);
            }));
        }
        try_join_all(tasks).await.unwrap();
        assert_eq!(store.state.lock().await.total_size, 1024);
        assert_eq!(store.state.lock().await.cached_entries.len(), 1);
        assert_eq!(store.local_store.list(None).await.unwrap().count().await, 1);
    }

    #[tokio::test]
    async fn init_with_existing_files() {
        let local_path = tempdir().unwrap();
        let remote_path = tempdir().unwrap();

        let local_store = Box::new(LocalFileSystem::new_with_prefix(local_path.path()).unwrap());
        let remote_store = Box::new(LocalFileSystem::new_with_prefix(remote_path.path()).unwrap());
        let config = CachedStoreConfig {
            max_cache_size: 4096,
        };

        for i in 0..5 {
            let location = Path::from(format!("{}.bin", i));
            local_store
                .put(&location, Bytes::from_static(&[0; 1024]))
                .await
                .unwrap();
        }

        let store = CachedStore::init(local_store, remote_store, config)
            .await
            .unwrap();
        assert_eq!(store.state.lock().await.total_size, 4096);
        assert_eq!(store.state.lock().await.cached_entries.len(), 4);
        assert_eq!(store.local_store.list(None).await.unwrap().count().await, 4);
    }
}
