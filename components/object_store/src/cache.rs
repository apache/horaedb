// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! This mod provides an [ObjectStore] implementor [CachedStore], which is made
//! up of:
//!
//! - Local Store: a local file system cache
//! - Remote Store: a remote OSS service
//! - LRU Manager: a cache eviction policy (LRU) to keep the local store size
//!   under control.
//!
//! ## Read
//! On serving a read request, [CachedStore] will first check if the object
//! is in the local store. If not, it will fetch it from the remote store and
//! store it in the local store. To simplify the implementation, all the read
//! sources returned are based on `LocalStore`. Ascii art below:
//!
//! Workflow
//!
//! ```no_rust,ignore
//! (read)
//!        Serve I/O requests
//!               ▲ │
//!             4 │ │1
//!               │ │
//!    ┌──────────┴─▼─────────┐
//!    │                      │
//!    │  Object Store Cache  │
//!    │                      │
//!    └─┬─▲────────────┬─▲───┘
//!   2.1│ │3.1      2.2│ │3.2
//!      │ │            │ │
//!  ┌───▼─┴──┐      ┌──▼─┴─────┐
//!  │Local FS│      │Remote OSS│
//!  └────────┘      └──────────┘
//! ```
//!
//! ## Write
//! For write requests, we will write the content to both underlying stores.
//!
//! ## Restart
//! To suit some deploy scenarios that aren't stateless, [ObjectStore] will try
//! to load all existing entries from `LocalStore` to `LRU Manager`.
//!
//! ## Purge
//! Both read and write operations may trigger purge on `LocalStore`. The purge
//! policy is defined by the `LRU Manager`. It will get a delete list from `LRU
//! Manager` and delete the list from `LocalStore`.
//!
//! To ensure the total size of `LocalStore` is always less than the threshold,
//! [CachedStore] will first purge enough space for the incoming new objects.

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
        let result = state.reserve(0);
        if !result.removed_path.is_empty() {
            Self::remove_paths(local_store.as_ref(), &result.removed_path).await?;
        }

        Ok(Self {
            local_store,
            remote_store,
            state: Mutex::new(state),
        })
    }

    /// Try putting object to local store. If local store cannot make enough
    /// space for the object, this function will skip putting it and return Ok.
    async fn try_put_local(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let required_size = bytes.len();

        let guard = {
            // TODO: check if this lock will block for a long time. If so we may need to
            // delete paths asynchronously.
            let mut state = self.state.lock().await;
            let result = state.reserve(required_size);
            Self::remove_paths(self.local_store.as_ref(), &result.removed_path).await?;
            result
        };

        // cannot reserve enough space, skip to put local store
        if !guard.is_success() {
            return Ok(());
        }

        let result = self.local_store.put(location, bytes.clone()).await;
        if result.is_err() {
            let _ = self.local_store.delete(location).await;
            self.state.lock().await.cancel_reserve(guard);
        } else {
            self.state.lock().await.confirm_reserve(guard, location);
        }
        result
    }

    async fn remove_paths(store: &dyn ObjectStore, paths: &[Path]) -> Result<()> {
        let tasks = paths
            .iter()
            .map(|path| store.delete(path))
            .collect::<Vec<_>>();
        try_join_all(tasks).await?;
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for CachedStore {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let _ = self.try_put_local(location, bytes.clone()).await;

        self.remote_store.put(location, bytes).await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        if self.state.lock().await.contains(location) {
            self.local_store.get(location).await
        } else {
            let remote_obj = self.remote_store.get(location).await?;
            let bytes = remote_obj.bytes().await?;
            let _ = self.try_put_local(location, bytes).await?;
            self.local_store.get(location).await
        }
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        if self.state.lock().await.contains(location) {
            self.local_store.get_range(location, range).await
        } else {
            let remote_obj = self.remote_store.get(location).await?;
            let bytes = remote_obj.bytes().await?;
            let _ = self.try_put_local(location, bytes).await?;
            self.local_store.get_range(location, range).await
        }
    }

    // TODO: consider whether we need to cache this request.
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
    fn new(max_size: usize, metas: Vec<ObjectMeta>) -> Self {
        let total_size = metas.iter().map(|m| m.size).sum();
        let mut cached_entries = LruCache::unbounded();
        for m in metas {
            cached_entries.put(m.location.to_string(), m);
        }

        Self {
            max_size,
            total_size,
            cached_entries,
        }
    }

    /// Try to remove an entry. Returns whether the entry was removed.
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

    /// Reserve space for a new entry. Returns paths that need to be removed and
    /// a optional [ReserveResult] that stands for the space. It needs to be
    /// consumed by either [remove_guard] (cancel this reserve operation) or
    /// [consume_guard] (confirm this reserve operation). If the [ReserveResult]
    /// is None means this reserve operation is failed and local store should
    /// not be written.
    #[must_use]
    fn reserve(&mut self, size: usize) -> ReserveResult {
        if self.total_size + size <= self.max_size {
            self.total_size += size;
            return ReserveResult {
                removed_path: Vec::new(),
                size: Some(size),
            };
        }

        let mut removed = Vec::new();
        while self.total_size + size > self.max_size {
            // try to pop a cached entry.
            let popped = self.cached_entries.pop_lru();
            if let Some((_, meta)) = popped {
                self.total_size -= meta.size;
                removed.push(meta.location);
            } else {
                return ReserveResult {
                    removed_path: removed,
                    // No cached entries left to remove but still haven't enough space. Return
                    // `None` to indicate that this reserve operation is failed.
                    size: None,
                };
            }
        }
        self.total_size += size;
        ReserveResult {
            removed_path: removed,
            size: Some(size),
        }
    }

    fn cancel_reserve(&mut self, guard: ReserveResult) {
        if let Some(size) = guard.size {
            self.total_size -= size;
        }
    }

    fn confirm_reserve(&mut self, guard: ReserveResult, location: &Path) {
        if guard.size.is_none() {
            return;
        }
        let size = guard.size.unwrap();
        let prev_cache = self.cached_entries.push(
            location.to_string(),
            ObjectMeta {
                location: location.to_owned(),
                last_modified: MIN_DATETIME,
                size,
            },
        );
        if let Some((_, meta)) = prev_cache {
            self.total_size -= meta.size;
        }
    }
}

struct ReserveResult {
    /// Paths that need to be removed.
    removed_path: Vec<Path>,
    /// The size of the reserved space. If this field is None means this reserve
    /// operation is failed.
    size: Option<usize>,
}

impl ReserveResult {
    fn is_success(&self) -> bool {
        self.size.is_some()
    }
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
            let guard = state.reserve(4096);
            assert_eq!(guard.removed_path.len(), 0);
            guards.push(guard);
        }
        assert_eq!(state.total_size, 4096 * 10);
        for guard in guards {
            state.confirm_reserve(guard, &Path::from("object.bin"));
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
