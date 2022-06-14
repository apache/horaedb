// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Cached object store wrapper.

use std::{fmt::Display, ops::Range};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::MIN_DATETIME;
use futures::{lock::Mutex, stream::BoxStream, TryStreamExt};
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

        Ok(Self {
            local_store,
            remote_store,
            state: Mutex::new(CacheState::new(config.max_cache_size, local_list)),
        })
    }

    /// Try putting object to local store.
    async fn put_local(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let required_size = bytes.len();
        let guard = self.state.lock().await.reserve(required_size);
        let result = self.local_store.put(location, bytes.clone()).await;
        if result.is_err() {
            let _ = self.local_store.delete(location).await;
            self.state.lock().await.remove_guard(guard);
        } else {
            self.state.lock().await.consume_guard(guard, location);
        }
        result
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
    fn reserve(&mut self, size: usize) -> EntryGuard {
        if self.total_size + size <= self.max_size {
            self.total_size += size;
            return EntryGuard { size };
        }

        let mut removed = Vec::new();
        while self.total_size + size > self.max_size {
            removed.push(self.cached_entries.pop_lru());
        }
        self.total_size += size;
        return EntryGuard { size };
    }

    fn remove_guard(&mut self, guard: EntryGuard) {
        self.total_size -= guard.size;
    }

    fn consume_guard(&mut self, guard: EntryGuard, location: &Path) {
        self.cached_entries.push(
            location.to_string(),
            ObjectMeta {
                location: location.to_owned(),
                last_modified: MIN_DATETIME,
                size: guard.size,
            },
        );
    }
}

struct EntryGuard {
    size: usize,
}
