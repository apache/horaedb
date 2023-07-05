// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::future::Future;

use tokio::sync::RwLock;

use crate::error::GenericResult;

#[derive(Debug)]
pub struct Allocator {
    last_id: u64,
    max_id: u64,
    alloc_step: u64,
}

impl Allocator {
    ///New a allocator
    pub fn new(last_id: u64, max_id: u64, alloc_step: u64) -> Self {
        Self {
            last_id,
            max_id,
            alloc_step,
        }
    }

    /// Alloc id
    pub async fn alloc_id<F, T>(&mut self, persist_next_max_id: F) -> GenericResult<u64>
    where
        F: FnOnce(u64) -> T,
        T: Future<Output = GenericResult<()>>,
    {
        if self.last_id < self.max_id {
            self.last_id += 1;
            return Ok(self.last_id);
        }

        // Update new max id.
        let next_max_id = self.last_id + self.alloc_step;

        // persist new max id.
        persist_next_max_id(next_max_id).await?;

        // Update memory.
        self.max_id = next_max_id;

        self.last_id += 1;
        Ok(self.last_id)
    }
}

pub struct IdAllocator {
    inner: RwLock<Allocator>,
}

impl IdAllocator {
    pub fn new(last_id: u64, max_id: u64, alloc_step: u64) -> Self {
        Self {
            inner: RwLock::new(Allocator::new(last_id, max_id, alloc_step)),
        }
    }

    /// Returns the last id
    pub async fn last_id(&self) -> u64 {
        self.inner.read().await.last_id
    }

    /// Set last id
    pub async fn set_last_id(&self, last_id: u64) {
        self.inner.write().await.last_id = last_id
    }

    /// Returns the max id
    pub async fn max_id(&self) -> u64 {
        self.inner.read().await.max_id
    }

    /// Set max id
    pub async fn set_max_id(&self, max_id: u64) {
        self.inner.write().await.max_id = max_id;
    }

    /// Return the alloc step
    pub async fn alloc_step(&self) -> u64 {
        self.inner.read().await.alloc_step
    }

    /// Set alloc step
    pub async fn set_alloc_step(&self, alloc_step: u64) {
        self.inner.write().await.alloc_step = alloc_step;
    }

    /// Alloc id
    pub async fn alloc_id<F, T>(&self, persist_next_max_id: F) -> GenericResult<u64>
    where
        F: FnOnce(u64) -> T,
        T: Future<Output = GenericResult<()>>,
    {
        self.inner.write().await.alloc_id(persist_next_max_id).await
    }
}
