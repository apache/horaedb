// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::Debug,
    sync::{Arc, RwLock},
};

use arrow_deps::parquet::file::metadata::ParquetMetaData;
use lru::LruCache;

pub trait MetaCache: Debug {
    fn get(&self, key: &str) -> Option<Arc<ParquetMetaData>>;

    fn put(&self, key: String, value: Arc<ParquetMetaData>);
}

pub trait DataCache: Debug {
    fn get(&self, key: &str) -> Option<Arc<Vec<u8>>>;

    fn put(&self, key: String, value: Arc<Vec<u8>>);
}

#[derive(Debug)]
pub struct LruMetaCache {
    cache: RwLock<LruCache<String, Arc<ParquetMetaData>>>,
}

impl LruMetaCache {
    pub fn new(cap: usize) -> Self {
        Self {
            cache: RwLock::new(LruCache::new(cap)),
        }
    }
}

impl MetaCache for LruMetaCache {
    fn get(&self, key: &str) -> Option<Arc<ParquetMetaData>> {
        self.cache.write().unwrap().get(key).cloned()
    }

    fn put(&self, key: String, value: Arc<ParquetMetaData>) {
        self.cache.write().unwrap().put(key, value);
    }
}

#[derive(Debug)]
pub struct LruDataCache {
    cache: RwLock<LruCache<String, Arc<Vec<u8>>>>,
}

impl LruDataCache {
    pub fn new(cap: usize) -> Self {
        Self {
            cache: RwLock::new(LruCache::new(cap)),
        }
    }
}

impl DataCache for LruDataCache {
    fn get(&self, key: &str) -> Option<Arc<Vec<u8>>> {
        self.cache.write().unwrap().get(key).cloned()
    }

    fn put(&self, key: String, value: Arc<Vec<u8>>) {
        self.cache.write().unwrap().put(key, value);
    }
}
