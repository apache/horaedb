// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::Debug,
    sync::{Arc, RwLock},
};

use lru::LruCache;
use parquet_ext::ParquetMetaDataRef;

use crate::sst::file::SstMetaDataRef;

#[derive(Debug, Clone)]
pub struct MetaData {
    pub parquet: ParquetMetaDataRef,
    pub custom: SstMetaDataRef,
}

#[derive(Debug)]
pub struct MetaCache {
    cache: RwLock<LruCache<String, MetaData>>,
}

impl MetaCache {
    pub fn new(cap: usize) -> Self {
        Self {
            cache: RwLock::new(LruCache::new(cap)),
        }
    }

    pub fn get(&self, key: &str) -> Option<MetaData> {
        self.cache.write().unwrap().get(key).cloned()
    }

    pub fn put(&self, key: String, value: MetaData) {
        self.cache.write().unwrap().put(key, value);
    }
}

pub type MetaCacheRef = Arc<MetaCache>;
