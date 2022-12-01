// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use common_util::config::ReadableSize;
use object_store::cache::CachedStoreConfig;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
/// Options for storage backend
pub struct StorageOptions {
    // 0 means disable mem cache
    pub mem_cache_capacity: ReadableSize,
    pub mem_cache_partition_bits: usize,
    // 0 means disable disk cache
    pub disk_cache_capacity: ReadableSize,
    pub disk_cache_page_size: ReadableSize,
    pub disk_cache_path: String,
    pub object_store: ObjectStoreOptions,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub enum ObjectStoreOptions {
    Local(LocalOptions),
    Aliyun(AliyunOptions),
    Cache(CacheOptions),
}

#[derive(Debug, Clone, Deserialize)]
pub struct LocalOptions {
    pub data_path: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AliyunOptions {
    pub key_id: String,
    pub key_secret: String,
    pub endpoint: String,
    pub bucket: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CacheOptions {
    pub local_store: Box<StorageOptions>,
    pub remote_store: Box<StorageOptions>,
    pub cache_opts: CachedStoreConfig,
}
