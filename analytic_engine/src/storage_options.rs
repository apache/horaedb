// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use common_util::config::{ReadableDuration, ReadableSize};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
/// Options for storage backend
pub struct StorageOptions {
    // 0 means disable mem cache
    pub mem_cache_capacity: ReadableSize,
    pub mem_cache_partition_bits: usize,
    // 0 means disable disk cache
    // Note: disk_cache_capacity % disk_cache_page_size should be 0
    pub disk_cache_capacity: ReadableSize,
    pub disk_cache_page_size: ReadableSize,
    pub disk_cache_path: String,
    pub object_store: ObjectStoreOptions,
}

impl Default for StorageOptions {
    fn default() -> Self {
        let root_path = "/tmp/ceresdb".to_string();

        StorageOptions {
            mem_cache_capacity: ReadableSize::mb(512),
            mem_cache_partition_bits: 6,
            disk_cache_path: root_path.clone(),
            disk_cache_capacity: ReadableSize::gb(0),
            disk_cache_page_size: ReadableSize::mb(2),
            object_store: ObjectStoreOptions::Local(LocalOptions { dir: root_path }),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub enum ObjectStoreOptions {
    Local(LocalOptions),
    Aliyun(AliyunOptions),
}

#[derive(Debug, Clone, Deserialize)]
pub struct LocalOptions {
    pub dir: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AliyunOptions {
    pub key_id: String,
    pub key_secret: String,
    pub endpoint: String,
    pub bucket: String,
    pub prefix: String,
    #[serde(default = "AliyunOptions::default_pool_max_idle_per_host")]
    pub pool_max_idle_per_host: usize,
    #[serde(default = "AliyunOptions::default_timeout")]
    pub timeout: ReadableDuration,
}

impl AliyunOptions {
    fn default_pool_max_idle_per_host() -> usize {
        1024
    }

    fn default_timeout() -> ReadableDuration {
        ReadableDuration::from(Duration::from_secs(60))
    }
}
