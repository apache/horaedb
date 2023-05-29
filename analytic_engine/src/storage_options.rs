// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use common_util::config::{ReadableDuration, ReadableSize};
use serde::{Deserialize, Serialize};
use table_kv::config::ObkvConfig;

#[derive(Debug, Clone, Deserialize, Serialize)]
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
    pub disk_cache_dir: String,
    pub object_store: ObjectStoreOptions,
}

impl Default for StorageOptions {
    fn default() -> Self {
        let root_path = "/tmp/ceresdb".to_string();

        StorageOptions {
            mem_cache_capacity: ReadableSize::mb(512),
            mem_cache_partition_bits: 6,
            disk_cache_dir: root_path.clone(),
            disk_cache_capacity: ReadableSize::gb(0),
            disk_cache_page_size: ReadableSize::mb(2),
            object_store: ObjectStoreOptions::Local(LocalOptions {
                data_dir: root_path,
            }),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[allow(clippy::large_enum_variant)]
pub enum ObjectStoreOptions {
    Local(LocalOptions),
    Aliyun(AliyunOptions),
    Obkv(ObkvOptions),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LocalOptions {
    pub data_dir: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
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
    #[serde(default = "AliyunOptions::default_keep_alive_time")]
    pub keep_alive_timeout: ReadableDuration,
    #[serde(default = "AliyunOptions::default_keep_alive_inverval")]
    pub keep_alive_interval: ReadableDuration,
}

impl AliyunOptions {
    fn default_pool_max_idle_per_host() -> usize {
        1024
    }

    fn default_timeout() -> ReadableDuration {
        ReadableDuration::from(Duration::from_secs(60))
    }

    fn default_keep_alive_time() -> ReadableDuration {
        ReadableDuration::from(Duration::from_secs(60))
    }

    fn default_keep_alive_inverval() -> ReadableDuration {
        ReadableDuration::from(Duration::from_secs(2))
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ObkvOptions {
    pub prefix: String,
    pub shard_num: usize,
    pub part_size: usize,
    /// Obkv client config
    pub client: ObkvConfig,
}
