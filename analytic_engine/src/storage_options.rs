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
    S3(S3Options),
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
    pub http_options: HttpOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObkvOptions {
    pub prefix: String,
    #[serde(default = "ObkvOptions::default_shard_num")]
    pub shard_num: usize,
    #[serde(default = "ObkvOptions::default_part_size")]
    pub part_size: ReadableSize,
    #[serde(default = "ObkvOptions::default_max_object_size")]
    pub max_object_size: ReadableSize,
    #[serde(default = "ObkvOptions::default_upload_parallelism")]
    pub upload_parallelism: usize,
    /// Obkv client config
    pub client: ObkvConfig,
}

impl ObkvOptions {
    fn default_max_object_size() -> ReadableSize {
        ReadableSize::gb(1)
    }

    fn default_part_size() -> ReadableSize {
        ReadableSize::mb(1)
    }

    fn default_shard_num() -> usize {
        512
    }

    fn default_upload_parallelism() -> usize {
        8
    }
}
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct S3Options {
    pub region: String,
    pub key_id: String,
    pub key_secret: String,
    pub endpoint: String,
    pub bucket: String,
    pub prefix: String,
    pub http_options: HttpOptions,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HttpOptions {
    #[serde(default = "HttpOptions::default_pool_max_idle_per_host")]
    pub pool_max_idle_per_host: usize,
    #[serde(default = "HttpOptions::default_timeout")]
    pub timeout: ReadableDuration,
    #[serde(default = "HttpOptions::default_keep_alive_time")]
    pub keep_alive_timeout: ReadableDuration,
    #[serde(default = "HttpOptions::default_keep_alive_inverval")]
    pub keep_alive_interval: ReadableDuration,
}

impl HttpOptions {
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
