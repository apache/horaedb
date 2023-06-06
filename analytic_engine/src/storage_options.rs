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
    #[serde(default)]
    pub http: HttpOptions,
    #[serde(default)]
    pub retry: RetryOptions,
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
    #[serde(default)]
    pub http: HttpOptions,
    #[serde(default)]
    pub retry: RetryOptions,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HttpOptions {
    pub pool_max_idle_per_host: usize,
    pub timeout: ReadableDuration,
    pub keep_alive_timeout: ReadableDuration,
    pub keep_alive_interval: ReadableDuration,
}

impl Default for HttpOptions {
    fn default() -> Self {
        Self {
            pool_max_idle_per_host: 1024,
            timeout: ReadableDuration::from(Duration::from_secs(60)),
            keep_alive_timeout: ReadableDuration::from(Duration::from_secs(60)),
            keep_alive_interval: ReadableDuration::from(Duration::from_secs(2)),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RetryOptions {
    pub max_retries: usize,
    pub retry_timeout: ReadableDuration,
}

impl Default for RetryOptions {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_timeout: ReadableDuration::from(Duration::from_secs(3 * 60)),
        }
    }
}
