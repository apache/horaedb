// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use serde::Deserialize;

/// Options for storage backend
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub enum StorageOptions {
    Local(LocalOptions),
    Aliyun(AliyunOptions),
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
