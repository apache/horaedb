use serde::Deserialize;

/// Options for storage backend
#[derive(Debug, Clone, Deserialize)]
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
