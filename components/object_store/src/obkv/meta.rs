// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{str, sync::Arc, time};

use common_util::{
    define_result,
    error::{BoxError, GenericError},
};
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, ResultExt, Snafu};
use table_kv::{ScanContext, ScanIter, TableKv, WriteBatch, WriteContext};
use upstream::{path::Path, Error as StoreError, Result as StoreResult};

use super::{util, OBKV};

pub const HEADER: u8 = 0x00_u8;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid utf8 string, err:{source}.\nBacktrace:\n{backtrace}"))]
    InvalidUtf8 {
        source: std::str::Utf8Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Invalid json, err:{}, json:{}.\nBacktrace:\n{}",
        source,
        json,
        backtrace
    ))]
    InvalidJson {
        json: String,
        source: serde_json::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to encode json, err:{}.\nBacktrace:\n{}", source, backtrace))]
    EncodeJson {
        source: serde_json::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to save meta, location:{}, err:{}", location, source))]
    SaveMeta {
        location: String,
        source: GenericError,
    },

    #[snafu(display("Failed to delete meta, location:{}, err:{}", location, source))]
    DeleteMeta {
        location: String,
        source: GenericError,
    },

    #[snafu(display("Failed to read meta, location:{}, err:{}", location, source))]
    ReadMeta {
        location: String,
        source: GenericError,
    },
}

define_result!(Error);

pub const OBJECT_STORE_META: &str = "obkv_object_store_meta";

/// The meta info of Obkv Object
///
/// **WARN: Do not change the field name, may lead to breaking changes!**
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ObkvObjectMeta {
    /// The full path to the object
    #[serde(rename = "location")]
    pub location: String,
    /// The last modified time in ms
    #[serde(rename = "last_modified")]
    pub last_modified: i64,
    /// The size in bytes of the object
    #[serde(rename = "size")]
    pub size: usize,
    /// The unique identifier for the object; For Obkv, it is composed with
    /// table_name @ path @ upload_id
    #[serde(rename = "unique_id")]
    pub unique_id: Option<String>,
    /// The size in bytes of one part. Note: maybe the size of last part less
    /// than part_size.
    #[serde(rename = "part_size")]
    pub part_size: usize,
    /// The paths of multi upload parts.
    #[serde(rename = "parts")]
    pub parts: Vec<String>,
    /// The version of object, we use the upload_id as version.
    /// TODO: Since `upload_id` is used by multiple objects, it may become very
    /// large. Should we assign a version number to each object to avoid
    /// this issue?
    #[serde(rename = "version")]
    pub version: String,
}

impl ObkvObjectMeta {
    #[inline]
    pub fn decode(data: &[u8]) -> Result<Self> {
        decode_json(data)
    }

    #[inline]
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_json(self)
    }
}

#[derive(Debug, Clone)]
pub struct MetaManager<T> {
    /// The table kv client
    pub client: Arc<T>,
}

impl<T: TableKv> std::fmt::Display for MetaManager<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ObjectStore-Obkv-MetaManager({:?})", self.client)?;
        Ok(())
    }
}

impl<T: TableKv> MetaManager<T> {
    pub async fn save_meta(&self, meta: ObkvObjectMeta) -> Result<()> {
        let mut batch = T::WriteBatch::default();
        let json = meta.encode()?;
        batch.insert(meta.location.as_bytes(), &json);
        self.client
            .as_ref()
            .write(WriteContext::default(), OBJECT_STORE_META, batch)
            .box_err()
            .with_context(|| SaveMeta {
                location: meta.location,
            })?;
        Ok(())
    }

    pub async fn read_meta(&self, location: &Path) -> Result<Option<ObkvObjectMeta>> {
        let value = self
            .client
            .as_ref()
            .get(OBJECT_STORE_META, location.as_ref().as_bytes())
            .box_err()
            .context(ReadMeta {
                location: location.as_ref().to_string(),
            })?;

        value.map(|v| decode_json(&v)).transpose()
    }

    pub async fn delete_meta(&self, meta: ObkvObjectMeta, location: &Path) -> Result<i64> {
        let affect_rows = self
            .client
            .as_ref()
            .delete(OBJECT_STORE_META, location.as_ref().as_bytes())
            .box_err()
            .context(DeleteMeta {
                location: meta.location,
            })?;

        Ok(affect_rows)
    }

    pub async fn delete_meta_with_version(&self, location: &Path, version: &str) -> Result<i64> {
        let meta_result = self.read_meta(location).await?;
        if let Some(meta) = meta_result {
            if meta.version == version {
                self.delete_meta(meta, location).await?;
            }
        }
        Ok(0)
    }

    pub async fn list_meta(
        &self,
        prefix: &Path,
    ) -> StoreResult<Vec<ObkvObjectMeta>, std::io::Error> {
        let scan_context: ScanContext = ScanContext {
            timeout: time::Duration::from_secs(10),
            batch_size: 1000,
        };

        let scan_request = util::scan_request_with_prefix(prefix.as_ref().as_bytes());

        let mut iter = self
            .client
            .scan(scan_context, OBJECT_STORE_META, scan_request)
            .map_err(|source| StoreError::Generic {
                store: OBKV,
                source: Box::new(source),
            })?;

        let mut metas = vec![];
        while iter.valid() {
            let value = iter.value();
            let meta = ObkvObjectMeta::decode(value).map_err(|source| StoreError::Generic {
                store: OBKV,
                source: Box::new(source),
            })?;
            metas.push(meta);
            iter.next().map_err(|source| StoreError::Generic {
                store: OBKV,
                source: Box::new(source),
            })?;
        }
        Ok(metas)
    }
}

fn decode_json<'a, T: serde::Deserialize<'a>>(data: &'a [u8]) -> Result<T> {
    assert_eq!(data[0], HEADER);
    let json = str::from_utf8(&data[1..]).context(InvalidUtf8)?;
    serde_json::from_str(json).context(InvalidJson { json })
}

fn encode_json<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    let json = serde_json::to_string(value).context(EncodeJson)?;
    let bytes = json.into_bytes();
    let mut key_buffer = Vec::with_capacity(bytes.len() + 1);
    key_buffer.push(HEADER);
    key_buffer.extend(bytes);
    Ok(key_buffer)
}
