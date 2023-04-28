// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{str, sync::Arc, time};

use common_util::{
    define_result,
    error::{BoxError, GenericError},
};
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, ResultExt, Snafu};
use table_kv::{
    KeyBoundary, ScanContext, ScanIter, ScanRequest, TableKv, WriteBatch, WriteContext,
};
use upstream::{path::Path, Error as StoreError, Result as StoreResult};

use super::OBKV;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid utf8 string, err:{}.\nBacktrace:\n{}", source, backtrace))]
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

const META_TABLE: &str = "obkv_object_store_meta";

/// The meta info of Obkv Object
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ObkvObjectMeta {
    /// The full path to the object
    pub location: String,
    /// The last modified time in ms
    pub last_modified: i64,
    /// The size in bytes of the object
    pub size: usize,
    /// The unique identifier for the object; For Obkv, it is composed with
    /// table_name & path
    pub e_tag: Option<String>,
    /// The size in bytes of one part. Note: maybe the size of last part less
    /// than part_size.
    pub part_size: usize,
    /// The paths of multi upload parts.
    pub parts: Vec<String>,
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
    /// The full path to the object
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
        batch.insert(meta.location.to_string().as_bytes(), &json);
        self.client
            .as_ref()
            .write(WriteContext::default(), META_TABLE, batch)
            .box_err()
            .with_context(|| SaveMeta {
                location: meta.location,
            })?;
        Ok(())
    }

    pub async fn read_meta(&self, location: &Path) -> Result<Option<ObkvObjectMeta>> {
        let values = self
            .client
            .as_ref()
            .get(META_TABLE, location.as_ref().as_bytes())
            .box_err()
            .context(ReadMeta {
                location: location.as_ref().to_string(),
            })?;

        if let Some(val) = values {
            let meta = decode_json(&val)?;
            Ok(meta)
        } else {
            Ok(None)
        }
    }

    pub async fn delete_meta(&self, meta: ObkvObjectMeta, location: &Path) -> Result<i64> {
        let affect_rows = self
            .client
            .as_ref()
            .delete(META_TABLE, location.as_ref().as_bytes())
            .box_err()
            .context(DeleteMeta {
                location: meta.location,
            })?;

        Ok(affect_rows)
    }

    pub async fn list_meta(
        &self,
        prefix: &Path,
    ) -> StoreResult<Vec<ObkvObjectMeta>, std::io::Error> {
        let scan_context: ScanContext = ScanContext {
            timeout: time::Duration::from_secs(10),
            batch_size: 1000,
        };

        let path = &prefix.as_ref().to_string();
        let key_buffer = path.as_bytes();
        let mut start_key = Vec::with_capacity(key_buffer.len() + 1);
        let mut end_key = Vec::with_capacity(key_buffer.len() + 1);
        start_key.extend(key_buffer);
        start_key.push(0x00);
        end_key.extend(key_buffer);
        end_key.push(0xff);
        let start = KeyBoundary::included(start_key.as_ref());
        let end = KeyBoundary::excluded(end_key.as_ref());
        let request = ScanRequest {
            start,
            end,
            reverse: false,
        };

        let mut iter = self
            .client
            .scan(scan_context, META_TABLE, request)
            .map_err(|source| StoreError::Generic {
                store: OBKV,
                source: Box::new(source),
            })?;

        let mut metas = vec![];
        while iter.next().unwrap() {
            let value = iter.value();
            let meta = ObkvObjectMeta::decode(value).map_err(|source| StoreError::Generic {
                store: OBKV,
                source: Box::new(source),
            })?;
            metas.push(meta);
        }
        Ok(metas)
    }
}

fn decode_json<'a, T: serde::Deserialize<'a>>(data: &'a [u8]) -> Result<T> {
    let json = str::from_utf8(data).context(InvalidUtf8)?;
    serde_json::from_str(json).context(InvalidJson { json })
}

fn encode_json<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    let json = serde_json::to_string(value).context(EncodeJson)?;
    Ok(json.into_bytes())
}
