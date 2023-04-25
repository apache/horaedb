// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{os::unix::prelude::OsStrExt, str, sync::Arc};

use common_util::define_result;
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, ResultExt, Snafu};
use table_kv::{TableKv, WriteContext};
use upstream::{
    path::{self, Path},
    Error as StoreError,
};
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
}

define_result!(Error);

/// The meta info of Obkv Object
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ObkvObjectMeta<T> {
    /// The full path to the object
    pub location: String,
    /// The last modified time in ms
    pub last_modified: u64,
    /// The size in bytes of the object
    pub size: u64,
    /// The unique identifier for the object; For Obkv, it is composed with
    /// table_name & path
    pub e_tag: Option<String>,
    /// The size in bytes of one part. Note: maybe the size of last part less
    /// than part_size.
    pub part_size: u64,
    /// The paths of multi upload parts.
    pub parts: Vec<String>,
}

#[inline]
pub fn meta_table() -> String {
    String::from("obkv_object_store_meta")
}

impl<T: TableKv> ObkvObjectMeta<T> {
    #[inline]
    pub fn decode(data: &[u8]) -> Result<Self> {
        decode_json(data)
    }

    #[inline]
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_json(self)
    }

    pub async fn save_meta(&self, client: Arc<T>) -> Result<(), std::io::Error> {
        let table_name = meta_table();

        let mut batch = T::WriteBatch::default();
        let json = self.encode().map_err(|source| StoreError::Generic {
            store: "OBKV",
            source: Box::new(source),
        })?;
        batch.insert(self.location.as_ref().as_bytes(), json.as_ref());
        client
            .as_ref()
            .write(WriteContext::default(), &table_name, batch)
            .map_err(|source| StoreError::Generic {
                store: "OBKV",
                source: Box::new(source),
            })?;
        Ok(())
    }

    pub async fn read_meta(
        client: Arc<T>,
        location: &Path,
    ) -> Result<Option<Self>, std::io::Error> {
        let table_name = meta_table();

        let values = client
            .as_ref()
            .get(&table_name, location.as_bytes())
            .map_err(|source| StoreError::Generic {
                store: "OBKV",
                source: Box::new(source),
            })?;

        if let Some(val) = values {
            decode_json(&val);
        }
        Ok(None)
    }

    pub async fn delete_meta(client: Arc<T>, location: &Path) -> Result<i64, std::io::Error> {
        let table_name = meta_table();

        let affect_rows = client
            .as_ref()
            .delete(&table_name, location.as_bytes())
            .map_err(|source| StoreError::Generic {
                store: "OBKV",
                source: Box::new(source),
            })?;
        Ok(affect_rows)
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
