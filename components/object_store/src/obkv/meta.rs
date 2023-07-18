// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{ops::Range, str, sync::Arc, time};

use generic_error::{BoxError, GenericError};
use macros::define_result;
use serde::{Deserialize, Serialize};
use snafu::{ensure, Backtrace, ResultExt, Snafu};
use table_kv::{ScanContext, ScanIter, TableKv, WriteBatch, WriteContext};
use upstream::{path::Path, Error as StoreError, Result as StoreResult};

use crate::obkv::{util, OBKV};

pub const HEADER: u8 = 0x00_u8;

pub const SCAN_TIMEOUT_SECS: u64 = 10;

pub const SCAN_BATCH_SIZE: i32 = 1000;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid utf8 string, err:{source}.\nBacktrace:\n{backtrace}"))]
    InvalidUtf8 {
        source: std::str::Utf8Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid json, err:{source}, json:{json}.\nBacktrace:\n{backtrace}"))]
    InvalidJson {
        json: String,
        source: serde_json::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to encode json, err:{source}.\nBacktrace:\n{backtrace}"))]
    EncodeJson {
        source: serde_json::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to save meta, location:{location}, err:{source}"))]
    SaveMeta {
        location: String,
        source: GenericError,
    },

    #[snafu(display("Failed to delete meta, location:{location}, err:{source}"))]
    DeleteMeta {
        location: String,
        source: GenericError,
    },

    #[snafu(display("Failed to read meta, location:{location}, err:{source}"))]
    ReadMeta {
        location: String,
        source: GenericError,
    },

    #[snafu(display(
        "Invalid header found, header:{header}, expect:{expect}.\nBacktrace:\n{backtrace}"
    ))]
    InvalidHeader {
        header: u8,
        expect: u8,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Out of range occurs, end:{end}, object_size:{object_size}.\nBacktrace:\n{backtrace}"
    ))]
    OutOfRange {
        end: usize,
        object_size: usize,
        backtrace: Backtrace,
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
    /// The version of object, Now we use the upload_id as version.
    #[serde(rename = "version")]
    pub version: String,
}

impl ObkvObjectMeta {
    #[inline]
    pub fn decode(data: &[u8]) -> Result<Self> {
        ensure!(
            data[0] == HEADER,
            InvalidHeader {
                header: data[0],
                expect: HEADER,
            }
        );
        let json = str::from_utf8(&data[1..]).context(InvalidUtf8)?;
        serde_json::from_str(json).context(InvalidJson { json })
    }

    #[inline]
    pub fn encode(&self) -> Result<Vec<u8>> {
        let size = self.estimate_size_of_json();
        let mut encode_bytes = Vec::with_capacity(size + 1);
        encode_bytes.push(HEADER);
        serde_json::to_writer(&mut encode_bytes, self).context(EncodeJson)?;
        Ok(encode_bytes)
    }

    /// Estimate the json string size of ObkvObjectMeta
    #[inline]
    pub fn estimate_size_of_json(&self) -> usize {
        // {}
        let mut size = 2;
        // size of key name, `,`, `""` and `:`
        size += (8 + 13 + 4 + 9 + 9 + 5 + 7) + 4 * 7;
        size += self.location.len() + 2;
        // last_modified
        size += 8;
        // size
        size += 8;
        // unique_id
        if let Some(id) = &self.unique_id {
            size += id.len() + 2;
        } else {
            size += 4;
        }
        // part_size
        size += 8;
        // parts
        for part in &self.parts {
            // part.len, `""`, `:`, and `,`
            size += part.len() + 4;
        }
        //{}
        size += 2;
        // version
        size += self.version.len();
        size
    }

    /// Compute the convered parts based on given range parameter
    pub fn compute_covered_parts(&self, range: Range<usize>) -> Result<ConveredParts> {
        ensure!(
            range.end <= self.size,
            OutOfRange {
                end: range.end,
                object_size: self.size,
            }
        );
        let batch_size = self.part_size;
        let start_index = range.start / batch_size;
        let start_offset = range.start % batch_size;
        let end_index = range.end / batch_size;
        let end_offset = range.end % batch_size;

        Ok(ConveredParts {
            part_keys: &self.parts[start_index..=end_index],
            start_offset,
            end_offset,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ConveredParts<'a> {
    /// The table kv client
    pub part_keys: &'a [String],
    pub start_offset: usize,
    pub end_offset: usize,
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
    pub async fn save(&self, meta: ObkvObjectMeta) -> Result<()> {
        let mut batch = T::WriteBatch::default();
        let encode_bytes = meta.encode()?;
        batch.insert(meta.location.as_bytes(), &encode_bytes);
        self.client
            .as_ref()
            .write(WriteContext::default(), OBJECT_STORE_META, batch)
            .box_err()
            .with_context(|| SaveMeta {
                location: meta.location,
            })?;
        Ok(())
    }

    pub async fn read(&self, location: &Path) -> Result<Option<ObkvObjectMeta>> {
        let value = self
            .client
            .as_ref()
            .get(OBJECT_STORE_META, location.as_ref().as_bytes())
            .box_err()
            .context(ReadMeta {
                location: location.as_ref().to_string(),
            })?;

        value.map(|v| ObkvObjectMeta::decode(&v)).transpose()
    }

    pub async fn delete(&self, meta: ObkvObjectMeta, location: &Path) -> Result<()> {
        self.client
            .as_ref()
            .delete(OBJECT_STORE_META, location.as_ref().as_bytes())
            .box_err()
            .context(DeleteMeta {
                location: meta.location,
            })?;

        Ok(())
    }

    pub async fn delete_with_version(&self, location: &Path, version: &str) -> Result<()> {
        let meta_result = self.read(location).await?;
        if let Some(meta) = meta_result {
            if meta.version == version {
                self.delete(meta, location).await?;
            }
        }
        Ok(())
    }

    pub async fn list(&self, prefix: &Path) -> StoreResult<Vec<ObkvObjectMeta>, std::io::Error> {
        let scan_context: ScanContext = ScanContext {
            timeout: time::Duration::from_secs(SCAN_TIMEOUT_SECS),
            batch_size: SCAN_BATCH_SIZE,
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

#[cfg(test)]
mod test {

    use std::ops::Range;

    use crate::obkv::meta::ObkvObjectMeta;

    #[test]
    fn test_estimate_size() {
        let meta = build_test_meta();

        let expect = meta.estimate_size_of_json();
        let json = &serde_json::to_string(&meta).unwrap();
        let real = json.len();
        println!("expect:{expect},real:{real}");
        assert!(expect.abs_diff(real) as f32 / (real as f32) < 0.1);
    }

    #[test]
    fn test_compute_convered_parts() {
        let meta = build_test_meta();

        let range1 = Range { start: 0, end: 1 };
        let expect = meta.compute_covered_parts(range1).unwrap();
        assert!(expect.part_keys.len() == 1);
        assert!(expect.start_offset == 0);
        assert!(expect.end_offset == 1);

        let range1 = Range {
            start: 0,
            end: 8190,
        };
        let expect = meta.compute_covered_parts(range1).unwrap();
        assert!(expect.part_keys.len() == 8);
        assert!(expect.start_offset == 0);
        assert!(expect.end_offset == 1022);

        let range1 = Range {
            start: 1023,
            end: 1025,
        };
        let expect = meta.compute_covered_parts(range1).unwrap();
        assert!(expect.part_keys.len() == 2);
        assert!(expect.start_offset == 1023);
        assert!(expect.end_offset == 1);

        let range1 = Range {
            start: 8189,
            end: 8190,
        };
        let expect = meta.compute_covered_parts(range1).unwrap();
        assert!(expect.part_keys.len() == 1);
        assert!(expect.start_offset == 1021);
        assert!(expect.end_offset == 1022);

        let range1 = Range {
            start: 8189,
            end: 8199,
        };
        let expect = meta.compute_covered_parts(range1);
        assert!(expect.is_err());
    }

    fn build_test_meta() -> ObkvObjectMeta {
        ObkvObjectMeta {
            location: String::from("/test/xxxxxxxxxxxxxxxxxxxxxxxxxxxxxfdsfjlajflk"),
            last_modified: 123456789,
            size: 8190,
            unique_id: Some(String::from("1245689u438uferjalfjkda")),
            part_size: 1024,
            parts: vec![
                String::from("/test/xx/0"),
                String::from("/test/xx/1"),
                String::from("/test/xx/4"),
                String::from("/test/xx/5"),
                String::from("/test/xx/0"),
                String::from("/test/xx/1"),
                String::from("/test/xx/4"),
                String::from("/test/xx/5"),
            ],
            version: String::from("123456fsdalfkassa;l;kjfaklasadffsd"),
        }
    }
}
