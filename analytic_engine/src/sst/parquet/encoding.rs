// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::convert::TryFrom;

use arrow::{compute, record_batch::RecordBatch as ArrowRecordBatch};
use async_trait::async_trait;
use bytes::Bytes;
use bytes_ext::{BytesMut, SafeBufMut};
use ceresdbproto::sst as sst_pb;
use common_types::schema::{ArrowSchemaRef, Schema};
use generic_error::{BoxError, GenericError};
use macros::define_result;
use parquet::{
    arrow::AsyncArrowWriter,
    basic::Compression,
    file::{metadata::KeyValue, properties::WriterProperties},
};
use prost::{bytes, Message};
use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};
use tokio::io::AsyncWrite;

use crate::sst::parquet::meta_data::ParquetMetaData;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to encode sst meta data, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    EncodeIntoPb {
        source: prost::EncodeError,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to decode sst meta data, base64 of meta value:{}, err:{}.\nBacktrace:\n{}",
        meta_value,
        source,
        backtrace,
    ))]
    DecodeFromPb {
        meta_value: String,
        source: prost::DecodeError,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to decode sst meta data, bytes:{:?}, err:{}.\nBacktrace:\n{}",
        bytes,
        source,
        backtrace,
    ))]
    DecodeFromBytes {
        bytes: Vec<u8>,
        source: prost::DecodeError,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Invalid meta key, expect:{}, given:{}.\nBacktrace:\n{}",
        expect,
        given,
        backtrace
    ))]
    InvalidMetaKey {
        expect: String,
        given: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Base64 meta value not found.\nBacktrace:\n{}", backtrace))]
    Base64MetaValueNotFound { backtrace: Backtrace },

    #[snafu(display(
        "Invalid base64 meta value length, base64 of meta value:{}.\nBacktrace:\n{}",
        meta_value,
        backtrace,
    ))]
    InvalidBase64MetaValueLen {
        meta_value: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to decode base64 meta value, base64 of meta value:{}, err:{}",
        meta_value,
        source
    ))]
    DecodeBase64MetaValue {
        meta_value: String,
        source: base64::DecodeError,
    },

    #[snafu(display(
        "Invalid meta value length, base64 of meta value:{}.\nBacktrace:\n{}",
        meta_value,
        backtrace
    ))]
    InvalidMetaValueLen {
        meta_value: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Invalid meta value header, base64 of meta value:{}.\nBacktrace:\n{}",
        meta_value,
        backtrace
    ))]
    InvalidMetaValueHeader {
        meta_value: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Invalid meta value header, bytes:{:?}.\nBacktrace:\n{}",
        bytes,
        backtrace
    ))]
    InvalidMetaBytesHeader {
        bytes: Vec<u8>,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to convert sst meta data from protobuf, err:{}", source))]
    ConvertSstMetaData {
        source: crate::sst::parquet::meta_data::Error,
    },

    #[snafu(display(
        "Failed to encode record batch into sst, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    EncodeRecordBatch {
        source: GenericError,
        backtrace: Backtrace,
    },
}

define_result!(Error);

// In v1 format, our customized meta is encoded in parquet itself, this may
// incur storage overhead since parquet KV only accept string, so we need to
// base64 our meta.
// In v2, we save meta in another independent file on object_store, its path is
// encoded in parquet KV, which is identified by `meta_path`.
pub const META_VERSION_V1: &str = "1";
pub const META_VERSION_CURRENT: &str = "2";
pub const META_KEY: &str = "meta"; // used in v1
pub const META_PATH_KEY: &str = "meta_path"; // used in v2
pub const META_VERSION_KEY: &str = "meta_version";
pub const META_VALUE_HEADER: u8 = 0;

/// Encode the sst custom meta data into binary key value pair.
pub fn encode_sst_meta_data(meta_data: ParquetMetaData) -> Result<Bytes> {
    let meta_data_pb = sst_pb::ParquetMetaData::from(meta_data);

    let mut buf = BytesMut::with_capacity(meta_data_pb.encoded_len() + 1);
    buf.try_put_u8(META_VALUE_HEADER)
        .expect("Should write header into the buffer successfully");

    // encode the sst custom meta data into protobuf binary
    meta_data_pb.encode(&mut buf).context(EncodeIntoPb)?;
    Ok(buf.into())
}

/// Decode the sst custom meta data from the binary key value pair.
pub fn decode_sst_meta_data_from_bytes(bytes: &[u8]) -> Result<ParquetMetaData> {
    ensure!(
        bytes[0] == META_VALUE_HEADER,
        InvalidMetaBytesHeader {
            bytes: bytes.to_vec()
        }
    );
    let meta_data_pb: sst_pb::ParquetMetaData =
        Message::decode(&bytes[1..]).context(DecodeFromBytes {
            bytes: bytes.to_vec(),
        })?;

    ParquetMetaData::try_from(meta_data_pb).context(ConvertSstMetaData)
}

/// Decode the sst meta data from the binary key value pair.
/// Used in v1 format.
pub fn decode_sst_meta_data_from_kv(kv: &KeyValue) -> Result<ParquetMetaData> {
    ensure!(
        kv.key == META_KEY,
        InvalidMetaKey {
            expect: META_KEY,
            given: &kv.key,
        }
    );

    let meta_value = kv.value.as_ref().context(Base64MetaValueNotFound)?;
    ensure!(
        !meta_value.is_empty(),
        InvalidBase64MetaValueLen { meta_value }
    );

    let raw_bytes = base64::decode(meta_value).context(DecodeBase64MetaValue { meta_value })?;

    decode_sst_meta_data_from_bytes(&raw_bytes)
}

/// RecordEncoder is used for encoding ArrowBatch.
///
/// TODO: allow pre-allocate buffer
#[async_trait]
trait RecordEncoder {
    /// Encode vector of arrow batch, return encoded row number
    async fn encode(&mut self, record_batches: Vec<ArrowRecordBatch>) -> Result<usize>;

    fn set_meta_data_path(&mut self, metadata_path: Option<String>) -> Result<()>;

    /// Return encoded bytes
    /// Note: trait method cannot receive `self`, so take a &mut self here to
    /// indicate this encoder is already consumed
    async fn close(&mut self) -> Result<()>;
}

struct ColumnarRecordEncoder<W> {
    // wrap in Option so ownership can be taken out behind `&mut self`
    arrow_writer: Option<AsyncArrowWriter<W>>,
    arrow_schema: ArrowSchemaRef,
}

impl<W: AsyncWrite + Send + Unpin> ColumnarRecordEncoder<W> {
    fn try_new(
        sink: W,
        schema: &Schema,
        num_rows_per_row_group: usize,
        max_buffer_size: usize,
        compression: Compression,
    ) -> Result<Self> {
        let arrow_schema = schema.to_arrow_schema_ref();

        let write_props = WriterProperties::builder()
            .set_max_row_group_size(num_rows_per_row_group)
            .set_compression(compression)
            .build();

        let arrow_writer = AsyncArrowWriter::try_new(
            sink,
            arrow_schema.clone(),
            max_buffer_size,
            Some(write_props),
        )
        .box_err()
        .context(EncodeRecordBatch)?;

        Ok(Self {
            arrow_writer: Some(arrow_writer),
            arrow_schema,
        })
    }
}

#[async_trait]
impl<W: AsyncWrite + Send + Unpin> RecordEncoder for ColumnarRecordEncoder<W> {
    async fn encode(&mut self, arrow_record_batch_vec: Vec<ArrowRecordBatch>) -> Result<usize> {
        assert!(self.arrow_writer.is_some());

        let record_batch = compute::concat_batches(&self.arrow_schema, &arrow_record_batch_vec)
            .box_err()
            .context(EncodeRecordBatch)?;

        self.arrow_writer
            .as_mut()
            .unwrap()
            .write(&record_batch)
            .await
            .box_err()
            .context(EncodeRecordBatch)?;

        Ok(record_batch.num_rows())
    }

    fn set_meta_data_path(&mut self, metadata_path: Option<String>) -> Result<()> {
        let path_kv = KeyValue {
            key: META_PATH_KEY.to_string(),
            value: metadata_path,
        };
        let version_kv = KeyValue {
            key: META_VERSION_KEY.to_string(),
            value: Some(META_VERSION_CURRENT.to_string()),
        };
        let writer = self.arrow_writer.as_mut().unwrap();
        writer.append_key_value_metadata(path_kv);
        writer.append_key_value_metadata(version_kv);

        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        assert!(self.arrow_writer.is_some());

        let arrow_writer = self.arrow_writer.take().unwrap();
        arrow_writer
            .close()
            .await
            .box_err()
            .context(EncodeRecordBatch)?;

        Ok(())
    }
}

pub struct ParquetEncoder {
    record_encoder: Box<dyn RecordEncoder + Send>,
}

impl ParquetEncoder {
    pub fn try_new<W: AsyncWrite + Unpin + Send + 'static>(
        sink: W,
        schema: &Schema,
        num_rows_per_row_group: usize,
        max_buffer_size: usize,
        compression: Compression,
    ) -> Result<Self> {
        Ok(ParquetEncoder {
            record_encoder: Box::new(ColumnarRecordEncoder::try_new(
                sink,
                schema,
                num_rows_per_row_group,
                max_buffer_size,
                compression,
            )?),
        })
    }

    /// Encode the record batch with [ArrowWriter] and the encoded contents is
    /// written to the buffer.
    pub async fn encode_record_batches(
        &mut self,
        arrow_record_batches: Vec<ArrowRecordBatch>,
    ) -> Result<usize> {
        if arrow_record_batches.is_empty() {
            return Ok(0);
        }

        self.record_encoder.encode(arrow_record_batches).await
    }

    pub fn set_meta_data_path(&mut self, meta_data_path: Option<String>) -> Result<()> {
        self.record_encoder.set_meta_data_path(meta_data_path)
    }

    pub async fn close(mut self) -> Result<()> {
        self.record_encoder.close().await
    }
}

/// RecordDecoder is used for decoding ArrowRecordBatch based on
/// `schema.StorageFormat`
trait RecordDecoder {
    fn decode(&self, arrow_record_batch: ArrowRecordBatch) -> Result<ArrowRecordBatch>;
}

struct ColumnarRecordDecoder {}

impl RecordDecoder for ColumnarRecordDecoder {
    fn decode(&self, arrow_record_batch: ArrowRecordBatch) -> Result<ArrowRecordBatch> {
        Ok(arrow_record_batch)
    }
}

pub struct ParquetDecoder {
    record_decoder: Box<dyn RecordDecoder>,
}

impl Default for ParquetDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl ParquetDecoder {
    pub fn new() -> Self {
        Self {
            record_decoder: Box::new(ColumnarRecordDecoder {}),
        }
    }

    pub fn decode_record_batch(
        &self,
        arrow_record_batch: ArrowRecordBatch,
    ) -> Result<ArrowRecordBatch> {
        self.record_decoder.decode(arrow_record_batch)
    }
}
