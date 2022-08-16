// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{convert::TryFrom, io::Write};

use arrow_deps::{
    arrow::record_batch::RecordBatch as ArrowRecordBatch,
    parquet::{
        arrow::ArrowWriter,
        basic::Compression,
        file::{metadata::KeyValue, properties::WriterProperties},
    },
};
use common_types::{
    bytes::{BytesMut, MemBufMut, Writer},
    schema::{ArrowSchemaRef, Schema, StorageFormat},
};
use common_util::define_result;
use proto::sst::SstMetaData as SstMetaDataPb;
use protobuf::Message;
use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};

use crate::sst::{file::SstMetaData, parquet::hybrid};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to encode sst meta data, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    EncodeIntoPb {
        source: protobuf::ProtobufError,
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
        source: protobuf::ProtobufError,
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

    #[snafu(display("Failed to convert sst meta data from protobuf, err:{}", source))]
    ConvertSstMetaData { source: crate::sst::file::Error },

    #[snafu(display(
        "Failed to encode record batch into sst, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    EncodeRecordBatch {
        source: Box<dyn std::error::Error + Send + Sync>,
        backtrace: Backtrace,
    },

    #[snafu(display("Tsid is required for hybrid format.\nBacktrace:\n{}", backtrace))]
    TsidRequired { backtrace: Backtrace },
}

define_result!(Error);

pub const META_KEY: &str = "meta";
pub const META_VALUE_HEADER: u8 = 0;

/// Encode the sst meta data into binary key value pair.
pub fn encode_sst_meta_data(meta_data: SstMetaData) -> Result<KeyValue> {
    let meta_data_pb = SstMetaDataPb::from(meta_data);

    let mut buf = BytesMut::with_capacity(meta_data_pb.compute_size() as usize + 1);
    buf.write_u8(META_VALUE_HEADER)
        .expect("Should write header into the buffer successfully");

    // encode the sst meta data into protobuf binary
    {
        let mut writer = Writer::new(&mut buf);
        meta_data_pb
            .write_to_writer(&mut writer)
            .context(EncodeIntoPb)?;
    }
    Ok(KeyValue {
        key: META_KEY.to_string(),
        value: Some(base64::encode(buf.as_ref())),
    })
}

/// Decode the sst meta data from the binary key value pair.
pub fn decode_sst_meta_data(kv: &KeyValue) -> Result<SstMetaData> {
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

    ensure!(!raw_bytes.is_empty(), InvalidMetaValueLen { meta_value });

    ensure!(
        raw_bytes[0] == META_VALUE_HEADER,
        InvalidMetaValueHeader { meta_value }
    );

    let meta_data_pb: SstMetaDataPb =
        Message::parse_from_bytes(&raw_bytes[1..]).context(DecodeFromPb { meta_value })?;

    SstMetaData::try_from(meta_data_pb).context(ConvertSstMetaData)
}

pub struct ParquetEncoder<W: Write> {
    writer: ArrowWriter<W>,
    format: StorageFormat,
    origin_arrow_schema: ArrowSchemaRef,
    hybrid_arrow_schema: Option<ArrowSchemaRef>,
    schema: Schema,
}

impl<W: Write> ParquetEncoder<W> {
    pub fn try_new(
        writer: W,
        num_rows_per_row_group: usize,
        compression: Compression,
        meta_data: &SstMetaData,
    ) -> Result<Self> {
        let write_props = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![encode_sst_meta_data(meta_data.clone())?]))
            .set_max_row_group_size(num_rows_per_row_group)
            .set_compression(compression)
            .build();
        let mut format = meta_data.schema.storage_format();
        // TODO: remove this overwrite when we can set format via table options
        if matches!(format, StorageFormat::Hybrid) && meta_data.schema.index_of_tsid().is_none() {
            format = StorageFormat::Columnar;
        }
        let mut hybrid_arrow_schema = None;
        let arrow_schema = match format {
            StorageFormat::Hybrid => {
                let tsid_idx = meta_data.schema.index_of_tsid().context(TsidRequired)?;
                let schema = hybrid::build_hybrid_arrow_schema(tsid_idx, &meta_data.schema);
                hybrid_arrow_schema = Some(schema.clone());
                schema
            }
            StorageFormat::Columnar => meta_data.schema.as_arrow_schema_ref().clone(),
        };
        let writer = ArrowWriter::try_new(writer, arrow_schema, Some(write_props))
            .map_err(|e| Box::new(e) as _)
            .context(EncodeRecordBatch)?;
        Ok(ParquetEncoder {
            writer,
            format,
            origin_arrow_schema: meta_data.schema.as_arrow_schema_ref().clone(),
            hybrid_arrow_schema,
            schema: meta_data.schema.clone(),
        })
    }

    /// Encode the record batch with [ArrowWriter] and the encoded contents is
    /// written to the buffer.
    pub fn encode_record_batch(
        &mut self,
        arrow_record_batch_vec: Vec<ArrowRecordBatch>,
    ) -> Result<usize> {
        if arrow_record_batch_vec.is_empty() {
            return Ok(0);
        }

        let record_batch = match self.format {
            StorageFormat::Hybrid => hybrid::convert_to_hybrid(
                &self.schema,
                self.hybrid_arrow_schema.clone().unwrap(),
                self.schema.index_of_tsid().expect("checked in try_new"),
                arrow_record_batch_vec,
            )
            .map_err(|e| Box::new(e) as _)
            .context(EncodeRecordBatch)?,
            StorageFormat::Columnar => {
                ArrowRecordBatch::concat(&self.origin_arrow_schema, &arrow_record_batch_vec)
                    .map_err(|e| Box::new(e) as _)
                    .context(EncodeRecordBatch)?
            }
        };

        self.writer
            .write(&record_batch)
            .map_err(|e| Box::new(e) as _)
            .context(EncodeRecordBatch)?;

        Ok(record_batch.num_rows())
    }

    pub fn close(self) -> Result<()> {
        self.writer
            .close()
            .map_err(|e| Box::new(e) as _)
            .context(EncodeRecordBatch)?;
        Ok(())
    }
}
