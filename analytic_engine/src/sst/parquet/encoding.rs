// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    convert::TryFrom,
    io::Write,
    sync::{Arc, Mutex},
};

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
    datum::DatumKind,
    schema::{ArrowSchemaRef, Schema, StorageFormat},
};
use common_util::define_result;
use proto::sst::SstMetaData as SstMetaDataPb;
use protobuf::Message;
use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};

use crate::sst::{
    file::SstMetaData,
    parquet::hybrid::{self, IndexedType},
};

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

    #[snafu(display(
        "Key column must be string type. type:{}\nBacktrace:\n{}",
        type_name,
        backtrace
    ))]
    StringKeyColumnRequired {
        type_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Hybrid format doesn't support variable length type, type:{}.\nBacktrace:\n{}",
        type_name,
        backtrace
    ))]
    VariableLengthType {
        type_name: String,
        backtrace: Backtrace,
    },
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

/// RecordEncoder is used for encoding ArrowBatch.
///
/// TODO: allow pre-allocate buffer
trait RecordEncoder {
    /// Encode vector of arrow batch, return encoded row number
    fn encode(&mut self, arrow_record_batch_vec: Vec<ArrowRecordBatch>) -> Result<usize>;

    /// Return encoded bytes
    /// Note: trait method cannot receive `self`, so take a &mut self here to
    /// indicate this encoder is already consumed
    fn close(&mut self) -> Result<Vec<u8>>;
}

/// EncodingWriter implements `Write` trait, useful when Writer need shared
/// ownership.
///
/// TODO: This is a temp workaround for [ArrowWriter](https://docs.rs/parquet/20.0.0/parquet/arrow/arrow_writer/struct.ArrowWriter.html), since it has no method to get underlying Writer
/// We can fix this by add `into_inner` method to it, or just replace it with
/// parquet2, which already have this method
/// https://github.com/CeresDB/ceresdb/issues/53
#[derive(Clone)]
struct EncodingWriter(Arc<Mutex<Vec<u8>>>);

impl EncodingWriter {
    fn into_bytes(self) -> Vec<u8> {
        self.0.lock().unwrap().clone()
    }
}

impl Write for EncodingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut inner = self.0.lock().unwrap();
        inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct ColumnarRecordEncoder {
    buf: EncodingWriter,
    // wrap in Option so ownership can be taken out behind `&mut self`
    arrow_writer: Option<ArrowWriter<EncodingWriter>>,
    arrow_schema: ArrowSchemaRef,
}

impl ColumnarRecordEncoder {
    fn try_new(write_props: WriterProperties, schema: &Schema) -> Result<Self> {
        let arrow_schema = schema.to_arrow_schema_ref();

        let buf = EncodingWriter(Arc::new(Mutex::new(Vec::new())));
        let arrow_writer =
            ArrowWriter::try_new(buf.clone(), arrow_schema.clone(), Some(write_props))
                .map_err(|e| Box::new(e) as _)
                .context(EncodeRecordBatch)?;

        Ok(Self {
            buf,
            arrow_writer: Some(arrow_writer),
            arrow_schema,
        })
    }
}

impl RecordEncoder for ColumnarRecordEncoder {
    fn encode(&mut self, arrow_record_batch_vec: Vec<ArrowRecordBatch>) -> Result<usize> {
        assert!(self.arrow_writer.is_some());

        let record_batch = ArrowRecordBatch::concat(&self.arrow_schema, &arrow_record_batch_vec)
            .map_err(|e| Box::new(e) as _)
            .context(EncodeRecordBatch)?;

        self.arrow_writer
            .as_mut()
            .unwrap()
            .write(&record_batch)
            .map_err(|e| Box::new(e) as _)
            .context(EncodeRecordBatch)?;

        Ok(record_batch.num_rows())
    }

    fn close(&mut self) -> Result<Vec<u8>> {
        assert!(self.arrow_writer.is_some());

        let arrow_writer = self.arrow_writer.take().unwrap();
        arrow_writer
            .close()
            .map_err(|e| Box::new(e) as _)
            .context(EncodeRecordBatch)?;

        Ok(self.buf.clone().into_bytes())
    }
}

struct HybridRecordEncoder {
    buf: EncodingWriter,
    // wrap in Option so ownership can be taken out behind `&mut self`
    arrow_writer: Option<ArrowWriter<EncodingWriter>>,
    arrow_schema: ArrowSchemaRef,
    tsid_type: IndexedType,
    non_collapsible_col_types: Vec<IndexedType>,
    // columns that can be collpased into list
    collapsible_col_types: Vec<IndexedType>,
}

impl HybridRecordEncoder {
    fn try_new(write_props: WriterProperties, schema: &Schema) -> Result<Self> {
        // TODO: What we really want here is a unique ID, tsid is one case
        // Maybe support other cases later.
        let tsid_idx = schema.index_of_tsid().context(TsidRequired)?;
        let tsid_type = IndexedType {
            idx: tsid_idx,
            data_type: schema.column(tsid_idx).data_type,
        };

        let mut non_collapsible_col_types = Vec::new();
        let mut collapsible_col_types = Vec::new();
        for (idx, col) in schema.columns().iter().enumerate() {
            if idx == tsid_idx {
                continue;
            }

            if schema.is_collapsible_column(idx) {
                // TODO: support variable length type
                ensure!(
                    col.data_type.size().is_some(),
                    VariableLengthType {
                        type_name: col.data_type.to_string(),
                    }
                );

                collapsible_col_types.push(IndexedType {
                    idx,
                    data_type: schema.column(idx).data_type,
                });
            } else {
                // TODO: support non-string key columns
                ensure!(
                    matches!(col.data_type, DatumKind::String),
                    StringKeyColumnRequired {
                        type_name: col.data_type.to_string(),
                    }
                );
                non_collapsible_col_types.push(IndexedType {
                    idx,
                    data_type: col.data_type,
                });
            }
        }

        let arrow_schema = hybrid::build_hybrid_arrow_schema(schema);

        let buf = EncodingWriter(Arc::new(Mutex::new(Vec::new())));
        let arrow_writer =
            ArrowWriter::try_new(buf.clone(), arrow_schema.clone(), Some(write_props))
                .map_err(|e| Box::new(e) as _)
                .context(EncodeRecordBatch)?;
        Ok(Self {
            buf,
            arrow_writer: Some(arrow_writer),
            arrow_schema,
            tsid_type,
            non_collapsible_col_types,
            collapsible_col_types,
        })
    }
}

impl RecordEncoder for HybridRecordEncoder {
    fn encode(&mut self, arrow_record_batch_vec: Vec<ArrowRecordBatch>) -> Result<usize> {
        assert!(self.arrow_writer.is_some());

        let record_batch = hybrid::convert_to_hybrid_record(
            &self.tsid_type,
            &self.non_collapsible_col_types,
            &self.collapsible_col_types,
            self.arrow_schema.clone(),
            arrow_record_batch_vec,
        )
        .map_err(|e| Box::new(e) as _)
        .context(EncodeRecordBatch)?;

        self.arrow_writer
            .as_mut()
            .unwrap()
            .write(&record_batch)
            .map_err(|e| Box::new(e) as _)
            .context(EncodeRecordBatch)?;

        Ok(record_batch.num_rows())
    }

    fn close(&mut self) -> Result<Vec<u8>> {
        assert!(self.arrow_writer.is_some());

        let arrow_writer = self.arrow_writer.take().unwrap();
        arrow_writer
            .close()
            .map_err(|e| Box::new(e) as _)
            .context(EncodeRecordBatch)?;
        Ok(self.buf.clone().into_bytes())
    }
}

pub struct ParquetEncoder {
    record_encoder: Box<dyn RecordEncoder + Send>,
}

impl ParquetEncoder {
    pub fn try_new(
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

        let record_encoder: Box<dyn RecordEncoder + Send> = match format {
            StorageFormat::Hybrid => Box::new(HybridRecordEncoder::try_new(
                write_props,
                &meta_data.schema,
            )?),
            StorageFormat::Columnar => Box::new(ColumnarRecordEncoder::try_new(
                write_props,
                &meta_data.schema,
            )?),
        };

        Ok(ParquetEncoder { record_encoder })
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

        self.record_encoder.encode(arrow_record_batch_vec)
    }

    pub fn close(mut self) -> Result<Vec<u8>> {
        self.record_encoder.close()
    }
}
