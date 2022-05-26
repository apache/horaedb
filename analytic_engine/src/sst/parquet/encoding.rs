// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::convert::TryFrom;

use arrow_deps::parquet::file::metadata::KeyValue;
use common_types::bytes::{BytesMut, MemBufMut, Writer};
use common_util::define_result;
use proto::sst::SstMetaData as SstMetaDataPb;
use protobuf::Message;
use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};

use crate::sst::file::SstMetaData;

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
