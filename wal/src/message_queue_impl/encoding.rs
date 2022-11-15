// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Meta encoding of wal's message queue implementation

use common_types::bytes::{self, Buf, BufMut, BytesMut, SafeBuf, SafeBufMut};
use common_util::{
    codec::{Decoder, Encoder},
    define_result,
};
use prost::Message;
use proto::wal_on_mq::{
    table_meta_data::SafeDeleteOffset, RegionMetaSnapshot as RegionMetaSnapshotPb,
    TableMetaData as TableMetaDataPb,
};
use snafu::{ensure, Backtrace, ResultExt, Snafu};

use crate::{
    kv_encoder::Namespace,
    manager::{self, RegionId},
    message_queue_impl::region_context::{RegionMetaSnapshot, TableMetaData},
};

const NEWEST_MQ_META_KEY_ENCODING_VERSION: u8 = 0;
const NEWEST_MQ_META_VALUE_ENCODING_VERSION: u8 = 0;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to encode meta key of message queue implementation, source:{}",
        source
    ))]
    EncodeMetaKey { source: bytes::Error },

    #[snafu(display(
        "Failed to encode meta value of message queue implementation, err:{}",
        source
    ))]
    EncodeMetaValue {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to decode meta key of message queue implementation, err:{}",
        source
    ))]
    DecodeMetaKey { source: bytes::Error },

    #[snafu(display(
        "Failed to decode meta value of message queue implementation, err:{}",
        source
    ))]
    DecodeMetaValue {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Found invalid meta key magic of message queue implementation, expect:{}, given:{}.\nBacktrace:\n{}",
        expect,
        given,
        backtrace
    ))]
    InvalidMetaKeyMagic {
        expect: u8,
        given: u8,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Found invalid namespace, expect:{:?}, given:{}.\nBacktrace:\n{}",
        expect,
        given,
        backtrace
    ))]
    InvalidNamespace {
        expect: Namespace,
        given: u8,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Found invalid version, expect:{}, given:{}.\nBacktrace:\n{}",
        expect,
        given,
        backtrace
    ))]
    InvalidVersion {
        expect: u8,
        given: u8,
        backtrace: Backtrace,
    },
}

define_result!(Error);

/// Generate wal data topic name
#[allow(unused)]
pub fn format_wal_data_topic_name(namespace: &str, region_id: RegionId) -> String {
    format!("{}_data_{}", namespace, region_id)
}

/// Generate wal meta topic name
#[allow(unused)]
pub fn format_wal_meta_topic_name(namespace: &str, region_id: RegionId) -> String {
    format!("{}_meta_{}", namespace, region_id)
}

#[allow(unused)]
#[derive(Clone, Debug)]
pub struct MetaEncoding {
    key_enc: MetaKeyEncoder,
    value_enc: MetaValueEncoder,
}

#[allow(unused)]
impl MetaEncoding {
    pub fn encode_key(&self, buf: &mut BytesMut, meta_key: &MetaKey) -> manager::Result<()> {
        buf.clear();
        buf.reserve(self.key_enc.estimate_encoded_size(meta_key));
        self.key_enc
            .encode(buf, meta_key)
            .map_err(|e| Box::new(e) as _)
            .context(manager::Encoding)?;

        Ok(())
    }

    pub fn encode_value(
        &self,
        buf: &mut BytesMut,
        region_meta_snapshot: RegionMetaSnapshot,
    ) -> manager::Result<()> {
        let meta_value = region_meta_snapshot.into();

        buf.clear();
        buf.reserve(self.value_enc.estimate_encoded_size(&meta_value));
        self.value_enc
            .encode(buf, &meta_value)
            .map_err(|e| Box::new(e) as _)
            .context(manager::Encoding)
    }

    pub fn decode_key(&self, mut buf: &[u8]) -> manager::Result<MetaKey> {
        self.key_enc
            .decode(&mut buf)
            .map_err(|e| Box::new(e) as _)
            .context(manager::Decoding)
    }

    pub fn decode_value(&self, mut buf: &[u8]) -> manager::Result<RegionMetaSnapshot> {
        let meta_value = self
            .value_enc
            .decode(&mut buf)
            .map_err(|e| Box::new(e) as _)
            .context(manager::Decoding)?;

        Ok(meta_value.into())
    }

    pub fn is_meta_key(&self, mut buf: &[u8]) -> manager::Result<bool> {
        self.key_enc
            .is_valid(&mut buf)
            .map_err(|e| Box::new(e) as _)
            .context(manager::Decoding)
    }

    pub fn newest() -> Self {
        Self {
            key_enc: MetaKeyEncoder {
                namespace: Namespace::Meta,
                version: NEWEST_MQ_META_KEY_ENCODING_VERSION,
            },
            value_enc: MetaValueEncoder {
                version: NEWEST_MQ_META_VALUE_ENCODING_VERSION,
            },
        }
    }
}

/// Message queue implementation's meta key
#[allow(unused)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetaKey(pub RegionId);

#[allow(unused)]
#[derive(Clone, Debug)]
pub struct MetaKeyEncoder {
    pub namespace: Namespace,
    pub version: u8,
}

#[allow(unused)]
impl MetaKeyEncoder {
    /// Determine whether the raw bytes is a valid meta key.
    pub fn is_valid<B: Buf>(&self, buf: &mut B) -> Result<bool> {
        let namespace = buf.try_get_u8().context(DecodeMetaKey)?;
        let version = buf.try_get_u8().context(DecodeMetaKey)?;
        Ok(namespace == self.namespace as u8 && version == self.version)
    }
}

impl Encoder<MetaKey> for MetaKeyEncoder {
    type Error = Error;

    /// Key format:
    ///
    /// ```text
    /// +--------------------+----------------+----------------+
    /// | version header(u8) |  namespace(u8) | region id(u64) |
    /// +--------------------+----------------+----------------+
    /// ```
    ///
    /// More information can be extended after the incremented `version header`.
    fn encode<B: BufMut>(&self, buf: &mut B, meta_key: &MetaKey) -> Result<()> {
        buf.try_put_u8(self.namespace as u8)
            .context(EncodeMetaKey)?;
        buf.try_put_u8(self.version).context(EncodeMetaKey)?;
        buf.try_put_u64(meta_key.0).context(EncodeMetaKey)?;

        Ok(())
    }

    fn estimate_encoded_size(&self, _log_key: &MetaKey) -> usize {
        // Refer to key format.
        1 + 1 + 8
    }
}

impl Decoder<MetaKey> for MetaKeyEncoder {
    type Error = Error;

    fn decode<B: Buf>(&self, buf: &mut B) -> Result<MetaKey> {
        // check namespace
        let namespace = buf.try_get_u8().context(DecodeMetaKey)?;
        ensure!(
            namespace == self.namespace as u8,
            InvalidNamespace {
                expect: self.namespace,
                given: namespace
            }
        );

        let version = buf.try_get_u8().context(DecodeMetaKey)?;
        ensure!(
            version == self.version,
            InvalidVersion {
                expect: self.version,
                given: version,
            }
        );

        let region_id = buf.try_get_u64().context(DecodeMetaKey)?;

        Ok(MetaKey(region_id))
    }
}

/// Message queue implementation's meta value.
///
/// Include all tables(of current shard) and meta data.
#[derive(Clone, Debug)]
pub struct MetaValue(RegionMetaSnapshotPb);

#[derive(Clone, Debug)]
pub struct MetaValueEncoder {
    pub version: u8,
}

impl Encoder<MetaValue> for MetaValueEncoder {
    type Error = Error;

    /// Key format:
    ///
    /// ```text
    /// +--------------------+----------------------+
    /// | version header(u8) | region meta snapshot |
    /// +--------------------+----------------------+
    /// ```
    ///
    /// More information can be extended after the incremented `version header`.
    fn encode<B: BufMut>(&self, buf: &mut B, meta_value: &MetaValue) -> Result<()> {
        buf.try_put_u8(self.version)
            .map_err(|e| Box::new(e) as _)
            .context(EncodeMetaValue)?;
        meta_value
            .0
            .encode(buf)
            .map_err(|e| Box::new(e) as _)
            .context(EncodeMetaValue)
    }

    fn estimate_encoded_size(&self, meta_value: &MetaValue) -> usize {
        // Refer to key format.
        1 + meta_value.0.encoded_len()
    }
}

impl Decoder<MetaValue> for MetaValueEncoder {
    type Error = Error;

    fn decode<B: Buf>(&self, buf: &mut B) -> Result<MetaValue> {
        // Check version.
        let version = buf
            .try_get_u8()
            .map_err(|e| Box::new(e) as _)
            .context(DecodeMetaValue)?;
        ensure!(
            version == self.version,
            InvalidVersion {
                expect: self.version,
                given: version,
            }
        );

        let region_meta_snapshot = Message::decode(buf)
            .map_err(|e| Box::new(e) as _)
            .context(DecodeMetaValue)?;

        Ok(MetaValue(region_meta_snapshot))
    }
}

impl From<RegionMetaSnapshot> for MetaValue {
    fn from(region_meta_snapshot: RegionMetaSnapshot) -> Self {
        let entries_pb: Vec<_> = region_meta_snapshot
            .entries
            .into_iter()
            .map(|entry| entry.into())
            .collect();
        let region_meta_snapshot_pb = RegionMetaSnapshotPb {
            entries: entries_pb,
        };

        MetaValue(region_meta_snapshot_pb)
    }
}

impl From<TableMetaData> for TableMetaDataPb {
    fn from(table_meta_data: TableMetaData) -> Self {
        TableMetaDataPb {
            table_id: table_meta_data.table_id,
            next_sequence_num: table_meta_data.next_sequence_num,
            latest_marked_deleted: table_meta_data.latest_marked_deleted,
            current_high_watermark: table_meta_data.current_high_watermark,
            safe_delete_offset: table_meta_data
                .safe_delete_offset
                .map(SafeDeleteOffset::Offset),
        }
    }
}

impl From<MetaValue> for RegionMetaSnapshot {
    fn from(meta_value: MetaValue) -> Self {
        let entries = meta_value.0.entries.into_iter().map(|e| e.into()).collect();
        Self { entries }
    }
}

impl From<TableMetaDataPb> for TableMetaData {
    fn from(table_meta_data_pb: TableMetaDataPb) -> Self {
        let safe_delete_offset = match table_meta_data_pb.safe_delete_offset {
            Some(SafeDeleteOffset::Offset(offset)) => Some(offset),
            _ => None,
        };

        Self {
            table_id: table_meta_data_pb.table_id,
            next_sequence_num: table_meta_data_pb.next_sequence_num,
            latest_marked_deleted: table_meta_data_pb.latest_marked_deleted,
            current_high_watermark: table_meta_data_pb.current_high_watermark,
            safe_delete_offset,
        }
    }
}

#[cfg(test)]
mod tests {
    use common_types::bytes::BytesMut;

    use super::{MetaEncoding, MetaKey};
    use crate::message_queue_impl::region_context::{RegionMetaSnapshot, TableMetaData};

    #[test]
    fn test_meta_encoding() {
        // Meta key
        let region_id = 42_u64;
        let test_meta_key = MetaKey(region_id);

        // Meta value
        let test_table_meta1 = TableMetaData {
            table_id: 0,
            next_sequence_num: 42,
            latest_marked_deleted: 40,
            current_high_watermark: 142,
            safe_delete_offset: Some(140),
        };

        let test_table_meta2 = TableMetaData {
            table_id: 1,
            next_sequence_num: 2,
            latest_marked_deleted: 1,
            current_high_watermark: 12,
            safe_delete_offset: Some(10),
        };

        let test_region_snapshot = RegionMetaSnapshot {
            entries: vec![test_table_meta1, test_table_meta2],
        };

        // Encode them.
        let meta_encoding = MetaEncoding::newest();

        let mut key_buf = BytesMut::new();
        let mut value_buf = BytesMut::new();
        meta_encoding
            .encode_key(&mut key_buf, &test_meta_key)
            .unwrap();
        meta_encoding
            .encode_value(&mut value_buf, test_region_snapshot.clone())
            .unwrap();

        // Decode and compare.
        let decoded_key = meta_encoding.decode_key(&key_buf).unwrap();
        let decoded_value = meta_encoding.decode_value(&value_buf).unwrap();
        assert_eq!(test_meta_key, decoded_key);
        assert_eq!(test_region_snapshot, decoded_value);
    }
}
