use common_types::{
    bytes::{self, BytesMut, MemBuf, MemBufMut},
    table::TableId,
    SequenceNumber,
};
use common_util::{
    codec::{Decoder, Encoder},
    define_result,
};
use message_queue::Offset;
use proto::wal::{
    TableNextSequenceEntry as TableNextSequenceEntryPb, TableNextSequences as TableNextSequencesPb,
};
use protobuf::Message;
use snafu::{ensure, Backtrace, ResultExt, Snafu};

use crate::{
    kv_encoder::Namespace,
    manager::{self, RegionId},
};

const META_KEY_MAGIC: u8 = 42;
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
pub fn format_wal_data_topic_name(namespace: &str, region_id: RegionId) -> String {
    format!("{}_data_{}", namespace, region_id)
}

/// Generate wal meta topic name
pub fn format_wal_meta_topic_name(namespace: &str, region_id: RegionId) -> String {
    format!("{}_meta_{}", namespace, region_id)
}

#[derive(Clone, Debug)]
pub struct MetaEncoding {
    key_enc: MetaKeyEncoder,
    value_enc: TableNextSequencesEncoder,
}

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

    pub fn encode_value(&self, meta_value: TableNextSequences) -> manager::Result<Vec<u8>> {
        self
            .value_enc
            .encode(meta_value)
            .map_err(|e| Box::new(e) as _)
            .context(manager::Encoding)
    }

    pub fn decode_key(&self, mut buf: &[u8]) -> manager::Result<MetaKey> {
        self.key_enc
            .decode(&mut buf)
            .map_err(|e| Box::new(e) as _)
            .context(manager::Decoding)
    }

    pub fn decode_value(&self, buf: &[u8]) -> manager::Result<TableNextSequences> {
        self.value_enc
            .decode(buf)
            .map_err(|e| Box::new(e) as _)
            .context(manager::Decoding)
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
                magic: META_KEY_MAGIC,
                version: NEWEST_MQ_META_KEY_ENCODING_VERSION,
            },
            value_enc: TableNextSequencesEncoder {},
        }
    }
}

/// Message queue implementation's meta key
#[derive(Clone, Debug)]
pub struct MetaKey {
    region_id: RegionId,
}

#[derive(Clone, Debug)]
pub struct MetaKeyEncoder {
    pub namespace: Namespace,
    pub magic: u8,
    pub version: u8,
}

impl MetaKeyEncoder {
    /// Determine whether the raw bytes is a valid meta key.
    pub fn is_valid<B: MemBuf>(&self, buf: &mut B) -> Result<bool> {
        let namespace = buf.read_u8().context(DecodeMetaKey)?;
        let magic = buf.read_u8().context(DecodeMetaKey)?;
        Ok(namespace == self.namespace as u8 && magic == self.magic)
    }
}

impl Encoder<MetaKey> for MetaKeyEncoder {
    type Error = Error;

    /// Key format:
    ///
    /// ```text
    /// +---------------+-----------+---------------------+----------------+
    /// | namespace(u8) | magic(u8) |  version header(u8) | region id(u64) |
    /// +---------------+-----------+---------------------+----------------+
    /// ```
    ///
    /// More information can be extended after the incremented `version header`.
    fn encode<B: MemBufMut>(&self, buf: &mut B, meta_key: &MetaKey) -> Result<()> {
        buf.write_u8(self.namespace as u8).context(EncodeMetaKey)?;
        buf.write_u8(self.magic).context(EncodeMetaKey)?;
        buf.write_u8(self.version).context(EncodeMetaKey)?;

        Ok(())
    }

    fn estimate_encoded_size(&self, _log_key: &MetaKey) -> usize {
        // Refer to key format.
        1 + 1 + 8 + 1
    }
}

impl Decoder<MetaKey> for MetaKeyEncoder {
    type Error = Error;

    fn decode<B: MemBuf>(&self, buf: &mut B) -> Result<MetaKey> {
        // check namespace
        let namespace = buf.read_u8().context(DecodeMetaKey)?;
        ensure!(
            namespace == self.namespace as u8,
            InvalidNamespace {
                expect: self.namespace,
                given: namespace
            }
        );

        let magic = buf.read_u8().context(DecodeMetaKey)?;
        ensure!(
            magic == self.magic,
            InvalidMetaKeyMagic {
                expect: self.magic,
                given: magic,
            }
        );

        let region_id = buf.read_u64().context(DecodeMetaKey)?;

        // check version
        let version = buf.read_u8().context(DecodeMetaKey)?;
        ensure!(
            version == self.version,
            InvalidVersion {
                expect: self.version,
                given: version
            }
        );

        Ok(MetaKey { region_id })
    }
}

/// Message queue implementation's meta value.
///
/// Include all tables(of current shard) and their next sequence number.
pub struct TableNextSequences {
    pub next_offset: Offset,
    pub entries: Vec<TableNextSequenceEntry>,
}

pub struct TableNextSequenceEntry {
    pub table_id: TableId,
    pub next_sequence_num: SequenceNumber,
}

impl TableNextSequenceEntry {
    pub fn new(table_id: TableId, next_sequence_num: SequenceNumber) -> Self {
        Self {
            table_id,
            next_sequence_num,
        }
    }
}

#[derive(Debug, Clone)]
struct TableNextSequencesEncoder;

impl TableNextSequencesEncoder {
    fn encode(&self, value: TableNextSequences) -> Result<Vec<u8>> {
        let value_pb: TableNextSequencesPb = value.into();
        value_pb
            .write_to_bytes()
            .map_err(|e| Box::new(e) as _)
            .context(EncodeMetaValue)
    }

    fn decode(&self, buf: &[u8]) -> Result<TableNextSequences> {
        let value_pb: TableNextSequencesPb = Message::parse_from_bytes(buf)
            .map_err(|e| Box::new(e) as _)
            .context(DecodeMetaValue)?;
        Ok(value_pb.into())
    }
}

impl From<TableNextSequences> for TableNextSequencesPb {
    fn from(table_next_sequences: TableNextSequences) -> Self {
        let mut table_next_sequences_pb = TableNextSequencesPb::default();
        table_next_sequences_pb.set_next_offset(table_next_sequences.next_offset);
        let entries_pb: Vec<_> = table_next_sequences
            .entries
            .into_iter()
            .map(|entry| entry.into())
            .collect();
        table_next_sequences_pb.set_entries(entries_pb.into());

        table_next_sequences_pb
    }
}

impl From<TableNextSequencesPb> for TableNextSequences {
    fn from(table_next_sequences_pb: TableNextSequencesPb) -> Self {
        let entries = table_next_sequences_pb
            .entries
            .into_iter()
            .map(|e| e.into())
            .collect();
        Self {
            next_offset: table_next_sequences_pb.next_offset,
            entries,
        }
    }
}

impl From<TableNextSequenceEntry> for TableNextSequenceEntryPb {
    fn from(table_next_sequence_entry: TableNextSequenceEntry) -> Self {
        let mut table_next_sequence_entry_pb = TableNextSequenceEntryPb::default();
        table_next_sequence_entry_pb.set_table_id(table_next_sequence_entry.table_id);
        table_next_sequence_entry_pb
            .set_next_sequence_num(table_next_sequence_entry.next_sequence_num);

        table_next_sequence_entry_pb
    }
}

impl From<TableNextSequenceEntryPb> for TableNextSequenceEntry {
    fn from(table_next_sequence_entry_pb: TableNextSequenceEntryPb) -> Self {
        Self {
            table_id: table_next_sequence_entry_pb.table_id,
            next_sequence_num: table_next_sequence_entry_pb.next_sequence_num,
        }
    }
}
