// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Partitioned table supports

pub mod rule;

use common_types::bytes::Bytes;
use prost::Message;
use proto::{meta_update as meta_pb, meta_update::partition_info::PartitionInfoEnum};
use snafu::{ensure, Backtrace, ResultExt, Snafu};

const DEFAULT_PARTITION_INFO_ENCODING_VERSION: u8 = 0;
const PARTITION_TABLE_PREFIX: &str = "__";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to build partition rule, msg:{}.\nBacktrace:{}\n",
        msg,
        backtrace
    ))]
    BuildPartitionRule { msg: String, backtrace: Backtrace },

    #[snafu(display(
        "Failed to locate partitions for write, msg:{}.\nBacktrace:{}\n",
        msg,
        backtrace
    ))]
    LocateWritePartition { msg: String, backtrace: Backtrace },

    #[snafu(display(
        "Failed to locate partitions for read, msg:{}.\nBacktrace:{}\n",
        msg,
        backtrace
    ))]
    LocateReadPartition { msg: String, backtrace: Backtrace },

    #[snafu(display("Internal error occurred, msg:{}", msg,))]
    Internal { msg: String },

    #[snafu(display("Failed to encode partition info by protobuf, err:{}", source))]
    EncodePartitionInfoToPb { source: prost::EncodeError },

    #[snafu(display(
        "Failed to decode partition info from protobuf bytes, buf:{:?}, err:{}",
        buf,
        source,
    ))]
    DecodePartitionInfoToPb {
        buf: Vec<u8>,
        source: prost::DecodeError,
    },

    #[snafu(display("Encoded partition info content is empty.\nBacktrace:\n{}", backtrace))]
    EmptyEncodedPartitionInfo { backtrace: Backtrace },

    #[snafu(display(
        "Invalid partition info encoding version, version:{}.\nBacktrace:\n{}",
        version,
        backtrace
    ))]
    InvalidPartitionInfoEncodingVersion { version: u8, backtrace: Backtrace },

    #[snafu(display("Partition info could not be empty"))]
    EmptyPartitionInfo {},
}

define_result!(Error);

/// Info for how to partition table
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PartitionInfo {
    Hash(HashPartitionInfo),
    Key(KeyPartitionInfo),
}

impl PartitionInfo {
    pub fn get_definitions(&self) -> Vec<PartitionDefinition> {
        match self {
            Self::Hash(v) => v.partition_definitions.clone(),
            Self::Key(v) => v.partition_definitions.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct PartitionDefinition {
    pub name: String,
    pub origin_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HashPartitionInfo {
    pub version: i32,
    pub partition_definitions: Vec<PartitionDefinition>,
    pub expr: Bytes,
    pub linear: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyPartitionInfo {
    pub version: i32,
    pub partition_definitions: Vec<PartitionDefinition>,
    pub partition_key: Vec<String>,
    pub linear: bool,
}

impl From<PartitionDefinition> for meta_pb::PartitionDefinition {
    fn from(definition: PartitionDefinition) -> Self {
        Self {
            name: definition.name,
            origin_name: definition
                .origin_name
                .map(meta_pb::partition_definition::OriginName::Origin),
        }
    }
}

impl From<meta_pb::PartitionDefinition> for PartitionDefinition {
    fn from(pb: meta_pb::PartitionDefinition) -> Self {
        let mut origin_name = None;
        if let Some(v) = pb.origin_name {
            match v {
                meta_pb::partition_definition::OriginName::Origin(name) => origin_name = Some(name),
            }
        }
        Self {
            name: pb.name,
            origin_name,
        }
    }
}

impl From<meta_pb::HashPartitionInfo> for HashPartitionInfo {
    fn from(partition_info_pb: meta_pb::HashPartitionInfo) -> Self {
        HashPartitionInfo {
            version: partition_info_pb.version,
            partition_definitions: partition_info_pb
                .partition_definitions
                .into_iter()
                .map(|v| v.into())
                .collect(),
            expr: Bytes::from(partition_info_pb.expr),
            linear: partition_info_pb.linear,
        }
    }
}

impl From<HashPartitionInfo> for meta_pb::HashPartitionInfo {
    fn from(partition_info: HashPartitionInfo) -> Self {
        meta_pb::HashPartitionInfo {
            version: partition_info.version,
            partition_definitions: partition_info
                .partition_definitions
                .into_iter()
                .map(|v| v.into())
                .collect(),
            expr: Bytes::into(partition_info.expr),
            linear: partition_info.linear,
        }
    }
}

impl From<meta_pb::KeyPartitionInfo> for KeyPartitionInfo {
    fn from(partition_info_pb: meta_pb::KeyPartitionInfo) -> Self {
        KeyPartitionInfo {
            version: partition_info_pb.version,
            partition_definitions: partition_info_pb
                .partition_definitions
                .into_iter()
                .map(|v| v.into())
                .collect(),
            partition_key: partition_info_pb.partition_key,
            linear: partition_info_pb.linear,
        }
    }
}

impl From<KeyPartitionInfo> for meta_pb::KeyPartitionInfo {
    fn from(partition_info: KeyPartitionInfo) -> Self {
        meta_pb::KeyPartitionInfo {
            version: partition_info.version,
            partition_definitions: partition_info
                .partition_definitions
                .into_iter()
                .map(|v| v.into())
                .collect(),
            partition_key: partition_info.partition_key,
            linear: partition_info.linear,
        }
    }
}

impl From<PartitionInfo> for meta_pb::PartitionInfo {
    fn from(partition_info: PartitionInfo) -> Self {
        match partition_info {
            PartitionInfo::Hash(v) => {
                let hash_partition_info = meta_pb::HashPartitionInfo::from(v);
                meta_pb::PartitionInfo {
                    partition_info_enum: Some(PartitionInfoEnum::Hash(hash_partition_info)),
                }
            }
            PartitionInfo::Key(v) => {
                let key_partition_info = meta_pb::KeyPartitionInfo::from(v);
                meta_pb::PartitionInfo {
                    partition_info_enum: Some(PartitionInfoEnum::Key(key_partition_info)),
                }
            }
        }
    }
}

impl TryFrom<meta_pb::PartitionInfo> for PartitionInfo {
    type Error = Error;

    fn try_from(
        partition_info_pb: meta_pb::PartitionInfo,
    ) -> std::result::Result<Self, Self::Error> {
        match partition_info_pb.partition_info_enum {
            Some(partition_info_enum) => match partition_info_enum {
                PartitionInfoEnum::Hash(v) => {
                    let hash_partition_info = HashPartitionInfo::from(v);
                    Ok(Self::Hash(hash_partition_info))
                }
                PartitionInfoEnum::Key(v) => {
                    let key_partition_info = KeyPartitionInfo::from(v);
                    Ok(Self::Key(key_partition_info))
                }
            },
            None => Err(Error::EmptyPartitionInfo {}),
        }
    }
}

pub fn format_sub_partition_table_name(table_name: &str, partition_name: &str) -> String {
    format!(
        "{}{}_{}",
        PARTITION_TABLE_PREFIX, table_name, partition_name
    )
}

/// Encoder for partition info with version control.
pub struct PartitionInfoEncoder {
    version: u8,
}

impl Default for PartitionInfoEncoder {
    fn default() -> Self {
        Self::new(DEFAULT_PARTITION_INFO_ENCODING_VERSION)
    }
}

impl PartitionInfoEncoder {
    fn new(version: u8) -> Self {
        Self { version }
    }

    pub fn encode(&self, partition_info: PartitionInfo) -> Result<Vec<u8>> {
        let pb_partition_info = meta_pb::PartitionInfo::from(partition_info);
        let mut buf = Vec::with_capacity(1 + pb_partition_info.encoded_len() as usize);
        buf.push(self.version);

        pb_partition_info
            .encode(&mut buf)
            .context(EncodePartitionInfoToPb)?;

        Ok(buf)
    }

    pub fn decode(&self, buf: &[u8]) -> Result<Option<PartitionInfo>> {
        if buf.is_empty() {
            return Ok(None);
        }

        self.ensure_version(buf[0])?;

        let pb_partition_info =
            meta_pb::PartitionInfo::decode(&buf[1..]).context(DecodePartitionInfoToPb { buf })?;

        Ok(Some(PartitionInfo::try_from(pb_partition_info)?))
    }

    fn ensure_version(&self, version: u8) -> Result<()> {
        ensure!(
            self.version == version,
            InvalidPartitionInfoEncodingVersion { version }
        );
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::partition::{
        rule::key::DEFAULT_PARTITION_VERSION, KeyPartitionInfo, PartitionDefinition, PartitionInfo,
        PartitionInfoEncoder,
    };

    #[test]
    fn test_partition_info_encoder() {
        let partition_info = PartitionInfo::Key(KeyPartitionInfo {
            version: DEFAULT_PARTITION_VERSION,
            partition_definitions: vec![
                PartitionDefinition {
                    name: "p0".to_string(),
                    origin_name: Some("partition_0".to_string()),
                },
                PartitionDefinition {
                    name: "p1".to_string(),
                    origin_name: None,
                },
            ],
            partition_key: vec!["col1".to_string(), "col2".to_string(), "col3".to_string()],
            linear: false,
        });
        let partition_info_encoder = PartitionInfoEncoder::default();
        let encode_partition_info = partition_info_encoder
            .encode(partition_info.clone())
            .unwrap();
        let decode_partition_info = partition_info_encoder
            .decode(&encode_partition_info)
            .unwrap()
            .unwrap();

        assert_eq!(decode_partition_info, partition_info);
    }
}
