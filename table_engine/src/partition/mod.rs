// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Partitioned table supports

pub mod rule;

use common_types::bytes::Bytes;
use prost::Message;
use proto::meta_update as meta_pb;
use snafu::{ensure, Backtrace, ResultExt, Snafu};

const DEFAULT_PARTITION_INFO_VERSION: u32 = 1;
const DEFAULT_PARTITION_INFO_ENCODING_VERSION: u8 = 0;

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
    DecodePartitionInfoTOPb {
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
}

define_result!(Error);

/// Info for how to partition table
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PartitionInfo {
    Hash(HashPartitionInfo),
    Key(KeyPartitionInfo),
}

impl PartitionInfo {
    pub fn get_definitions(&self) -> Vec<Definition> {
        match self {
            Self::Hash(v) => v.definitions.clone(),
            Self::Key(v) => v.definitions.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Definition {
    pub name: String,
    pub origin_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HashPartitionInfo {
    pub definitions: Vec<Definition>,
    pub expr: Bytes,
    pub linear: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyPartitionInfo {
    pub definitions: Vec<Definition>,
    pub partition_key: Vec<String>,
    pub linear: bool,
}

impl From<Definition> for meta_pb::Definition {
    fn from(definition: Definition) -> Self {
        Self {
            name: definition.name,
            origin_name: definition
                .origin_name
                .map(meta_pb::definition::OriginName::Origin),
        }
    }
}

impl From<meta_pb::Definition> for Definition {
    fn from(pb: meta_pb::Definition) -> Self {
        let mut origin_name = None;
        if let Some(v) = pb.origin_name {
            match v {
                meta_pb::definition::OriginName::Origin(name) => origin_name = Some(name),
            }
        }
        Self {
            name: pb.name,
            origin_name,
        }
    }
}

impl From<PartitionInfo> for meta_pb::partition_info::PartitionInfoEnum {
    fn from(partition_info: PartitionInfo) -> Self {
        match partition_info {
            PartitionInfo::Hash(v) => Self::Hash(meta_pb::HashPartitionInfo {
                definitions: v.definitions.into_iter().map(|v| v.into()).collect(),
                expr: v.expr.to_vec(),
                linear: v.linear,
            }),
            PartitionInfo::Key(v) => Self::KeyPartition(meta_pb::KeyPartitionInfo {
                definitions: v.definitions.into_iter().map(|v| v.into()).collect(),
                partition_key: v.partition_key,
                linear: v.linear,
            }),
        }
    }
}

impl From<meta_pb::partition_info::PartitionInfoEnum> for PartitionInfo {
    fn from(pb: meta_pb::partition_info::PartitionInfoEnum) -> Self {
        match pb {
            meta_pb::partition_info::PartitionInfoEnum::Hash(v) => Self::Hash(HashPartitionInfo {
                definitions: v.definitions.into_iter().map(|v| v.into()).collect(),
                expr: Bytes::from(v.expr),
                linear: v.linear,
            }),
            meta_pb::partition_info::PartitionInfoEnum::KeyPartition(v) => {
                Self::Key(KeyPartitionInfo {
                    definitions: v.definitions.into_iter().map(|v| v.into()).collect(),
                    partition_key: v.partition_key,
                    linear: v.linear,
                })
            }
        }
    }
}

pub fn format_sub_partition_table_name(table_name: &str, partition_name: &str) -> String {
    format!("____{}_{}", table_name, partition_name)
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
        let pb_partition_info = meta_pb::PartitionInfo {
            partition_info_enum: Some(meta_pb::partition_info::PartitionInfoEnum::from(
                partition_info,
            )),
        };
        let mut buf = Vec::with_capacity(1 + pb_partition_info.encoded_len() as usize);
        buf.push(self.version);

        pb_partition_info
            .encode(&mut buf)
            .context(EncodePartitionInfoToPb)?;

        Ok(buf)
    }

    pub fn decode(&self, buf: &[u8]) -> Result<PartitionInfo> {
        ensure!(!buf.is_empty(), EmptyEncodedPartitionInfo);

        self.ensure_version(buf[0])?;

        let pb_partition_info =
            meta_pb::PartitionInfo::decode(buf).context(DecodePartitionInfoTOPb { buf })?;

        return Ok(PartitionInfo::from(
            pb_partition_info.partition_info_enum.unwrap(),
        ));
    }

    fn ensure_version(&self, version: u8) -> Result<()> {
        ensure!(
            self.version == version,
            InvalidPartitionInfoEncodingVersion { version }
        );
        Ok(())
    }
}
