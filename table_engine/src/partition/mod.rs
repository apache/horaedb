// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Partitioned table supports

pub mod rule;

use common_types::bytes::Bytes;
use proto::meta_update as meta_pb;
use snafu::{Backtrace, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
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
        "Failed to locate partitions for write, msg:{}.\nBacktrace:{}\n",
        msg,
        backtrace
    ))]
    LocateReadPartition { msg: String, backtrace: Backtrace },
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

#[derive(Clone, Debug, PartialEq, Eq)]
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

impl From<PartitionInfo> for meta_pb::add_table_meta::PartitionInfo {
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

impl From<meta_pb::add_table_meta::PartitionInfo> for PartitionInfo {
    fn from(pb: meta_pb::add_table_meta::PartitionInfo) -> Self {
        match pb {
            meta_pb::add_table_meta::PartitionInfo::Hash(v) => Self::Hash(HashPartitionInfo {
                definitions: v.definitions.into_iter().map(|v| v.into()).collect(),
                expr: Bytes::from(v.expr),
                linear: v.linear,
            }),
            meta_pb::add_table_meta::PartitionInfo::KeyPartition(v) => {
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
