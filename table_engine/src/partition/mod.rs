// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Partitioned table supports

pub mod rule;

use proto::meta_update as meta_pb;

/// Info for how to partition table
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PartitionInfo {
    Hash(HashPartitionInfo),
}

impl PartitionInfo {
    pub fn get_definitions(&self) -> Vec<Definition> {
        match self {
            Self::Hash(v) => v.definitions.clone(),
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
    pub columns: Vec<String>,
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
                columns: v.columns,
            }),
        }
    }
}

impl From<meta_pb::add_table_meta::PartitionInfo> for PartitionInfo {
    fn from(pb: meta_pb::add_table_meta::PartitionInfo) -> Self {
        match pb {
            meta_pb::add_table_meta::PartitionInfo::Hash(v) => Self::Hash(HashPartitionInfo {
                definitions: v.definitions.into_iter().map(|v| v.into()).collect(),
                columns: v.columns,
            }),
        }
    }
}

pub fn format_sub_partition_table_name(table_name: &str, partition_name: &str) -> String {
    format!("__{}_{}", table_name, partition_name)
}
