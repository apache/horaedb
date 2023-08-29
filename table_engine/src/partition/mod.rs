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

//! Partitioned table supports

pub mod rule;

use bytes_ext::Bytes;
use ceresdbproto::cluster::partition_info::Info;
use common_types::schema::Schema;
use datafusion::{error::DataFusionError, prelude::Expr};
use generic_error::{BoxError, GenericError};
use macros::define_result;
use snafu::{Backtrace, ResultExt, Snafu};

use crate::{partition::rule::df_adapter::DfPartitionRuleAdapter, remote::model::TableIdentifier};

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

    #[snafu(display("Failed to build partitions, msg:{msg}, err:{source}\n"))]
    BuildPartitionsWithCause { msg: String, source: GenericError },

    #[snafu(display("Failed to build partitions, msg:{msg}.\nBacktrace:{backtrace}\n"))]
    BuildPartitionsNoCause { msg: String, backtrace: Backtrace },
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
            Self::Hash(v) => v.definitions.clone(),
            Self::Key(v) => v.definitions.clone(),
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
    pub definitions: Vec<PartitionDefinition>,
    pub expr: Bytes,
    pub linear: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyPartitionInfo {
    pub version: i32,
    pub definitions: Vec<PartitionDefinition>,
    pub partition_key: Vec<String>,
    pub linear: bool,
}

impl From<PartitionDefinition> for ceresdbproto::cluster::PartitionDefinition {
    fn from(definition: PartitionDefinition) -> Self {
        Self {
            name: definition.name,
            origin_name: definition
                .origin_name
                .map(ceresdbproto::cluster::partition_definition::OriginName::Origin),
        }
    }
}

impl From<ceresdbproto::cluster::PartitionDefinition> for PartitionDefinition {
    fn from(pb: ceresdbproto::cluster::PartitionDefinition) -> Self {
        let mut origin_name = None;
        if let Some(v) = pb.origin_name {
            match v {
                ceresdbproto::cluster::partition_definition::OriginName::Origin(name) => {
                    origin_name = Some(name)
                }
            }
        }
        Self {
            name: pb.name,
            origin_name,
        }
    }
}

impl From<ceresdbproto::cluster::HashPartitionInfo> for HashPartitionInfo {
    fn from(partition_info_pb: ceresdbproto::cluster::HashPartitionInfo) -> Self {
        HashPartitionInfo {
            version: partition_info_pb.version,
            definitions: partition_info_pb
                .definitions
                .into_iter()
                .map(|v| v.into())
                .collect(),
            expr: Bytes::from(partition_info_pb.expr),
            linear: partition_info_pb.linear,
        }
    }
}

impl From<HashPartitionInfo> for ceresdbproto::cluster::HashPartitionInfo {
    fn from(partition_info: HashPartitionInfo) -> Self {
        ceresdbproto::cluster::HashPartitionInfo {
            version: partition_info.version,
            definitions: partition_info
                .definitions
                .into_iter()
                .map(|v| v.into())
                .collect(),
            expr: Bytes::into(partition_info.expr),
            linear: partition_info.linear,
        }
    }
}

impl From<ceresdbproto::cluster::KeyPartitionInfo> for KeyPartitionInfo {
    fn from(partition_info_pb: ceresdbproto::cluster::KeyPartitionInfo) -> Self {
        KeyPartitionInfo {
            version: partition_info_pb.version,
            definitions: partition_info_pb
                .definitions
                .into_iter()
                .map(|v| v.into())
                .collect(),
            partition_key: partition_info_pb.partition_key,
            linear: partition_info_pb.linear,
        }
    }
}

impl From<KeyPartitionInfo> for ceresdbproto::cluster::KeyPartitionInfo {
    fn from(partition_info: KeyPartitionInfo) -> Self {
        ceresdbproto::cluster::KeyPartitionInfo {
            version: partition_info.version,
            definitions: partition_info
                .definitions
                .into_iter()
                .map(|v| v.into())
                .collect(),
            partition_key: partition_info.partition_key,
            linear: partition_info.linear,
        }
    }
}

impl From<PartitionInfo> for ceresdbproto::cluster::PartitionInfo {
    fn from(partition_info: PartitionInfo) -> Self {
        match partition_info {
            PartitionInfo::Hash(v) => {
                let hash_partition_info = ceresdbproto::cluster::HashPartitionInfo::from(v);
                ceresdbproto::cluster::PartitionInfo {
                    info: Some(Info::Hash(hash_partition_info)),
                }
            }
            PartitionInfo::Key(v) => {
                let key_partition_info = ceresdbproto::cluster::KeyPartitionInfo::from(v);
                ceresdbproto::cluster::PartitionInfo {
                    info: Some(Info::Key(key_partition_info)),
                }
            }
        }
    }
}

impl TryFrom<ceresdbproto::cluster::PartitionInfo> for PartitionInfo {
    type Error = Error;

    fn try_from(
        partition_info_pb: ceresdbproto::cluster::PartitionInfo,
    ) -> std::result::Result<Self, Self::Error> {
        match partition_info_pb.info {
            Some(info) => match info {
                Info::Hash(v) => {
                    let hash_partition_info = HashPartitionInfo::from(v);
                    Ok(Self::Hash(hash_partition_info))
                }
                Info::Key(v) => {
                    let key_partition_info = KeyPartitionInfo::from(v);
                    Ok(Self::Key(key_partition_info))
                }
            },
            None => Err(Error::EmptyPartitionInfo {}),
        }
    }
}

#[inline]
pub fn format_sub_partition_table_name(table_name: &str, partition_name: &str) -> String {
    format!("{PARTITION_TABLE_PREFIX}{table_name}_{partition_name}")
}

#[inline]
pub fn is_sub_partition_table(table_name: &str) -> bool {
    table_name.starts_with(PARTITION_TABLE_PREFIX)
}

/// Explicit selected partition
#[derive(Debug, Clone)]
pub enum SelectedPartitions {
    /// Represent by id
    Ids(Vec<usize>),
    /// Represent by name
    Names(Vec<String>),
}

pub struct QueryPartitionsBuilder<'a> {
    pub catalog_name: &'a str,
    pub schema_name: &'a str,
    pub table_name: &'a str,
    pub partition_info: &'a PartitionInfo,
}

impl<'a> QueryPartitionsBuilder<'a> {
    pub fn new(
        catalog_name: &'a str,
        schema_name: &'a str,
        table_name: &'a str,
        partition_info: &'a PartitionInfo,
    ) -> Self {
        Self {
            catalog_name,
            schema_name,
            table_name,
            partition_info,
        }
    }

    pub fn build(self, mode: BuildPartitionsMode<'a>) -> Result<Vec<TableIdentifier>> {
        let sub_tables = match mode {
            BuildPartitionsMode::Normal(NormalMode { exprs, schema }) => {
                let df_partition_rule =
                    DfPartitionRuleAdapter::new(self.partition_info.clone(), schema)
                        .box_err()
                        .with_context(|| BuildPartitionsWithCause {
                            msg: "failed to build partition rule".to_string(),
                        })?;

                // Evaluate expr and locate partition.
                let partitions = df_partition_rule
                    .locate_partitions_for_read(exprs)
                    .box_err()
                    .with_context(|| BuildPartitionsWithCause {
                        msg: "failed to locate partition for read".to_string(),
                    })?;

                self.build_sub_table_idents_from_ids(
                    &self.table_name,
                    self.partition_info,
                    &partitions,
                )
            }
            BuildPartitionsMode::Explicit(ExplicitMode {
                selected_partitions,
            }) => match selected_partitions {
                SelectedPartitions::Ids(ids) => {
                    self.build_sub_table_idents_from_ids(&self.table_name, self.partition_info, ids)
                }
                SelectedPartitions::Names(_) => {
                    unimplemented!()
                }
            },
        };

        Ok(sub_tables)
    }

    fn build_sub_table_idents_from_ids(
        &self,
        table_name: &str,
        partition_info: &PartitionInfo,
        partition_ids: &[usize],
    ) -> Vec<TableIdentifier> {
        let definitions = partition_info.get_definitions();
        partition_ids
            .into_iter()
            .map(|p| {
                let partition_name = &definitions[*p].name;
                TableIdentifier {
                    catalog: self.catalog_name.to_string(),
                    schema: self.schema_name.to_string(),
                    table: format_sub_partition_table_name(table_name, partition_name),
                }
            })
            .collect()
    }
}

pub enum BuildPartitionsMode<'a> {
    Normal(NormalMode<'a>),
    Explicit(ExplicitMode<'a>),
}

pub struct NormalMode<'a> {
    pub exprs: &'a [Expr],
    pub schema: &'a Schema,
}

pub struct ExplicitMode<'a> {
    pub selected_partitions: &'a SelectedPartitions,
}
