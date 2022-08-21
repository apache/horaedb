// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use common_types::schema::TIMESTAMP_COLUMN;
use common_util::define_result;
pub use meta_client::types::{
    AllocSchemaIdRequest, AllocSchemaIdResponse, AllocTableIdRequest, AllocTableIdResponse,
    DropTableRequest, GetTablesRequest,
};
use meta_client::types::{ShardId, ShardInfo, TableId};
use serde::Deserialize;
use snafu::{Backtrace, Snafu};
use table_engine::ANALYTIC_ENGINE_TYPE;

pub mod cluster_impl;
pub mod config;
mod table_manager;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(display("Build meta client failed, err:{}.", source))]
    BuildMetaClient { source: meta_client::Error },

    #[snafu(display("Meta client start failed, err:{}.", source))]
    StartMetaClient { source: meta_client::Error },

    #[snafu(display("Meta client execute failed, err:{}.", source))]
    MetaClientFailure { source: meta_client::Error },

    #[snafu(display(
        "Shard not found in current node, shard_id:{}.\nBacktrace:\n{}",
        shard_id,
        backtrace
    ))]
    ShardNotFound {
        shard_id: ShardId,
        backtrace: Backtrace,
    },
}

define_result!(Error);

pub type TableName = String;
pub type SchemaName = String;

pub type ClusterRef = Arc<dyn Cluster + Send + Sync>;
pub type TableManipulatorRef = Arc<dyn TableManipulator + Send + Sync>;
pub type ClusterTopologyRef = Arc<ClusterTopology>;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SchemaConfig {
    pub auto_create_tables: bool,
    pub default_engine_type: String,
    pub default_timestamp_column_name: String,
}

impl Default for SchemaConfig {
    fn default() -> Self {
        Self {
            auto_create_tables: false,
            default_engine_type: ANALYTIC_ENGINE_TYPE.to_string(),
            default_timestamp_column_name: TIMESTAMP_COLUMN.to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Node {
    pub addr: String,
    pub port: u16,
}

#[derive(Debug, Clone)]
pub struct TableNodeShards {
    pub table_id: TableId,
    pub node_shards: Vec<NodeShard>,
}

#[derive(Debug, Clone)]
pub struct NodeShard {
    pub shard: ShardInfo,
    pub node: Node,
}

#[derive(Clone, Debug, Default)]
pub struct ClusterTopology {
    pub schema_tables: HashMap<SchemaName, HashMap<TableName, TableNodeShards>>,
    pub schema_configs: HashMap<SchemaName, SchemaConfig>,
}

#[async_trait]
pub trait TableManipulator {
    async fn open_table(
        &self,
        schema_name: &str,
        table_name: &str,
        table_id: TableId,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn close_table(
        &self,
        schema_name: &str,
        table_name: &str,
        table_id: TableId,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// Cluster manages tables and shard infos in cluster mode.
#[async_trait]
pub trait Cluster {
    async fn start(&self) -> Result<()>;
    async fn stop(&self) -> Result<()>;
    async fn fetch_topology(&self) -> Result<ClusterTopologyRef>;
}
