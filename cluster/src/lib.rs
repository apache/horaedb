// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use async_trait::async_trait;
use common_types::table::TableId;
use common_util::define_result;
pub use meta_client::types::{
    AllocSchemaIdRequest, AllocSchemaIdResponse, AllocTableIdRequest, AllocTableIdResponse,
    DropTableRequest, GetShardTablesRequest,
};
use meta_client::types::{ClusterNodesRef, RouteTablesRequest, RouteTablesResponse, ShardId};
use snafu::{Backtrace, Snafu};

pub mod cluster_impl;
pub mod config;
mod table_manager;
pub mod topology;

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

pub type ClusterRef = Arc<dyn Cluster + Send + Sync>;
pub type TableManipulatorRef = Arc<dyn TableManipulator + Send + Sync>;

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
    async fn route_tables(&self, req: &RouteTablesRequest) -> Result<RouteTablesResponse>;
    async fn fetch_nodes(&self) -> Result<ClusterNodesRef>;
}
