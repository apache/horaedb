// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Cluster sub-crate includes serval functionalities for supporting CeresDB
//! server to running in the distribute mode. Including:
//! - Request CeresMeta for reading topology or configuration.
//! - Accept CeresMeta's commands like open/close shard or create/drop table
//!   etc.
//!
//! The core types are [Cluster] trait and its implementation [ClusterImpl].

use std::sync::Arc;

use async_trait::async_trait;
use ceresdbproto::meta_event::{
    CloseShardRequest, CreateTableOnShardRequest, DropTableOnShardRequest, OpenShardRequest,
};
use common_types::schema::SchemaName;
use common_util::define_result;
use meta_client::types::{
    ClusterNodesRef, RouteTablesRequest, RouteTablesResponse, ShardId, ShardInfo, TablesOfShard,
};
use snafu::{Backtrace, Snafu};

pub mod cluster_impl;
pub mod config;
pub mod shard_tables_cache;
// FIXME: Remove this lint ignore derive when topology about schema tables is
// finished.
#[allow(dead_code)]
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
        "Fail to open shard, shard_id:{}, msg:{}.\nBacktrace:\n{}",
        shard_id,
        msg,
        backtrace
    ))]
    OpenShard {
        shard_id: ShardId,
        msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Fail to open shard, source:{}.", source))]
    OpenShardWithCause {
        shard_id: ShardId,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Shard not found, msg:{}.\nBacktrace:\n{}", msg, backtrace))]
    ShardNotFound { msg: String, backtrace: Backtrace },

    #[snafu(display("Table not found, msg:{}.\nBacktrace:\n{}", msg, backtrace))]
    TableNotFound { msg: String, backtrace: Backtrace },

    #[snafu(display(
        "Schema not found in current node, schema name:{}.\nBacktrace:\n{}",
        schema_name,
        backtrace
    ))]
    SchemaNotFound {
        schema_name: SchemaName,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Shard version mismatch, shard_info:{:?}, expect version:{}.\nBacktrace:\n{}",
        shard_info,
        expect_version,
        backtrace
    ))]
    ShardVersionMismatch {
        shard_info: ShardInfo,
        expect_version: u64,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Cluster nodes are not found in the topology, version:{}.\nBacktrace:\n{}",
        version,
        backtrace
    ))]
    ClusterNodesNotFound { version: u64, backtrace: Backtrace },
}

define_result!(Error);

pub type ClusterRef = Arc<dyn Cluster + Send + Sync>;

#[derive(Clone, Debug)]
pub struct ClusterNodesResp {
    pub cluster_topology_version: u64,
    pub cluster_nodes: ClusterNodesRef,
}

/// Cluster manages tables and shard infos in cluster mode.
#[async_trait]
pub trait Cluster {
    async fn start(&self) -> Result<()>;
    async fn stop(&self) -> Result<()>;
    async fn open_shard(&self, req: &OpenShardRequest) -> Result<TablesOfShard>;
    async fn close_shard(&self, req: &CloseShardRequest) -> Result<TablesOfShard>;
    async fn create_table_on_shard(&self, req: &CreateTableOnShardRequest) -> Result<()>;
    async fn drop_table_on_shard(&self, req: &DropTableOnShardRequest) -> Result<()>;
    async fn route_tables(&self, req: &RouteTablesRequest) -> Result<RouteTablesResponse>;
    async fn fetch_nodes(&self) -> Result<ClusterNodesResp>;
}
