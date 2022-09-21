// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Cluster sub-crate includes serval functionalities for supporting CeresDB
//! server to running in the distribute mode. Including:
//! - Catalog / Schema / Table's create, open, close and drop operations.
//! - Request CeresMeta for reading topology or configuration.
//! - Accept CeresMeta's command events like create/drop table etc,.
//!
//! The core types are [Cluster] trait and its implementation [ClusterImpl].

use std::sync::Arc;

use async_trait::async_trait;
use common_util::define_result;
pub use meta_client::types::{
    AllocSchemaIdRequest, AllocSchemaIdResponse, AllocTableIdRequest, AllocTableIdResponse,
    DropTableRequest, GetShardTablesRequest,
};
use meta_client::types::{ClusterNodesRef, RouteTablesRequest, RouteTablesResponse, ShardId};
use snafu::{Backtrace, Snafu};

pub mod cluster_impl;
pub mod config;
pub mod table_manager;
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
        "Shard not found in current node, shard_id:{}.\nBacktrace:\n{}",
        shard_id,
        backtrace
    ))]
    ShardNotFound {
        shard_id: ShardId,
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
    async fn route_tables(&self, req: &RouteTablesRequest) -> Result<RouteTablesResponse>;
    async fn fetch_nodes(&self) -> Result<ClusterNodesResp>;
}
