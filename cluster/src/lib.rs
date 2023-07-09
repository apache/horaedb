// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Cluster sub-crate includes serval functionalities for supporting CeresDB
//! server to running in the distribute mode. Including:
//! - Request CeresMeta for reading topology or configuration.
//! - Accept CeresMeta's commands like open/close shard or create/drop table
//!   etc.
//!
//! The core types are [Cluster] trait and its implementation [ClusterImpl].

#![feature(trait_alias)]

use std::sync::Arc;

use async_trait::async_trait;
use common_types::schema::SchemaName;
use common_util::{define_result, error::GenericError};
use meta_client::types::{
    ClusterNodesRef, RouteTablesRequest, RouteTablesResponse, ShardId, ShardInfo, ShardVersion,
};
use shard_lock_manager::ShardLockManagerRef;
use snafu::{Backtrace, Snafu};

use crate::shard_set::ShardRef;

pub mod cluster_impl;
pub mod config;
pub mod shard_lock_manager;
pub mod shard_set;
#[allow(dead_code)]
pub mod topology;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(display("Invalid arguments, msg:{msg}.\nBacktrace:{backtrace}"))]
    InvalidArguments { msg: String, backtrace: Backtrace },

    #[snafu(display("Internal error, msg:{msg}, err:{source}"))]
    Internal { msg: String, source: GenericError },

    #[snafu(display("Build meta client failed, err:{source}."))]
    BuildMetaClient { source: meta_client::Error },

    #[snafu(display("Meta client start failed, err:{source}."))]
    StartMetaClient { source: meta_client::Error },

    #[snafu(display("Meta client execute failed, err:{source}."))]
    MetaClientFailure { source: meta_client::Error },

    #[snafu(display("Etcd client failure, msg:{msg}, err:{source}.\nBacktrace:\n{backtrace}"))]
    EtcdClientFailureWithCause {
        msg: String,
        source: etcd_client::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Fail to open shard, shard_id:{shard_id}, msg:{msg}.\nBacktrace:\n{backtrace}",
    ))]
    OpenShard {
        shard_id: ShardId,
        msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Fail to open shard, shard_id:{shard_id}, source:{source}."))]
    OpenShardWithCause {
        shard_id: ShardId,
        source: GenericError,
    },

    #[snafu(display("Fail to close shard, shard_id:{shard_id}, source:{source}."))]
    CloseShardWithCause {
        shard_id: ShardId,
        source: GenericError,
    },

    #[snafu(display("Shard not found, msg:{msg}.\nBacktrace:\n{backtrace}"))]
    ShardNotFound { msg: String, backtrace: Backtrace },

    #[snafu(display("Table not found, msg:{msg}.\nBacktrace:\n{backtrace}"))]
    TableNotFound { msg: String, backtrace: Backtrace },

    #[snafu(display("Table already exists, msg:{msg}.\nBacktrace:\n{backtrace}"))]
    TableAlreadyExists { msg: String, backtrace: Backtrace },

    #[snafu(display(
        "Schema not found in current node, schema name:{schema_name}.\nBacktrace:\n{backtrace}",
    ))]
    SchemaNotFound {
        schema_name: SchemaName,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Shard version mismatch, shard_info:{shard_info:?}, expect version:{expect_version}.\nBacktrace:\n{backtrace}",
    ))]
    ShardVersionMismatch {
        shard_info: ShardInfo,
        expect_version: ShardVersion,
        backtrace: Backtrace,
    },

    #[snafu(display("Update on a frozen shard, shard_id:{shard_id}.\nBacktrace:\n{backtrace}",))]
    UpdateFrozenShard {
        shard_id: ShardId,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Cluster nodes are not found in the topology, version:{version}.\nBacktrace:\n{backtrace}",
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

    /// Fetch related information and open shard.
    async fn open_shard(&self, shard_info: &ShardInfo) -> Result<ShardRef>;

    /// Get shard.
    ///
    /// If target shard has opened in cluster, return it. Otherwise, return
    /// None.
    fn get_shard(&self, shard_id: ShardId) -> Option<ShardRef>;

    /// Close shard.
    ///
    /// Return error if the shard is not found.
    async fn close_shard(&self, shard_id: ShardId) -> Result<ShardRef>;

    async fn route_tables(&self, req: &RouteTablesRequest) -> Result<RouteTablesResponse>;
    async fn fetch_nodes(&self) -> Result<ClusterNodesResp>;
    fn shard_lock_manager(&self) -> ShardLockManagerRef;
}
