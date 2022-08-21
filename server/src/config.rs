// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Server configs

use std::collections::HashMap;

use analytic_engine;
use cluster::{config::ClusterConfig, SchemaConfig};
use common_types::schema::TIMESTAMP_COLUMN;
use serde_derive::Deserialize;
use table_engine::ANALYTIC_ENGINE_TYPE;

use crate::route::rule_based::{ClusterView, RuleList, ShardView};

/// The deployment mode decides how to start the CeresDB.
///
/// [DeployMode::Standalone] means to start one or multiple CeresDB instance(s)
/// alone without CeresMeta.
///
/// [DeployMode::Cluster] means to start one or multiple CeresDB instance(s)
/// under the control of CeresMeta.
#[derive(Debug, Clone, Copy, Deserialize)]
pub enum DeployMode {
    Standalone,
    Cluster,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct RuntimeConfig {
    // Runtime for reading data
    pub read_thread_num: usize,
    // Runtime for writing data
    pub write_thread_num: usize,
    // Runtime for communicating with meta cluster
    pub meta_thread_num: usize,
    // Runtime for background tasks
    pub background_thread_num: usize,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct StaticRouteConfig {
    pub rule_list: RuleList,
    pub topology: StaticTopologyConfig,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct SchemaShardView {
    pub schema: String,
    pub auto_create_tables: bool,
    pub default_engine_type: String,
    pub default_timestamp_column_name: String,
    pub shard_views: Vec<ShardView>,
}

impl Default for SchemaShardView {
    fn default() -> Self {
        Self {
            schema: "".to_string(),
            auto_create_tables: false,
            default_engine_type: ANALYTIC_ENGINE_TYPE.to_string(),
            default_timestamp_column_name: TIMESTAMP_COLUMN.to_string(),
            shard_views: Vec::default(),
        }
    }
}

impl From<SchemaShardView> for SchemaConfig {
    fn from(view: SchemaShardView) -> Self {
        Self {
            auto_create_tables: view.auto_create_tables,
            default_engine_type: view.default_engine_type,
            default_timestamp_column_name: view.default_timestamp_column_name,
        }
    }
}

#[derive(Debug, Default, Deserialize, Clone)]
#[serde(default)]
pub struct StaticTopologyConfig {
    schema_shards: Vec<SchemaShardView>,
}

impl From<&StaticTopologyConfig> for ClusterView {
    fn from(config: &StaticTopologyConfig) -> Self {
        let mut schema_configs = HashMap::with_capacity(config.schema_shards.len());
        let mut schema_shards = HashMap::with_capacity(config.schema_shards.len());

        for schema_shard_view in config.schema_shards.clone() {
            let schema = schema_shard_view.schema.clone();
            schema_shards.insert(
                schema.clone(),
                schema_shard_view
                    .shard_views
                    .iter()
                    .map(|shard| (shard.shard_id, shard.clone()))
                    .collect(),
            );
            schema_configs.insert(schema, SchemaConfig::from(schema_shard_view));
        }
        ClusterView {
            schema_shards,
            schema_configs,
        }
    }
}

// TODO(yingwen): Split config into several sub configs.
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct Config {
    /// The address to listen.
    pub bind_addr: String,
    pub mysql_port: u16,
    pub http_port: u16,
    pub grpc_port: u16,
    pub grpc_server_cq_count: usize,

    // Engine related configs:
    pub runtime: RuntimeConfig,

    // Log related configs:
    pub log_level: String,
    pub enable_async_log: bool,
    pub async_log_channel_len: i32,

    // Tracing related configs:
    pub tracing_log_dir: String,
    pub tracing_log_name: String,
    pub tracing_level: String,

    // Config of static router.
    pub static_route: StaticRouteConfig,

    // Analytic engine configs:
    pub analytic: analytic_engine::Config,

    // Deployment configs:
    pub deploy_mode: DeployMode,
    pub cluster: ClusterConfig,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            read_thread_num: 8,
            write_thread_num: 8,
            meta_thread_num: 2,
            background_thread_num: 8,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        let grpc_port = 8831;
        Self {
            bind_addr: String::from("127.0.0.1"),
            http_port: 5000,
            mysql_port: 3307,
            grpc_port,
            grpc_server_cq_count: 20,
            runtime: RuntimeConfig::default(),
            log_level: "debug".to_string(),
            enable_async_log: true,
            async_log_channel_len: 102400,
            tracing_log_dir: String::from("/tmp/ceresdb"),
            tracing_log_name: String::from("tracing"),
            tracing_level: String::from("info"),
            static_route: StaticRouteConfig::default(),
            analytic: analytic_engine::Config::default(),
            deploy_mode: DeployMode::Standalone,
            cluster: ClusterConfig::default(),
        }
    }
}
