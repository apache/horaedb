// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Server configs

use std::{collections::HashMap, str::FromStr};

use analytic_engine;
use ceresdbproto::storage;
use cluster::config::{ClusterConfig, SchemaConfig};
use common_types::schema::TIMESTAMP_COLUMN;
use meta_client::types::ShardId;
use serde_derive::Deserialize;
use table_engine::ANALYTIC_ENGINE_TYPE;

use crate::{
    http::DEFAULT_MAX_BODY_SIZE,
    limiter::LimiterConfig,
    route::rule_based::{ClusterView, RuleList},
};

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
    pub rules: RuleList,
    pub topology: StaticTopologyConfig,
}

#[derive(Debug, Clone, Deserialize, Eq, Hash, PartialEq)]
pub struct Endpoint {
    pub addr: String,
    pub port: u16,
}

impl Endpoint {
    pub fn new(addr: String, port: u16) -> Self {
        Self { addr, port }
    }
}

impl ToString for Endpoint {
    fn to_string(&self) -> String {
        format!("{}:{}", self.addr, self.port)
    }
}

impl FromStr for Endpoint {
    type Err = Box<dyn std::error::Error + Send + Sync>;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let (addr, raw_port) = match s.rsplit_once(':') {
            Some(v) => v,
            None => {
                let err_msg = "Can't find ':' in the source string".to_string();
                return Err(Self::Err::from(err_msg));
            }
        };
        let port = raw_port.parse().map_err(|e| {
            let err_msg = format!("Fail to parse port:{}, err:{}", raw_port, e);
            Self::Err::from(err_msg)
        })?;

        Ok(Endpoint {
            addr: addr.to_string(),
            port,
        })
    }
}

impl From<Endpoint> for storage::Endpoint {
    fn from(endpoint: Endpoint) -> Self {
        storage::Endpoint {
            ip: endpoint.addr,
            port: endpoint.port as u32,
        }
    }
}

impl From<storage::Endpoint> for Endpoint {
    fn from(endpoint: storage::Endpoint) -> Self {
        Endpoint {
            addr: endpoint.ip,
            port: endpoint.port as u16,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ShardView {
    pub shard_id: ShardId,
    pub endpoint: Endpoint,
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
    pub schema_shards: Vec<SchemaShardView>,
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
                    .map(|shard| (shard.shard_id, shard.endpoint.clone()))
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
    pub http_max_body_size: u64,
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

    // Analytic engine configs.
    pub analytic: analytic_engine::Config,

    // Query engine config.
    pub query: query_engine::Config,

    // Deployment configs:
    pub deploy_mode: DeployMode,
    pub cluster: ClusterConfig,

    // Config of limiter
    pub limiter: LimiterConfig,
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
            http_max_body_size: DEFAULT_MAX_BODY_SIZE,
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
            query: query_engine::Config::default(),
            analytic: analytic_engine::Config::default(),
            deploy_mode: DeployMode::Standalone,
            cluster: ClusterConfig::default(),
            limiter: LimiterConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_endpoint() {
        let cases = [
            (
                "abc.1234.com:1000",
                Endpoint::new("abc.1234.com".to_string(), 1000),
            ),
            (
                "127.0.0.1:1000",
                Endpoint::new("127.0.0.1".to_string(), 1000),
            ),
            (
                "fe80::dce8:23ff:fe0c:f2c0:1000",
                Endpoint::new("fe80::dce8:23ff:fe0c:f2c0".to_string(), 1000),
            ),
        ];

        for (source, expect) in cases {
            let target: Endpoint = source.parse().expect("Should succeed to parse endpoint");
            assert_eq!(target, expect);
        }
    }

    #[test]
    fn test_parse_invalid_endpoint() {
        let cases = [
            "abc.1234.com:1000000",
            "fe80::dce8:23ff:fe0c:f2c0",
            "127.0.0.1",
            "abc.1234.com",
            "abc.1234.com:abcd",
        ];

        for source in cases {
            assert!(source.parse::<Endpoint>().is_err());
        }
    }
}
