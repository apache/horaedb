// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// Config for ceresdb server.

use cluster::config::ClusterConfig;
use serde::Deserialize;
use server::{
    config::{ServerConfig, StaticRouteConfig},
    limiter::LimiterConfig,
};

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct NodeInfo {
    pub addr: String,
    pub zone: String,
    pub idc: String,
    pub binary_version: String,
}

impl Default for NodeInfo {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1".to_string(),
            zone: "".to_string(),
            idc: "".to_string(),
            binary_version: "".to_string(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// The information of the host node.
    pub node: NodeInfo,

    /// Config for service of server, including http, mysql and grpc.
    pub server: ServerConfig,

    /// Runtime config.
    pub runtime: RuntimeConfig,

    /// Logger config.
    pub logger: logger::Config,

    /// Tracing config.
    pub tracing: tracing_util::Config,

    /// Analytic engine config.
    pub analytic: analytic_engine::Config,

    /// Query engine config.
    pub query_engine: query_engine::Config,

    /// The deployment of the server.
    pub cluster_deployment: Option<ClusterDeployment>,

    /// Config of limiter
    pub limiter: LimiterConfig,
}

/// The cluster deployment decides how to deploy the CeresDB cluster.
///
/// [ClusterDeployment::NoMeta] means to start one or multiple CeresDB
/// instance(s) without CeresMeta.
///
/// [ClusterDeployment::WithMeta] means to start one or multiple CeresDB
/// instance(s) under the control of CeresMeta.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "mode")]
pub enum ClusterDeployment {
    NoMeta(StaticRouteConfig),
    WithMeta(ClusterConfig),
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
