// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// Config for ceresdb server.

use cluster::config::ClusterConfig;
use serde::Deserialize;
use server::{
    config::{ServerConfig, StaticRouteConfig},
    limiter::LimiterConfig,
};

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct Config {
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
    pub query: query_engine::Config,

    /// Cluster config.
    pub cluster: ClusterConfig,

    /// Config for static route.
    pub static_route: StaticRouteConfig,

    /// Config of limiter
    pub limiter: LimiterConfig,
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
