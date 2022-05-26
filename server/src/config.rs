// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Server configs

use analytic_engine;
use meta_client::MetaClientConfig;
use serde_derive::Deserialize;

use crate::router::RuleList;

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct RuntimeConfig {
    // Runtime for reading data
    pub read_thread_num: usize,
    // Runtime for writing data
    pub write_thread_num: usize,
    // Runtime for background tasks
    pub background_thread_num: usize,
}

// TODO(yingwen): Split config into several sub configs.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct Config {
    /// The address to listen.
    pub bind_addr: String,
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

    // Meta client related configs:
    pub meta_client: MetaClientConfig,
    // Config of router.
    pub route_rules: RuleList,

    // Analytic engine configs:
    pub analytic: analytic_engine::Config,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            read_thread_num: 8,
            write_thread_num: 8,
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
            grpc_port,
            grpc_server_cq_count: 20,
            runtime: RuntimeConfig::default(),
            log_level: "debug".to_string(),
            enable_async_log: true,
            async_log_channel_len: 102400,
            tracing_log_dir: String::from("/tmp/ceresdbx"),
            tracing_log_name: String::from("tracing"),
            tracing_level: String::from("info"),
            meta_client: MetaClientConfig {
                node: String::from("127.0.0.1"),
                port: grpc_port,
                ..Default::default()
            },
            route_rules: RuleList::default(),
            analytic: analytic_engine::Config::default(),
        }
    }
}
