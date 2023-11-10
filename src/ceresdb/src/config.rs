// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Config for ceresdb server.

use cluster::config::ClusterConfig;
use proxy::limiter::LimiterConfig;
use serde::{Deserialize, Serialize};
use server::config::{ServerConfig, StaticRouteConfig};
use size_ext::ReadableSize;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct NodeInfo {
    /// The address of the ceresdb node. It can be a domain name or an IP
    /// address without port followed.
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

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
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
    pub query_engine: query_engine::config::Config,

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
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "mode")]
#[allow(clippy::large_enum_variant)]
pub enum ClusterDeployment {
    NoMeta(StaticRouteConfig),
    WithMeta(ClusterConfig),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct RuntimeConfig {
    /// Runtime for reading data
    pub read_thread_num: usize,
    /// The size of the stack used by the read thread
    ///
    /// The size should be a set as a large number if the complex query exists.
    pub read_thread_stack_size: ReadableSize,
    /// Runtime for writing data
    pub write_thread_num: usize,
    /// Runtime for communicating with meta cluster
    pub meta_thread_num: usize,
    /// Runtime for compaction
    pub compact_thread_num: usize,
    /// Runtime for other tasks which may not important
    pub default_thread_num: usize,
    /// Runtime for io
    pub io_thread_num: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            read_thread_num: 8,
            read_thread_stack_size: ReadableSize::mb(16),
            write_thread_num: 8,
            meta_thread_num: 2,
            compact_thread_num: 4,
            default_thread_num: 8,
            io_thread_num: 4,
        }
    }
}
