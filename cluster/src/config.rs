// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use common_types::schema::TIMESTAMP_COLUMN;
use common_util::config::ReadableDuration;
use etcd_client::ConnectOptions;
use meta_client::meta_impl::MetaClientConfig;
use serde::{Deserialize, Serialize};
use table_engine::ANALYTIC_ENGINE_TYPE;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
// TODO: move this to table_engine crates
pub struct SchemaConfig {
    pub default_engine_type: String,
    pub default_timestamp_column_name: String,
}

impl Default for SchemaConfig {
    fn default() -> Self {
        Self {
            default_engine_type: ANALYTIC_ENGINE_TYPE.to_string(),
            default_timestamp_column_name: TIMESTAMP_COLUMN.to_string(),
        }
    }
}

const DEFAULT_ETCD_ROOT_PATH: &str = "/ceresdb";

#[derive(Clone, Deserialize, Debug, Serialize)]
#[serde(default)]
pub struct EtcdClientConfig {
    /// The etcd server addresses
    pub server_addrs: Vec<String>,
    /// Root path in the etcd used by the ceresdb server
    pub root_path: String,

    /// Timeout to connect to etcd cluster
    pub connect_timeout: ReadableDuration,
    /// Timeout for each rpc request
    pub rpc_timeout: ReadableDuration,

    /// The lease of the shard lock in seconds.
    ///
    /// It should be greater than `shard_lock_lease_check_interval`.
    pub shard_lock_lease_ttl_sec: u64,
    /// The interval of checking whether the shard lock lease is expired
    pub shard_lock_lease_check_interval: ReadableDuration,
}

impl From<&EtcdClientConfig> for ConnectOptions {
    fn from(config: &EtcdClientConfig) -> Self {
        ConnectOptions::default()
            .with_connect_timeout(config.connect_timeout.0)
            .with_timeout(config.rpc_timeout.0)
    }
}

impl Default for EtcdClientConfig {
    fn default() -> Self {
        Self {
            server_addrs: vec!["127.0.0.1:2379".to_string()],
            root_path: DEFAULT_ETCD_ROOT_PATH.to_string(),

            rpc_timeout: ReadableDuration::secs(5),
            connect_timeout: ReadableDuration::secs(5),
            shard_lock_lease_ttl_sec: 15,
            shard_lock_lease_check_interval: ReadableDuration::millis(200),
        }
    }
}

#[derive(Default, Clone, Deserialize, Debug, Serialize)]
#[serde(default)]
pub struct ClusterConfig {
    pub cmd_channel_buffer_size: usize,
    pub meta_client: MetaClientConfig,
    pub etcd_client: EtcdClientConfig,
}
