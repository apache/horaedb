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

use std::time::Duration;

use common_types::schema::TIMESTAMP_COLUMN;
use etcd_client::ConnectOptions;
use meta_client::meta_impl::MetaClientConfig;
use serde::{Deserialize, Serialize};
use table_engine::ANALYTIC_ENGINE_TYPE;
use time_ext::ReadableDuration;

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
const MIN_SHARD_LOCK_LEASE_TTL_SEC: u64 = 15;

#[derive(Clone, Deserialize, Debug, Serialize)]
#[serde(default)]
pub struct EtcdClientConfig {
    /// The etcd server addresses
    pub server_addrs: Vec<String>,
    /// Root path in the etcd used by the ceresdb server
    pub root_path: String,

    /// Timeout to connect to etcd cluster
    pub connect_timeout: ReadableDuration,

    /// The lease of the shard lock in seconds.
    ///
    /// It should be greater than `shard_lock_lease_check_interval`.
    /// NOTE: the rpc timeout to the etcd cluster is determined by it.
    pub shard_lock_lease_ttl_sec: u64,
    /// The interval of checking whether the shard lock lease is expired
    pub shard_lock_lease_check_interval: ReadableDuration,
    /// The shard lock can be reacquired in a fast way if set.
    pub enable_shard_lock_fast_reacquire: bool,
}

impl EtcdClientConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.shard_lock_lease_ttl_sec < MIN_SHARD_LOCK_LEASE_TTL_SEC {
            return Err(format!(
                "shard_lock_lease_ttl_sec should be greater than {MIN_SHARD_LOCK_LEASE_TTL_SEC}"
            ));
        }

        if self.shard_lock_lease_check_interval.0
            >= Duration::from_secs(self.shard_lock_lease_ttl_sec)
        {
            return Err(format!(
                "shard_lock_lease_check_interval({}) should be less than shard_lock_lease_ttl_sec({}s)",
                self.shard_lock_lease_check_interval, self.shard_lock_lease_ttl_sec,
            ));
        }

        Ok(())
    }

    pub fn rpc_timeout(&self) -> Duration {
        Duration::from_secs(self.shard_lock_lease_ttl_sec) / 6
    }
}

impl From<&EtcdClientConfig> for ConnectOptions {
    fn from(config: &EtcdClientConfig) -> Self {
        ConnectOptions::default()
            .with_connect_timeout(config.connect_timeout.0)
            .with_timeout(config.rpc_timeout())
    }
}

impl Default for EtcdClientConfig {
    fn default() -> Self {
        Self {
            server_addrs: vec!["127.0.0.1:2379".to_string()],
            root_path: DEFAULT_ETCD_ROOT_PATH.to_string(),
            connect_timeout: ReadableDuration::secs(5),
            shard_lock_lease_ttl_sec: 30,
            shard_lock_lease_check_interval: ReadableDuration::millis(200),
            enable_shard_lock_fast_reacquire: false,
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
