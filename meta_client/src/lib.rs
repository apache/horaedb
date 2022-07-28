// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Client to communicate with meta

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use common_types::schema::TIMESTAMP_COLUMN;
use common_util::define_result;
use serde_derive::Deserialize;
use snafu::{Backtrace, Snafu};
use table_engine::ANALYTIC_ENGINE_TYPE;

use crate::static_client::StaticMetaClient;

mod load_balance;
mod static_client;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(display(
        "Invalid node addr of cluster view, node:{}.\nBacktrace:\n{}",
        node,
        backtrace
    ))]
    InvalidNodeAddr { node: String, backtrace: Backtrace },

    #[snafu(display(
        "Invalid node port of cluster view, node:{}, err:{}.\nBacktrace:\n{}",
        node,
        source,
        backtrace
    ))]
    InvalidNodePort {
        node: String,
        source: std::num::ParseIntError,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to create schema:{}, catalog:{}, err:{}",
        schema,
        catalog,
        source
    ))]
    FailOnChangeView {
        schema: String,
        catalog: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to get catalog:{}, err:{}", catalog, source))]
    FailGetCatalog {
        catalog: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

define_result!(Error);

type ShardViewMap = HashMap<ShardId, ShardView>;

#[async_trait]
pub trait MetaWatcher {
    async fn on_change(&self, view: ClusterViewRef) -> Result<()>;
}

pub type MetaWatcherPtr = Box<dyn MetaWatcher + Send + Sync>;

/// Meta client abstraction
#[async_trait]
pub trait MetaClient {
    /// Start the meta client
    async fn start(&self) -> Result<()>;

    /// Get current cluster view.
    ///
    /// The cluster view is updated by background workers periodically
    fn get_cluster_view(&self) -> ClusterViewRef;
}

// TODO(yingwen): Now meta use i32 as shard id, maybe switch to unsigned number
pub type ShardId = i32;

#[derive(Debug, Clone, Deserialize)]
pub struct Node {
    pub addr: String,
    pub port: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ShardView {
    pub shard_id: ShardId,
    pub node: Node,
}

fn default_engine_type() -> String {
    ANALYTIC_ENGINE_TYPE.to_string()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SchemaConfig {
    pub auto_create_tables: bool,
    pub default_engine_type: String,
    pub default_timestamp_column_name: String,
}

impl Default for SchemaConfig {
    fn default() -> Self {
        Self {
            auto_create_tables: false,
            default_engine_type: default_engine_type(),
            default_timestamp_column_name: default_timestamp_column_name(),
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

#[derive(Debug, Default, Clone, Deserialize)]
pub struct ClusterView {
    pub schema_shards: HashMap<String, ShardViewMap>,
    pub schema_configs: HashMap<String, SchemaConfig>,
}

pub type ClusterViewRef = Arc<ClusterView>;

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct MetaClientConfig {
    pub cluster: String,
    /// Local ip address of this node, used as endpoint ip in meta.
    pub node: String,
    /// Grpc port of this node, also used as endpoint port in meta.
    pub port: u16,
    /// The static cluster view used by static meta client.
    pub cluster_view: ClusterViewConfig,
}

impl Default for MetaClientConfig {
    fn default() -> Self {
        Self {
            cluster: String::new(),
            node: String::new(),
            port: 8831,
            cluster_view: ClusterViewConfig {
                schema_shards: Vec::new(),
            },
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct SchemaShardView {
    schema: String,
    auto_create_tables: bool,
    pub default_engine_type: String,
    default_timestamp_column_name: String,
    shard_views: Vec<ShardView>,
}

impl Default for SchemaShardView {
    fn default() -> Self {
        Self {
            schema: "".to_string(),
            auto_create_tables: false,
            default_engine_type: default_engine_type(),
            default_timestamp_column_name: default_timestamp_column_name(),
            shard_views: Vec::default(),
        }
    }
}

#[inline]
fn default_timestamp_column_name() -> String {
    TIMESTAMP_COLUMN.to_string()
}

#[derive(Debug, Deserialize, Clone)]
pub struct ClusterViewConfig {
    schema_shards: Vec<SchemaShardView>,
}

impl ClusterViewConfig {
    pub(crate) fn to_cluster_view(&self) -> ClusterView {
        let mut schema_configs = HashMap::with_capacity(self.schema_shards.len());
        let mut schema_shards = HashMap::with_capacity(self.schema_shards.len());

        for schema_shard_view in self.schema_shards.clone() {
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

/// Create a meta client with given `config`.
pub fn build_meta_client(
    config: MetaClientConfig,
    watcher: Option<MetaWatcherPtr>,
) -> Result<Arc<dyn MetaClient + Send + Sync>> {
    let meta_client = StaticMetaClient::new(config, watcher);
    Ok(Arc::new(meta_client))
}
