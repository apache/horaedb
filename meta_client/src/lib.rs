// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Client to communicate with meta

use std::{
    collections::HashMap,
    convert::TryFrom,
    sync::{Arc, RwLock},
    time::Duration,
};

use async_trait::async_trait;
use ceresdbproto::{
    meta::{CommonNodeInfo, NodeType},
    metagrpc::{
        ClusterViewResponse, FetchClusterViewRequest, NameSpace, RegisterNodeRequest,
        RegisterNodeResponse,
    },
    metagrpc_grpc::CeresmetaRpcServiceClient,
};
use common_types::{bytes::Bytes, schema::TIMESTAMP_COLUMN};
use common_util::{config::ReadableDuration, define_result, runtime::Runtime};
use futures::TryStreamExt;
use grpcio::{ChannelBuilder, Environment};
use load_balance::{LoadBalancer, RandomLoadBalancer};
use log::{error, info};
use reqwest::{self, StatusCode, Url};
use serde::de::DeserializeOwned;
use serde_derive::Deserialize;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use table_engine::ANALYTIC_ENGINE_TYPE;
use tokio::time;

use crate::static_client::StaticMetaClient;

mod load_balance;
mod static_client;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(display("Build http client failed, err:{}.\nBacktrace:\n{}", source, backtrace))]
    BuildHttpClient {
        source: reqwest::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Invalid meta addr, addr:{}, err:{}.\nBacktrace:\n{}",
        meta_addr,
        source,
        backtrace
    ))]
    InvalidMetaAddr {
        meta_addr: String,
        source: url::ParseError,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to join url, input:{}, err:{}.\nBacktrace:\n{}",
        input,
        source,
        backtrace
    ))]
    JoinUrl {
        input: String,
        source: url::ParseError,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to send http request, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    SendHttp {
        source: reqwest::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to parse http text, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    ParseText {
        source: reqwest::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Bad http status, status:{}, url:{}, text:{:?}.\nBacktrace:\n{}",
        status,
        url,
        text,
        backtrace
    ))]
    BadHttpStatus {
        status: StatusCode,
        url: String,
        text: Bytes,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to parse json text, text:{:?}, err:{}.\nBacktrace:\n{}",
        text,
        source,
        backtrace
    ))]
    ParseJson {
        text: Bytes,
        source: serde_json::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to fetch cluster view, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    FetchClusterViewError {
        source: grpcio::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Encountered register node, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    RegisterNodeError {
        source: grpcio::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Encountered build rpc client, err:{}", source))]
    BuildRpcClientError { source: load_balance::Error },

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

impl TryFrom<ClusterViewResponse> for ClusterView {
    type Error = Error;

    fn try_from(result: ClusterViewResponse) -> Result<ClusterView> {
        let mut schema_shards = HashMap::with_capacity(result.schema_shards.len());
        let mut schema_configs = HashMap::with_capacity(result.schema_shards.len());

        for (schema, shard_view) in result.schema_shards {
            let mut schema_view = HashMap::with_capacity(shard_view.shard_nodes.len());
            for (shard_id, shard_node) in shard_view.shard_nodes {
                let mut addr_port = shard_node.split(':');
                let addr = addr_port
                    .next()
                    .context(InvalidNodeAddr { node: &shard_node })?;
                let port = addr_port
                    .next()
                    .context(InvalidNodeAddr { node: &shard_node })?
                    .parse()
                    .context(InvalidNodePort { node: &shard_node })?;
                let node = Node {
                    addr: addr.to_string(),
                    port,
                };
                schema_view.insert(shard_id, ShardView { shard_id, node });
            }
            schema_shards.insert(schema.clone(), schema_view);
            // TODO(boyan) support config in ClusterViewResponse
            schema_configs.insert(schema, SchemaConfig::default());
        }

        Ok(ClusterView {
            schema_shards,
            schema_configs,
        })
    }
}

pub type ClusterViewRef = Arc<ClusterView>;

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct MetaClientConfig {
    pub cluster: String,
    pub meta_addr: String,
    pub meta_version: String,
    /// Local ip address of this node, used as endpoint ip in meta.
    pub node: String,
    /// Grpc port of this node, also used as endpoint port in meta.
    pub port: u16,
    pub meta_members_url: String,
    pub lease: ReadableDuration,
    pub timeout: ReadableDuration,
    pub cq_count: usize,
    ///
    /// - If `enable_meta` is true, the client will fetch cluster view from
    ///   remote meta ndoe.
    /// - If `enable_meta` is false, the client will try to read cluster view
    ///   from `cluster_view`.
    pub enable_meta: bool,
    /// The static cluster view used by static meta client.
    pub cluster_view: ClusterViewConfig,
}

impl Default for MetaClientConfig {
    fn default() -> Self {
        Self {
            cluster: String::new(),
            meta_addr: "http://127.0.0.1:8080".to_string(),
            meta_version: String::from("v1"),
            node: String::new(),
            port: 8831,
            meta_members_url: "ceresmeta/members".to_string(),
            lease: ReadableDuration::secs(10),
            timeout: ReadableDuration::secs(5),
            cq_count: 8,
            enable_meta: false,
            cluster_view: ClusterViewConfig {
                schema_shards: Vec::new(),
            },
        }
    }
}

impl From<&MetaClientConfig> for RegisterNodeRequest {
    fn from(meta_config: &MetaClientConfig) -> Self {
        let mut req = RegisterNodeRequest::new();
        req.set_node_type(NodeType::Data);
        req.set_ns(NameSpace {
            cluster: meta_config.cluster.to_string(),
            version: meta_config.meta_version.to_string(),
            ..Default::default()
        });
        req.set_node_info(CommonNodeInfo {
            node: format!("{}:{}", meta_config.node, meta_config.port),
            lease: meta_config.lease.as_secs() as i32,
            ..Default::default()
        });
        req
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

struct MetaClientImplInner {
    meta_grpc_address: RwLock<Vec<String>>,
    http_client: reqwest::Client,
    balancer: Box<dyn LoadBalancer + Send + Sync>,
    meta_config: MetaClientConfig,
    cluster_view: RwLock<ClusterViewRef>,
    members_url: Url,
    watcher: Option<MetaWatcherPtr>,
}

impl MetaClientImplInner {
    fn new(meta_config: MetaClientConfig, watcher: Option<MetaWatcherPtr>) -> Result<Self> {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from(meta_config.timeout))
            .build()
            .context(BuildHttpClient)?;

        let members_url = Url::parse(&meta_config.meta_addr)
            .context(InvalidMetaAddr {
                meta_addr: &meta_config.meta_addr,
            })?
            .join(format!("{}/", meta_config.meta_version).as_str())
            .context(JoinUrl {
                input: &meta_config.meta_version,
            })?
            .join(&meta_config.meta_members_url)
            .context(JoinUrl {
                input: &meta_config.meta_members_url,
            })?;

        Ok(Self {
            meta_grpc_address: RwLock::new(Vec::new()),
            http_client,
            balancer: Box::new(RandomLoadBalancer),
            meta_config,
            cluster_view: RwLock::new(Arc::new(ClusterView::default())),
            members_url,
            watcher,
        })
    }

    async fn fetch_cluster_view(&self) -> Result<()> {
        let client = self.build_rpc_client()?;
        let mut req = FetchClusterViewRequest::new();
        req.set_ns(NameSpace {
            cluster: self.meta_config.cluster.to_string(),
            version: self.meta_config.meta_version.to_string(),
            ..Default::default()
        });
        let mut receiver = client
            .fetch_cluster_view(&req)
            .context(FetchClusterViewError)?;

        while let Some(result) = receiver.try_next().await.context(FetchClusterViewError)? {
            self.update_cluster_view_by_result(result).await?;

            info!(
                "Fetch cluster view from meta, cluster:{}, view:{:#?}",
                self.meta_config.cluster,
                *self.cluster_view.read().unwrap(),
            );
        }

        Ok(())
    }

    async fn update_cluster_view_by_result(&self, view_result: ClusterViewResponse) -> Result<()> {
        let view = Arc::new(ClusterView::try_from(view_result)?);

        {
            let mut cluster_view = self.cluster_view.write().unwrap();
            *cluster_view = view.clone();
        }

        if let Some(w) = &self.watcher {
            w.on_change(view).await?;
        }

        Ok(())
    }

    fn meta_addresses(&self) -> Vec<String> {
        self.meta_grpc_address.read().unwrap().clone()
    }

    fn build_rpc_client(&self) -> Result<CeresmetaRpcServiceClient> {
        let meta_addresses = self.meta_addresses();
        let meta_rpc_addr = self
            .balancer
            .select(&meta_addresses)
            .context(BuildRpcClientError)?;

        let cb = ChannelBuilder::new(Arc::new(Environment::new(self.meta_config.cq_count)));
        Ok(CeresmetaRpcServiceClient::new(cb.connect(meta_rpc_addr)))
    }

    async fn register(&self, client: &CeresmetaRpcServiceClient) -> Result<RegisterNodeResponse> {
        let req = RegisterNodeRequest::from(&self.meta_config);
        client.register_node(&req).context(RegisterNodeError)
    }

    async fn get_bytes_from_url(&self, url: Url) -> Result<Bytes> {
        let resp = self
            .http_client
            .get(self.members_url.clone())
            .send()
            .await
            .context(SendHttp)?;
        let status = resp.status();
        let text = resp.bytes().await.context(ParseText)?;

        if status.is_success() {
            info!(
                "Get bytes from url success, status:{}, url:{}, bytes:{:?}",
                status, url, text
            );

            Ok(text)
        } else {
            error!(
                "Failed to get bytes from url, status:{}, url:{}, bytes:{:?}",
                status, url, text
            );

            BadHttpStatus { status, url, text }.fail()
        }
    }

    async fn get_from_url<T: DeserializeOwned>(&self, url: Url) -> Result<T> {
        let full = self.get_bytes_from_url(url).await?;

        serde_json::from_slice(&full).context(ParseJson { text: full })
    }

    async fn pull_meta_grpc_address(&self) -> Result<()> {
        let addresses: Vec<String> = self.get_from_url(self.members_url.clone()).await?;

        *self.meta_grpc_address.write().unwrap() = addresses;

        Ok(())
    }

    // TODO(yingwen): Store the value in field
    fn error_wait_lease(&self) -> Duration {
        Duration::from_secs(self.meta_config.lease.as_secs() / 2)
    }

    // Register node every 2/3 lease
    fn register_interval(&self) -> Duration {
        Duration::from_secs(self.meta_config.lease.as_secs() * 2 / 3)
    }

    fn fetch_view_interval(&self) -> Duration {
        Duration::from_secs(self.meta_config.lease.as_secs() * 3)
    }

    async fn start_fetch_cluster_view(&self) {
        loop {
            match self.fetch_cluster_view().await {
                Ok(()) => {
                    info!(
                        "Fetch cluster view finished, cluster:{}",
                        self.meta_config.cluster
                    );
                }
                Err(e) => {
                    error!(
                        "Failed to fetch cluster view from meta, cluster:{}, error:{}",
                        self.meta_config.cluster, e
                    );
                }
            }

            time::sleep(self.error_wait_lease()).await;
        }
    }

    async fn register_loop(&self) -> Result<()> {
        let mut interval = time::interval(self.register_interval());
        let rpc_client = self.build_rpc_client()?;

        loop {
            let resp = self.register(&rpc_client).await?;
            info!(
                "Register node successfully, cluster:{}, response:{:#?}",
                self.meta_config.cluster, resp
            );

            interval.tick().await;
        }
    }

    async fn start_register(&self) {
        loop {
            if let Err(e) = self.register_loop().await {
                error!(
                    "Failed to register node to meta, cluster:{}, error:{}",
                    self.meta_config.cluster, e
                );

                time::sleep(self.error_wait_lease()).await;
            }
        }
    }

    async fn start_refresh_meta_addresses(&self) {
        let mut interval = time::interval(self.fetch_view_interval());

        loop {
            match self.pull_meta_grpc_address().await {
                Ok(()) => {
                    interval.tick().await;
                }
                Err(e) => {
                    error!(
                        "Failed to refresh meta addresses from meta, url:{}, error:{}",
                        self.members_url, e
                    );

                    time::sleep(self.error_wait_lease()).await
                }
            }
        }
    }
}

/// Default meta client impl, will interact with a remote meta node.
pub struct MetaClientImpl {
    inner: Arc<MetaClientImplInner>,
    runtime: Arc<Runtime>,
}

impl MetaClientImpl {
    pub fn new(
        config: MetaClientConfig,
        runtime: Arc<Runtime>,
        watcher: Option<MetaWatcherPtr>,
    ) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(MetaClientImplInner::new(config, watcher)?),
            runtime,
        })
    }
}

#[async_trait]
impl MetaClient for MetaClientImpl {
    async fn start(&self) -> Result<()> {
        info!(
            "Meta client is starting, config:{:?}",
            self.inner.meta_config
        );

        self.inner.pull_meta_grpc_address().await?;

        let inner = self.inner.clone();
        self.runtime.spawn(async move {
            inner.start_refresh_meta_addresses().await;
        });

        let inner = self.inner.clone();
        self.runtime.spawn(async move {
            inner.start_register().await;
        });

        let inner = self.inner.clone();
        self.runtime.spawn(async move {
            inner.start_fetch_cluster_view().await;
        });

        info!("Meta client has started");

        Ok(())
    }

    fn get_cluster_view(&self) -> ClusterViewRef {
        self.inner.cluster_view.read().unwrap().clone()
    }
}

/// Create a meta client with given `config`.
pub fn build_meta_client(
    config: MetaClientConfig,
    runtime: Arc<Runtime>,
    watcher: Option<MetaWatcherPtr>,
) -> Result<Arc<dyn MetaClient + Send + Sync>> {
    if config.enable_meta {
        let meta_client = MetaClientImpl::new(config, runtime, watcher)?;
        Ok(Arc::new(meta_client))
    } else {
        let meta_client = StaticMetaClient::new(config, watcher);
        Ok(Arc::new(meta_client))
    }
}
