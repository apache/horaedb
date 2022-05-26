// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Client to communicate with meta

use std::{
    sync::{Arc, RwLock as StdRwLock},
    time::Duration,
};

use async_trait::async_trait;
use ceresdbproto::{
    metagrpcV2::{
        AllocSchemaIdRequest as PbAllocSchemaIdRequest,
        AllocTableIdRequest as PbAllocTableIdRequest, DropTableRequest as PbDropTableRequest,
        GetTablesRequest as PbGetTablesRequest, NodeHeartbeatRequest as PbNodeHeartbeatRequest,
        NodeHeartbeatResponse as PbNodeHeartbeatResponse,
    },
    metagrpcV2_grpc::CeresmetaRpcServiceClient,
};
use common_types::bytes::Bytes;
use common_util::{config::ReadableDuration, define_result, runtime::Runtime};
use futures::{SinkExt, TryStreamExt};
use grpcio::{
    CallOption, ChannelBuilder, ClientDuplexReceiver, ClientDuplexSender, Environment, WriteFlags,
};
use load_balance::{LoadBalancer, RandomLoadBalancer};
use log::{error, info, warn};
use reqwest::{self, StatusCode, Url};
use serde::de::DeserializeOwned;
use serde_derive::Deserialize;
use snafu::{Backtrace, ResultExt, Snafu};
use tokio::{
    sync::{mpsc::Sender, RwLock},
    time,
};
pub use types::*;

mod load_balance;
mod types;

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
        "Failed to fetch action cmd, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    FetchActionCmdError {
        source: grpcio::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Encountered build rpc client, err:{}", source))]
    BuildRpcClientError { source: load_balance::Error },

    #[snafu(display("Failed to get grpc client, grpc client is none, msg:{}", msg))]
    FailGetGrpcClient { msg: String },

    #[snafu(display("Failed to send heartbeat, cluster:{}, err:{}", cluster, source))]
    FailSendHeartbeat {
        cluster: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to notify action cmd, action cmd:{:?}, err:{}",
        action_cmd,
        source
    ))]
    FailNotifyActionCmd {
        action_cmd: ActionCmd,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to alloc schema id, err:{}", source))]
    FailAllocSchemaId {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to alloc table id, err:{}", source))]
    FailAllocTableId {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to drop table, err:{}", source))]
    FailDropTable {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to get tables, err:{}", source))]
    FailGetTables {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Meta error, resp header:{:?}.\nBacktrace:\n{}", header, backtrace))]
    Meta {
        header: ResponseHeader,
        backtrace: Backtrace,
    },
}

define_result!(Error);

const DEFAULT_META_URL_VERSION: &str = "v1";

/// Meta client abstraction
#[async_trait]
pub trait MetaClient {
    /// Start the meta client
    async fn start(&self) -> Result<()>;

    async fn alloc_schema_id(&self, _: AllocSchemaIdRequest) -> Result<AllocSchemaIdResponse>;

    async fn alloc_table_id(&self, _: AllocTableIdRequest) -> Result<AllocTableIdResponse>;

    async fn drop_table(&self, _: DropTableRequest) -> Result<DropTableResponse>;

    async fn get_tables(&self, _: GetTablesRequest) -> Result<GetTablesResponse>;

    async fn send_heartbeat(&self, _: Vec<ShardInfo>) -> Result<()>;
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct MetaClientConfig {
    pub cluster_name: String,
    pub meta_addr: String,
    pub meta_members_url: String,
    pub lease: ReadableDuration,
    pub timeout: ReadableDuration,
    pub cq_count: usize,
}

impl Default for MetaClientConfig {
    fn default() -> Self {
        Self {
            cluster_name: String::new(),
            meta_addr: "http://127.0.0.1:8080".to_string(),
            meta_members_url: "ceresmeta/members".to_string(),
            lease: ReadableDuration::secs(10),
            timeout: ReadableDuration::secs(5),
            cq_count: 8,
        }
    }
}

struct NodeHeartbeatChannel {
    heartbeat_sender: ClientDuplexSender<PbNodeHeartbeatRequest>,
    action_cmd_receiver: Option<ClientDuplexReceiver<PbNodeHeartbeatResponse>>,
}

struct GrpcClient {
    client: CeresmetaRpcServiceClient,
    heartbeat_channel: NodeHeartbeatChannel,
}

struct MetaClientImplInner {
    meta_grpc_address: StdRwLock<Vec<String>>,
    http_client: reqwest::Client,
    balancer: Box<dyn LoadBalancer + Send + Sync>,
    meta_config: MetaClientConfig,
    node_meta_info: NodeMetaInfo,
    members_url: Url,

    grpc_client: RwLock<Option<GrpcClient>>,

    notify_sender: Option<Sender<ActionCmd>>,
}

impl MetaClientImplInner {
    fn new(
        meta_config: MetaClientConfig,
        node_meta_info: NodeMetaInfo,
        sender: Option<Sender<ActionCmd>>,
    ) -> Result<Self> {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from(meta_config.timeout))
            .build()
            .context(BuildHttpClient)?;

        let members_url = Url::parse(&meta_config.meta_addr)
            .context(InvalidMetaAddr {
                meta_addr: &meta_config.meta_addr,
            })?
            .join(format!("{}/", DEFAULT_META_URL_VERSION).as_str())
            .unwrap()
            .join(&meta_config.meta_members_url)
            .context(JoinUrl {
                input: &meta_config.meta_members_url,
            })?;

        let client = Self {
            meta_grpc_address: StdRwLock::new(Vec::new()),
            http_client,
            balancer: Box::new(RandomLoadBalancer),
            meta_config,
            node_meta_info,
            members_url,
            grpc_client: RwLock::new(None),
            notify_sender: sender,
        };

        Ok(client)
    }

    fn request_header(&self) -> RequestHeader {
        RequestHeader {
            node: self.node_meta_info.node.to_string(),
            cluster_name: self.meta_config.cluster_name.clone(),
        }
    }

    fn node_meta_info(&self) -> NodeMetaInfo {
        self.node_meta_info.clone()
    }

    fn get_cluster_name(&self) -> &str {
        // let a :Option<ClientUnaryReceiver>=None;

        self.meta_config.cluster_name.as_str()
    }

    fn connect_grpc_client(&self) -> Result<GrpcClient> {
        let client = self.build_rpc_client()?;
        let (sender, receiver) = client
            .node_heartbeat_opt(CallOption::default())
            .context(FetchActionCmdError)?;
        Ok(GrpcClient {
            client,
            heartbeat_channel: NodeHeartbeatChannel {
                heartbeat_sender: sender,
                action_cmd_receiver: Some(receiver),
            },
        })
    }

    async fn reconnect_heartbeat_channel(&self) {
        let grpc_client = &mut *self.grpc_client.write().await;
        loop {
            match self.connect_grpc_client() {
                Ok(client) => {
                    *grpc_client = Some(client);
                    return;
                }
                Err(e) => {
                    error!("Grpc reconnect failed, error:{}", e);
                    time::sleep(self.error_wait_lease()).await;
                }
            }
        }
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

    fn fetch_view_interval(&self) -> Duration {
        Duration::from_secs(self.meta_config.lease.as_secs() * 3)
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

                    time::sleep(self.error_wait_lease()).await;
                }
            }
        }
    }

    async fn start_fetch_action_cmd(&self) {
        loop {
            let mut receiver = None;
            if let Some(client) = &mut *self.grpc_client.write().await {
                receiver = client.heartbeat_channel.action_cmd_receiver.take();
                if receiver.is_none() {
                    error!("Failed to fetch action cmd receiver");
                }
            } else {
                error!("Grpc client is not inited");
            }

            if let Some(v) = receiver {
                match self.fetch_action_cmd(v).await {
                    Ok(()) => {
                        info!(
                            "Fetch cluster view finished, cluster:{}",
                            self.get_cluster_name()
                        );
                    }
                    Err(e) => {
                        self.reconnect_heartbeat_channel().await;
                        error!(
                            "Failed to get action cmd, cluster:{}, error:{}",
                            self.get_cluster_name(),
                            e
                        );
                    }
                }
            }

            time::sleep(self.error_wait_lease()).await;
        }
    }

    async fn fetch_action_cmd(
        &self,
        mut receiver: ClientDuplexReceiver<PbNodeHeartbeatResponse>,
    ) -> Result<()> {
        while let Some(resp) = receiver.try_next().await.context(FetchActionCmdError)? {
            info!(
                "Fetch action cmd from meta, cluster:{}, action_cmd:{:?}",
                self.get_cluster_name(),
                resp,
            );
            if let Some(notify_sender) = &self.notify_sender {
                let resp: NodeHeartbeatResponse = resp.into();
                if let Err(e) = check_response_header(&resp.header) {
                    error!("Fetch action cmd failed, err:{}", e);
                    continue;
                }
                if let Some(action_cmd) = resp.action_cmd {
                    if let Err(e) = notify_sender.send(action_cmd.clone()).await {
                        error!(
                            "Notify sender send failed, action cmd:{:?}, err:{}",
                            action_cmd, e
                        );
                    }
                } else {
                    warn!("Fetch action cmd is empty, resp:{:?}", resp)
                }
            }
        }

        Ok(())
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
        node_meta_info: NodeMetaInfo,
        runtime: Arc<Runtime>,
        sender: Option<Sender<ActionCmd>>,
    ) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(MetaClientImplInner::new(config, node_meta_info, sender)?),
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
        self.inner.reconnect_heartbeat_channel().await;

        let inner = self.inner.clone();
        self.runtime.spawn(async move {
            inner.start_refresh_meta_addresses().await;
        });

        let inner = self.inner.clone();
        self.runtime.spawn(async move {
            inner.start_fetch_action_cmd().await;
        });

        info!("Meta client has started");

        Ok(())
    }

    async fn alloc_schema_id(&self, req: AllocSchemaIdRequest) -> Result<AllocSchemaIdResponse> {
        if let Some(grpc_client) = &mut *self.inner.grpc_client.write().await {
            let mut pb_req: PbAllocSchemaIdRequest = req.into();
            pb_req.set_header(self.inner.request_header().into());
            let pb_resp = grpc_client
                .client
                .alloc_schema_id_async_opt(&pb_req, CallOption::default())
                .map_err(|e| Box::new(e) as _)
                .context(FailAllocSchemaId)?
                .await
                .map_err(|e| Box::new(e) as _)
                .context(FailAllocSchemaId)?;
            let resp: AllocSchemaIdResponse = pb_resp.into();
            check_response_header(&resp.header)?;
            Ok(resp)
        } else {
            FailGetGrpcClient {
                msg: "alloc schema id".to_string(),
            }
            .fail()
        }
    }

    async fn alloc_table_id(&self, req: AllocTableIdRequest) -> Result<AllocTableIdResponse> {
        if let Some(grpc_client) = &mut *self.inner.grpc_client.write().await {
            let mut pb_req: PbAllocTableIdRequest = req.into();
            pb_req.set_header(self.inner.request_header().into());
            let pb_resp = grpc_client
                .client
                .alloc_table_id_async_opt(&pb_req, CallOption::default())
                .map_err(|e| Box::new(e) as _)
                .context(FailAllocTableId)?
                .await
                .map_err(|e| Box::new(e) as _)
                .context(FailAllocTableId)?;
            let resp: AllocTableIdResponse = pb_resp.into();
            check_response_header(&resp.header)?;
            Ok(resp)
        } else {
            FailGetGrpcClient {
                msg: "alloc table id".to_string(),
            }
            .fail()
        }
    }

    async fn drop_table(&self, req: DropTableRequest) -> Result<DropTableResponse> {
        if let Some(grpc_client) = &mut *self.inner.grpc_client.write().await {
            let mut pb_req: PbDropTableRequest = req.into();
            pb_req.set_header(self.inner.request_header().into());
            let pb_resp = grpc_client
                .client
                .drop_table_async_opt(&pb_req, CallOption::default())
                .map_err(|e| Box::new(e) as _)
                .context(FailDropTable)?
                .await
                .map_err(|e| Box::new(e) as _)
                .context(FailDropTable)?;
            let resp: DropTableResponse = pb_resp.into();
            check_response_header(&resp.header)?;
            Ok(resp)
        } else {
            FailGetGrpcClient {
                msg: "drop table".to_string(),
            }
            .fail()
        }
    }

    async fn get_tables(&self, req: GetTablesRequest) -> Result<GetTablesResponse> {
        if let Some(grpc_client) = &mut *self.inner.grpc_client.write().await {
            let mut pb_req: PbGetTablesRequest = req.into();
            pb_req.set_header(self.inner.request_header().into());
            let pb_resp = grpc_client
                .client
                .get_tables_async_opt(&pb_req, CallOption::default())
                .map_err(|e| Box::new(e) as _)
                .context(FailGetTables)?
                .await
                .map_err(|e| Box::new(e) as _)
                .context(FailGetTables)?;
            let resp: GetTablesResponse = pb_resp.into();
            check_response_header(&resp.header)?;
            Ok(resp)
        } else {
            FailGetGrpcClient {
                msg: "get tables".to_string(),
            }
            .fail()
        }
    }

    async fn send_heartbeat(&self, shards_info: Vec<ShardInfo>) -> Result<()> {
        if let Some(grpc_client) = &mut *self.inner.grpc_client.write().await {
            info!(
                "Meta client send heartbeat, cluster:{}, shards_info:{:?}",
                self.inner.get_cluster_name(),
                shards_info
            );
            let mut pb_request = PbNodeHeartbeatRequest::new();
            pb_request.set_header(self.inner.request_header().into());
            let node_info = NodeInfo {
                node_meta_info: self.inner.node_meta_info(),
                shards_info,
            };
            pb_request.set_info(node_info.into());
            if let Err(e) = grpc_client
                .heartbeat_channel
                .heartbeat_sender
                .send((pb_request, WriteFlags::default()))
                .await
                .map_err(|e| Box::new(e) as _)
                .context(FailSendHeartbeat {
                    cluster: self.inner.get_cluster_name(),
                })
            {
                self.inner.reconnect_heartbeat_channel().await;
                return Err(e);
            };
        } else {
            error!("Grpc_client is none");
        }

        Ok(())
    }
}

fn check_response_header(header: &ResponseHeader) -> Result<()> {
    if header.success {
        Ok(())
    } else {
        Meta {
            header: header.clone(),
        }
        .fail()
    }
}

/// Create a meta client with given `config`.
pub fn build_meta_client(
    config: MetaClientConfig,
    node_meta_info: NodeMetaInfo,
    runtime: Arc<Runtime>,
    sender: Option<Sender<ActionCmd>>,
) -> Result<Arc<dyn MetaClient + Send + Sync>> {
    let meta_client = MetaClientImpl::new(config, node_meta_info, runtime, sender)?;
    Ok(Arc::new(meta_client))
}
