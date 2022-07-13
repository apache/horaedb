use std::{
    sync::{Arc, Weak},
    time::Duration,
};

use async_trait::async_trait;
use ceresdbxproto::{
    metagrpcV2::{
        AllocSchemaIdRequest as PbAllocSchemaIdRequest,
        AllocTableIdRequest as PbAllocTableIdRequest, DropTableRequest as PbDropTableRequest,
        GetTablesRequest as PbGetTablesRequest, NodeHeartbeatRequest as PbNodeHeartbeatRequest,
        NodeHeartbeatResponse as PbNodeHeartbeatResponse,
    },
    metagrpcV2_grpc::CeresmetaRpcServiceClient,
};
use common_util::{config::ReadableDuration, define_result, runtime::Runtime};
use futures::{SinkExt, TryStreamExt};
use grpcio::{
    CallOption, ChannelBuilder, ClientDuplexReceiver, ClientDuplexSender, Environment, WriteFlags,
};
use log::{error, info, warn};
use serde_derive::Deserialize;
use snafu::{Backtrace, ResultExt, Snafu};
use tokio::{sync::RwLock, time};
pub use types::*;

mod types;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(display(
        "Failed to fetch action cmd, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    FetchActionCmd {
        source: grpcio::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to get grpc client, grpc client is none, msg:{}.\nBacktrace:\n{}",
        msg,
        backtrace
    ))]
    FailGetGrpcClient { msg: String, backtrace: Backtrace },

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

    #[snafu(display(
        "Handler event failed, handler:{}, event:{:?}, err:{}",
        name,
        event,
        source
    ))]
    FailHandlerEvent {
        name: String,
        event: ActionCmd,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

define_result!(Error);

pub type EventHandlerRef = Arc<dyn EventHandler + Send + Sync>;

#[async_trait]
pub trait EventHandler {
    fn name(&self) -> &str;

    async fn handle(
        &self,
        event: &ActionCmd,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// Meta client abstraction
#[async_trait]
pub trait MetaClient {
    /// Start the meta client and the events will occur afterwards.
    async fn start(&self) -> Result<()>;

    /// Register handler for the event.
    ///
    /// It is better to register handlers before calling `start`.
    async fn register_event_handler(&self, handler: EventHandlerRef) -> Result<()>;

    async fn alloc_schema_id(&self, req: AllocSchemaIdRequest) -> Result<AllocSchemaIdResponse>;

    async fn alloc_table_id(&self, req: AllocTableIdRequest) -> Result<AllocTableIdResponse>;

    async fn drop_table(&self, req: DropTableRequest) -> Result<DropTableResponse>;

    async fn get_tables(&self, req: GetTablesRequest) -> Result<GetTablesResponse>;

    async fn send_heartbeat(&self, req: Vec<ShardInfo>) -> Result<()>;
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct MetaClientConfig {
    pub cluster_name: String,
    pub meta_addr: String,
    pub lease: ReadableDuration,
    pub timeout: ReadableDuration,
    pub cq_count: usize,
}

impl Default for MetaClientConfig {
    fn default() -> Self {
        Self {
            cluster_name: String::new(),
            meta_addr: "http://127.0.0.1:8080".to_string(),
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
    heartbeat_channel: RwLock<NodeHeartbeatChannel>,
}

// The handler may contain the Self instance which will cause circular
// dependency.
type EventHandlers = Vec<Weak<dyn EventHandler + Send + Sync>>;

struct MetaClientImplInner {
    meta_config: MetaClientConfig,
    node_meta_info: NodeMetaInfo,

    grpc_client: RwLock<Option<GrpcClient>>,
    handlers: RwLock<EventHandlers>,
}

impl MetaClientImplInner {
    fn new(meta_config: MetaClientConfig, node_meta_info: NodeMetaInfo) -> Result<Self> {
        let client = Self {
            meta_config,
            node_meta_info,
            grpc_client: RwLock::new(None),
            handlers: RwLock::new(EventHandlers::new()),
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
        self.meta_config.cluster_name.as_str()
    }

    fn connect_grpc_client(&self) -> Result<GrpcClient> {
        let client = self.build_rpc_client();
        let (sender, receiver) = client
            .node_heartbeat_opt(CallOption::default())
            .context(FetchActionCmd)?;
        Ok(GrpcClient {
            client,
            heartbeat_channel: RwLock::new(NodeHeartbeatChannel {
                heartbeat_sender: sender,
                action_cmd_receiver: Some(receiver),
            }),
        })
    }

    async fn reconnect_heartbeat_channel(&self) {
        loop {
            info!("Grpc reconnect begin");
            match self.connect_grpc_client() {
                Ok(client) => {
                    info!("Grpc reconnect success");
                    let grpc_client = &mut *self.grpc_client.write().await;
                    *grpc_client = Some(client);
                    return;
                }
                Err(e) => {
                    error!("Grpc reconnect failed, err:{}", e);
                    time::sleep(self.error_wait_lease()).await;
                }
            }
        }
    }

    fn build_rpc_client(&self) -> CeresmetaRpcServiceClient {
        let cb = ChannelBuilder::new(Arc::new(Environment::new(self.meta_config.cq_count)));
        CeresmetaRpcServiceClient::new(cb.connect(&self.meta_config.meta_addr))
    }

    // TODO(yingwen): Store the value in field
    fn error_wait_lease(&self) -> Duration {
        Duration::from_secs(self.meta_config.lease.as_secs() / 2)
    }

    async fn start_fetch_action_cmd(&self) {
        loop {
            info!("Fetch action cmd get grpc client");
            let receiver = match &*self.grpc_client.read().await {
                Some(client) => {
                    info!("Fetch action cmd get grpc client inner");
                    client
                        .heartbeat_channel
                        .write()
                        .await
                        .action_cmd_receiver
                        .take()
                }
                None => {
                    warn!("Grpc client is not inited when starting fetch action cmd");
                    None
                }
            };

            info!("Fetch action cmd get grpc client fetch_action_cmd");
            if let Some(v) = receiver {
                match self.fetch_action_cmd(v).await {
                    Ok(()) => {
                        info!(
                            "Fetch cluster action cmd finished, cluster:{}",
                            self.get_cluster_name()
                        );
                    }
                    Err(err) => {
                        error!(
                            "Failed to get action cmd, cluster:{}, err:{}",
                            self.get_cluster_name(),
                            err
                        );
                        self.reconnect_heartbeat_channel().await;
                    }
                }
            } else {
                warn!("Skip action command fetch, because no receiver found");
            }

            time::sleep(self.error_wait_lease()).await;
        }
    }

    async fn fetch_action_cmd(
        &self,
        mut receiver: ClientDuplexReceiver<PbNodeHeartbeatResponse>,
    ) -> Result<()> {
        info!("Start fetch action cmd loop");
        while let Some(resp) = receiver.try_next().await.context(FetchActionCmd)? {
            info!(
                "Fetch action cmd from meta, cluster:{}, action_cmd:{:?}",
                self.get_cluster_name(),
                resp,
            );

            let resp = NodeHeartbeatResponse::from(resp);
            if let Err(e) = check_response_header(&resp.header) {
                error!("Fetch action cmd failed, err:{}", e);
                continue;
            }
            let event = match resp.action_cmd {
                Some(action_cmd) => action_cmd,
                None => {
                    warn!("Fetch action cmd is empty, resp:{:?}", resp);
                    continue;
                }
            };

            if let Err(e) = self.handle_event(&event).await {
                error!("Handler fail to handle event:{:?}, err:{}", event, e);
            }
        }

        Ok(())
    }

    pub async fn handle_event(&self, event: &ActionCmd) -> Result<()> {
        let handlers = self.handlers.read().await;
        for handler in handlers.iter() {
            if let Some(handler) = handler.upgrade() {
                handler.handle(event).await.context(FailHandlerEvent {
                    name: handler.name().to_string(),
                    event: event.clone(),
                })?;
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
    ) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(MetaClientImplInner::new(config, node_meta_info)?),
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

        self.inner.reconnect_heartbeat_channel().await;

        let inner = self.inner.clone();
        self.runtime.spawn(async move {
            inner.start_fetch_action_cmd().await;
        });

        info!("Meta client has started");

        Ok(())
    }

    async fn register_event_handler(&self, handler: EventHandlerRef) -> Result<()> {
        let mut handlers = self.inner.handlers.write().await;
        handlers.push(Arc::downgrade(&handler));

        Ok(())
    }

    async fn alloc_schema_id(&self, req: AllocSchemaIdRequest) -> Result<AllocSchemaIdResponse> {
        if let Some(grpc_client) = &*self.inner.grpc_client.read().await {
            let mut pb_req = PbAllocSchemaIdRequest::from(req);
            pb_req.set_header(self.inner.request_header().into());
            let pb_resp = grpc_client
                .client
                .alloc_schema_id_async_opt(&pb_req, CallOption::default())
                .map_err(|e| Box::new(e) as _)
                .context(FailAllocSchemaId)?
                .await
                .map_err(|e| Box::new(e) as _)
                .context(FailAllocSchemaId)?;
            let resp = AllocSchemaIdResponse::from(pb_resp);
            info!(
                "Meta client alloc schema id, req:{:?}, resp:{:?}",
                pb_req, resp
            );
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
        if let Some(grpc_client) = &*self.inner.grpc_client.read().await {
            let mut pb_req = PbAllocTableIdRequest::from(req);
            pb_req.set_header(self.inner.request_header().into());
            let pb_resp = grpc_client
                .client
                .alloc_table_id_async_opt(&pb_req, CallOption::default())
                .map_err(|e| Box::new(e) as _)
                .context(FailAllocTableId)?
                .await
                .map_err(|e| Box::new(e) as _)
                .context(FailAllocTableId)?;
            let resp = AllocTableIdResponse::from(pb_resp);
            info!(
                "Meta client alloc table id, req:{:?}, resp:{:?}",
                pb_req, resp
            );
            check_response_header(&resp.header)?;
            self.inner
                .handle_event(&ActionCmd::AddTableCmd(AddTableCmd {
                    schema_name: resp.schema_name.clone(),
                    name: resp.name.clone(),
                    shard_id: resp.shard_id,
                    schema_id: resp.schema_id,
                    id: resp.id,
                }))
                .await?;

            Ok(resp)
        } else {
            FailGetGrpcClient {
                msg: "alloc table id".to_string(),
            }
            .fail()
        }
    }

    async fn drop_table(&self, req: DropTableRequest) -> Result<DropTableResponse> {
        if let Some(grpc_client) = &*self.inner.grpc_client.read().await {
            let mut pb_req = PbDropTableRequest::from(req.clone());
            pb_req.set_header(self.inner.request_header().into());
            let pb_resp = grpc_client
                .client
                .drop_table_async_opt(&pb_req, CallOption::default())
                .map_err(|e| Box::new(e) as _)
                .context(FailDropTable)?
                .await
                .map_err(|e| Box::new(e) as _)
                .context(FailDropTable)?;
            let resp = DropTableResponse::from(pb_resp);
            info!("Meta client drop table, req:{:?}, resp:{:?}", pb_req, resp);
            check_response_header(&resp.header)?;
            self.inner
                .handle_event(&ActionCmd::DropTableCmd(DropTableCmd {
                    schema_name: req.schema_name.clone(),
                    name: req.name.clone(),
                }))
                .await?;
            Ok(resp)
        } else {
            FailGetGrpcClient {
                msg: "drop table".to_string(),
            }
            .fail()
        }
    }

    async fn get_tables(&self, req: GetTablesRequest) -> Result<GetTablesResponse> {
        if let Some(grpc_client) = &*self.inner.grpc_client.read().await {
            let mut pb_req = PbGetTablesRequest::from(req);
            pb_req.set_header(self.inner.request_header().into());
            let pb_resp = grpc_client
                .client
                .get_tables_async_opt(&pb_req, CallOption::default())
                .map_err(|e| Box::new(e) as _)
                .context(FailGetTables)?
                .await
                .map_err(|e| Box::new(e) as _)
                .context(FailGetTables)?;
            let resp = GetTablesResponse::from(pb_resp);
            info!("Meta client get tables, req:{:?}, resp:{:?}", pb_req, resp);
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
        let ret = if let Some(grpc_client) = &*self.inner.grpc_client.read().await {
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
            info!("Meta client send heartbeat req:{:?}", pb_request);
            grpc_client
                .heartbeat_channel
                .write()
                .await
                .heartbeat_sender
                .send((pb_request, WriteFlags::default()))
                .await
                .map_err(|e| Box::new(e) as _)
                .context(FailSendHeartbeat {
                    cluster: self.inner.get_cluster_name(),
                })
        } else {
            error!("Meta client send heartbeat failed, grpc client is none");
            Ok(())
        };
        if ret.is_err() {
            self.inner.reconnect_heartbeat_channel().await;
        }
        ret
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
) -> Result<Arc<dyn MetaClient + Send + Sync>> {
    let meta_client = MetaClientImpl::new(config, node_meta_info, runtime)?;
    Ok(Arc::new(meta_client))
}
