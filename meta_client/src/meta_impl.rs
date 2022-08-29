// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use async_trait::async_trait;
use ceresdbproto_deps::ceresdbproto::{
    common::ResponseHeader, meta_service, meta_service_grpc::CeresmetaRpcServiceClient,
};
use common_util::{
    config::ReadableDuration,
    runtime::{JoinHandle, Runtime},
};
use futures::{SinkExt, TryStreamExt};
use grpcio::{
    CallOption, ChannelBuilder, ClientDuplexReceiver, ClientDuplexSender, Environment, WriteFlags,
};
use log::{debug, error, info, warn};
use serde_derive::Deserialize;
use snafu::{OptionExt, ResultExt};
use tokio::{
    sync::{
        mpsc::{self, Receiver, Sender},
        RwLock,
    },
    time,
};

use crate::{
    types::{
        ActionCmd, AllocSchemaIdRequest, AllocSchemaIdResponse, AllocTableIdRequest,
        AllocTableIdResponse, CreateTableCmd, DropTableCmd, DropTableRequest, GetNodesRequest,
        GetNodesResponse, GetShardTablesRequest, GetShardTablesResponse, NodeHeartbeatResponse,
        NodeInfo, NodeMetaInfo, RequestHeader, RouteTablesRequest, RouteTablesResponse, ShardInfo,
    },
    EventHandler, EventHandlerRef, FailAllocSchemaId, FailAllocTableId, FailDropTable,
    FailGetGrpcClient, FailGetTables, FailHandleEvent, FailRouteTables, FailSendHeartbeat,
    FetchActionCmd, InitHeartBeatStream, MetaClient, MetaRpc, Result,
};

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
            meta_addr: "127.0.0.1:8080".to_string(),
            lease: ReadableDuration::secs(10),
            timeout: ReadableDuration::secs(5),
            cq_count: 8,
        }
    }
}

struct NodeHeartbeatChannel {
    heartbeat_sender: ClientDuplexSender<meta_service::NodeHeartbeatRequest>,
    action_cmd_receiver: Option<ClientDuplexReceiver<meta_service::NodeHeartbeatResponse>>,
}

struct GrpcClient {
    client: CeresmetaRpcServiceClient,
    heartbeat_channel: RwLock<NodeHeartbeatChannel>,
}

// The handler may contain the Self instance which will cause circular
// dependency.
type EventHandlers = Vec<Weak<dyn EventHandler + Send + Sync>>;

struct Inner {
    meta_config: MetaClientConfig,
    node_meta_info: NodeMetaInfo,

    grpc_client: RwLock<Option<GrpcClient>>,
    handlers: RwLock<EventHandlers>,
}

impl Inner {
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
            node: self.node_meta_info.endpoint(),
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
            .context(InitHeartBeatStream)?;
        Ok(GrpcClient {
            client,
            heartbeat_channel: RwLock::new(NodeHeartbeatChannel {
                heartbeat_sender: sender,
                action_cmd_receiver: Some(receiver),
            }),
        })
    }

    async fn reconnect_heartbeat_channel(&self) -> GrpcClient {
        info!("gRPC reconnect begin");

        loop {
            match self.connect_grpc_client() {
                Ok(client) => {
                    info!("gRPC reconnect succeeds");
                    return client;
                }
                Err(e) => {
                    error!("gRPC reconnect failed, err:{}", e);

                    time::sleep(self.error_wait_lease()).await;
                }
            }
        }
    }

    fn build_rpc_client(&self) -> CeresmetaRpcServiceClient {
        let cb = ChannelBuilder::new(Arc::new(Environment::new(self.meta_config.cq_count)));
        CeresmetaRpcServiceClient::new(cb.connect(&self.meta_config.meta_addr))
    }

    // TODO: Store the value in field
    fn error_wait_lease(&self) -> Duration {
        self.meta_config.lease.0 / 2
    }

    async fn start_fetch_action_cmd_loop(&self, mut stop_rx: Receiver<()>) {
        info!("Begin fetching action cmd loop");
        loop {
            let mut receiver = None;
            if let Some(client) = &*self.grpc_client.read().await {
                receiver = client
                    .heartbeat_channel
                    .write()
                    .await
                    .action_cmd_receiver
                    .take();
            };

            if receiver.is_none() {
                warn!("gRPC stream is not init and try to reconnect the stream");
                let client = self.reconnect_heartbeat_channel().await;
                *self.grpc_client.write().await = Some(client);
                continue;
            }

            match self
                .fetch_and_handle_cmd(receiver.unwrap(), &mut stop_rx)
                .await
            {
                Ok(true) => {
                    warn!("Received exit message during fetching action cmd, exit the fetch_action_cmd loop");
                    return;
                }
                Ok(false) => info!("Heartbeat stream is exhausted and will rebuild another stream"),
                Err(err) => error!(
                    "Failed to get action cmd, cluster:{}, err:{}",
                    self.get_cluster_name(),
                    err
                ),
            }

            if time::timeout(self.error_wait_lease(), stop_rx.recv())
                .await
                .is_ok()
            {
                warn!("Receive exit message, exit the fetch_action_cmd loop");
                return;
            }
        }
    }

    /// Fetch and handle the action commands from the heartbeat response.
    ///
    /// Tells caller whether the procedure should stop.
    async fn fetch_and_handle_cmd(
        &self,
        mut receiver: ClientDuplexReceiver<meta_service::NodeHeartbeatResponse>,
        stop_rx: &mut Receiver<()>,
    ) -> Result<bool> {
        info!("Start keep fetch action cmd loop");
        loop {
            let fetch_res = tokio::select! {
                res = receiver.try_next() => Some(res),
                _ = stop_rx.recv() => None,
            };
            if fetch_res.is_none() {
                warn!("Receive exit message, exit the fetch_and_handle_cmd loop");
                return Ok(true);
            }
            let resp = match fetch_res.unwrap().context(FetchActionCmd)? {
                Some(resp) => resp,
                None => return Ok(false),
            };

            info!(
                "Fetch action cmd from meta, cluster:{}, heartbeat response:{:?}",
                self.get_cluster_name(),
                resp,
            );

            if let Err(e) = check_response_header(resp.get_header()) {
                error!("Fetch action cmd failed, err:{}", e);
                continue;
            }

            let resp = NodeHeartbeatResponse::from(resp);
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
    }

    pub async fn handle_event(&self, event: &ActionCmd) -> Result<()> {
        let handlers = self.handlers.read().await;
        for handler in handlers.iter() {
            if let Some(handler) = handler.upgrade() {
                handler.handle(event).await.context(FailHandleEvent {
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
    inner: Arc<Inner>,
    runtime: Arc<Runtime>,

    eventloop_handle: Mutex<Option<JoinHandle<()>>>,
    stop_eventloop_tx: Mutex<Option<Sender<()>>>,
}

impl MetaClientImpl {
    pub fn new(
        config: MetaClientConfig,
        node_meta_info: NodeMetaInfo,
        runtime: Arc<Runtime>,
    ) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(Inner::new(config, node_meta_info)?),
            runtime,
            eventloop_handle: Mutex::new(None),
            stop_eventloop_tx: Mutex::new(None),
        })
    }
}

#[async_trait]
impl MetaClient for MetaClientImpl {
    async fn start(&self) -> Result<()> {
        info!(
            "Meta client is starting with config:{:?}",
            self.inner.meta_config
        );

        let client = self.inner.reconnect_heartbeat_channel().await;
        *self.inner.grpc_client.write().await = Some(client);

        let inner = self.inner.clone();
        let (tx, rx) = mpsc::channel(1);
        let eventloop_handle = self.runtime.spawn(async move {
            inner.start_fetch_action_cmd_loop(rx).await;
        });

        *self.eventloop_handle.lock().unwrap() = Some(eventloop_handle);
        *self.stop_eventloop_tx.lock().unwrap() = Some(tx);

        info!("Meta client has started");
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        info!("Meta client is stopping");

        {
            let tx = self.stop_eventloop_tx.lock().unwrap().take();
            if let Some(tx) = tx {
                let _ = tx.send(()).await;
            }
        }
        info!("Stop eventloop signal is sent");

        {
            let handle = self.eventloop_handle.lock().unwrap().take();
            if let Some(handle) = handle {
                let _ = handle.await;
            }
        }

        info!("Meta client has stopped");
        Ok(())
    }

    async fn register_event_handler(&self, handler: EventHandlerRef) -> Result<()> {
        let mut handlers = self.inner.handlers.write().await;
        handlers.push(Arc::downgrade(&handler));

        Ok(())
    }

    async fn alloc_schema_id(&self, req: AllocSchemaIdRequest) -> Result<AllocSchemaIdResponse> {
        let grpc_client_guard = self.inner.grpc_client.read().await;
        let grpc_client = grpc_client_guard.as_ref().context(FailGetGrpcClient)?;

        let mut pb_req = meta_service::AllocSchemaIdRequest::from(req);
        pb_req.set_header(self.inner.request_header().into());
        let pb_resp = grpc_client
            .client
            .alloc_schema_id_async_opt(&pb_req, CallOption::default())
            .map_err(|e| Box::new(e) as _)
            .context(FailAllocSchemaId)?
            .await
            .map_err(|e| Box::new(e) as _)
            .context(FailAllocSchemaId)?;

        info!(
            "Meta client alloc schema id, req:{:?}, resp:{:?}",
            pb_req, pb_resp
        );

        check_response_header(pb_resp.get_header())?;
        Ok(AllocSchemaIdResponse::from(pb_resp))
    }

    async fn alloc_table_id(&self, req: AllocTableIdRequest) -> Result<AllocTableIdResponse> {
        let grpc_client_guard = self.inner.grpc_client.read().await;
        let grpc_client = grpc_client_guard.as_ref().context(FailGetGrpcClient)?;

        let mut pb_req = meta_service::AllocTableIdRequest::from(req);
        pb_req.set_header(self.inner.request_header().into());
        let pb_resp = grpc_client
            .client
            .alloc_table_id_async_opt(&pb_req, CallOption::default())
            .map_err(|e| Box::new(e) as _)
            .context(FailAllocTableId)?
            .await
            .map_err(|e| Box::new(e) as _)
            .context(FailAllocTableId)?;

        info!(
            "Meta client alloc table id, req:{:?}, resp:{:?}",
            pb_req, pb_resp
        );

        check_response_header(pb_resp.get_header())?;
        let resp = AllocTableIdResponse::from(pb_resp);

        let add_table_cmd = ActionCmd::CreateTableCmd(CreateTableCmd {
            schema_name: resp.schema_name.clone(),
            name: resp.name.clone(),
            shard_id: resp.shard_id,
            schema_id: resp.schema_id,
            id: resp.id,
        });
        self.inner.handle_event(&add_table_cmd).await?;

        Ok(resp)
    }

    async fn drop_table(&self, req: DropTableRequest) -> Result<()> {
        let grpc_client_guard = self.inner.grpc_client.read().await;
        let grpc_client = grpc_client_guard.as_ref().context(FailGetGrpcClient)?;

        let mut pb_req = meta_service::DropTableRequest::from(req.clone());
        pb_req.set_header(self.inner.request_header().into());
        let pb_resp = grpc_client
            .client
            .drop_table_async_opt(&pb_req, CallOption::default())
            .map_err(|e| Box::new(e) as _)
            .context(FailDropTable)?
            .await
            .map_err(|e| Box::new(e) as _)
            .context(FailDropTable)?;

        info!(
            "Meta client drop table, req:{:?}, resp:{:?}",
            pb_req, pb_resp
        );

        check_response_header(pb_resp.get_header())?;
        let drop_table_cmd = ActionCmd::DropTableCmd(DropTableCmd {
            schema_name: req.schema_name.clone(),
            name: req.name.clone(),
        });

        self.inner.handle_event(&drop_table_cmd).await
    }

    async fn get_tables(&self, req: GetShardTablesRequest) -> Result<GetShardTablesResponse> {
        let grpc_client_guard = self.inner.grpc_client.read().await;
        let grpc_client = grpc_client_guard.as_ref().context(FailGetGrpcClient)?;

        let mut pb_req = meta_service::GetShardTablesRequest::from(req);
        pb_req.set_header(self.inner.request_header().into());

        let pb_resp = grpc_client
            .client
            .get_shard_tables_async_opt(&pb_req, CallOption::default())
            .map_err(|e| Box::new(e) as _)
            .context(FailGetTables)?
            .await
            .map_err(|e| Box::new(e) as _)
            .context(FailGetTables)?;

        debug!(
            "Meta client get tables, req:{:?}, resp:{:?}",
            pb_req, pb_resp
        );

        check_response_header(pb_resp.get_header())?;

        Ok(GetShardTablesResponse::from(pb_resp))
    }

    async fn route_tables(&self, req: RouteTablesRequest) -> Result<RouteTablesResponse> {
        // TODO: maybe we can define a macro to avoid these boilerplate codes.
        let grpc_client_guard = self.inner.grpc_client.read().await;
        let grpc_client = grpc_client_guard.as_ref().context(FailGetGrpcClient)?;

        let mut pb_req = meta_service::RouteTablesRequest::from(req);
        pb_req.set_header(self.inner.request_header().into());

        let pb_resp = grpc_client
            .client
            .route_tables_async_opt(&pb_req, CallOption::default())
            .map_err(|e| Box::new(e) as _)
            .context(FailRouteTables)?
            .await
            .map_err(|e| Box::new(e) as _)
            .context(FailRouteTables)?;

        debug!(
            "Meta client route tables, req:{:?}, resp:{:?}",
            pb_req, pb_resp
        );

        check_response_header(pb_resp.get_header())?;
        Ok(RouteTablesResponse::from(pb_resp))
    }

    async fn get_nodes(&self, req: GetNodesRequest) -> Result<GetNodesResponse> {
        // TODO: maybe we can define a macro to avoid these boilerplate codes.
        let grpc_client_guard = self.inner.grpc_client.read().await;
        let grpc_client = grpc_client_guard.as_ref().context(FailGetGrpcClient)?;

        let mut pb_req = meta_service::GetNodesRequest::from(req);
        pb_req.set_header(self.inner.request_header().into());

        let pb_resp = grpc_client
            .client
            .get_nodes_async_opt(&pb_req, CallOption::default())
            .map_err(|e| Box::new(e) as _)
            .context(FailRouteTables)?
            .await
            .map_err(|e| Box::new(e) as _)
            .context(FailRouteTables)?;

        debug!(
            "Meta client get nodes, req:{:?}, resp:{:?}",
            pb_req, pb_resp
        );

        check_response_header(pb_resp.get_header())?;
        Ok(GetNodesResponse::from(pb_resp))
    }

    async fn send_heartbeat(&self, shards_info: Vec<ShardInfo>) -> Result<()> {
        info!(
            "Meta client send heartbeat, cluster:{}, shards_info:{:?}",
            self.inner.get_cluster_name(),
            shards_info
        );

        let grpc_client_guard = self.inner.grpc_client.read().await;
        let grpc_client = grpc_client_guard.as_ref().context(FailGetGrpcClient)?;

        let mut pb_req = meta_service::NodeHeartbeatRequest::new();
        pb_req.set_header(self.inner.request_header().into());
        let node_info = NodeInfo {
            node_meta_info: self.inner.node_meta_info(),
            shards_info,
        };
        pb_req.set_info(node_info.into());

        info!("Meta client send heartbeat req:{:?}", pb_req);

        let send_res = grpc_client
            .heartbeat_channel
            .write()
            .await
            .heartbeat_sender
            .send((pb_req, WriteFlags::default()))
            .await
            .map_err(|e| Box::new(e) as _)
            .context(FailSendHeartbeat {
                cluster: self.inner.get_cluster_name(),
            });

        // FIXME: reconnect the heartbeat channel iff specific errors occur.
        if send_res.is_err() {
            let client = self.inner.reconnect_heartbeat_channel().await;
            drop(grpc_client_guard);
            *self.inner.grpc_client.write().await = Some(client);
        }
        send_res
    }
}

fn check_response_header(header: &ResponseHeader) -> Result<()> {
    if header.code == 0 {
        Ok(())
    } else {
        MetaRpc {
            code: header.code,
            msg: header.get_error(),
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
