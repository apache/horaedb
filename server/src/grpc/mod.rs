// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Grpc services

use std::{
    net::{AddrParseError, SocketAddr},
    stringify,
    sync::Arc,
    time::Duration,
};

use analytic_engine::setup::OpenedWals;
use ceresdbproto::{
    meta_event::meta_event_service_server::MetaEventServiceServer,
    remote_engine::remote_engine_service_server::RemoteEngineServiceServer,
    storage::storage_service_server::StorageServiceServer,
};
use cluster::ClusterRef;
use common_types::column_schema;
use common_util::{
    define_result,
    error::{BoxError, GenericError},
    runtime::{JoinHandle, Runtime},
};
use futures::FutureExt;
use log::{info, warn};
use query_engine::executor::Executor as QueryExecutor;
use router::RouterRef;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use table_engine::engine::EngineRuntimes;
use tokio::sync::oneshot::{self, Sender};
use tonic::transport::Server;

use crate::{
    grpc::{
        meta_event_service::MetaServiceImpl, remote_engine_service::RemoteEngineServiceImpl,
        storage_service::StorageServiceImpl,
    },
    instance::InstanceRef,
    proxy::{forward, hotspot, Proxy},
    schema_config_provider::{self, SchemaConfigProviderRef},
};

mod meta_event_service;
mod metrics;
mod remote_engine_service;
pub(crate) mod storage_service;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Internal error, message:{}, cause:{}", msg, source))]
    Internal { msg: String, source: GenericError },

    #[snafu(display(
        "Failed to keep grpc service, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    FailServe {
        source: tonic::transport::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to parse rpc service addr, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    InvalidRpcServeAddr {
        source: AddrParseError,
        backtrace: Backtrace,
    },

    #[snafu(display("Missing meta client config.\nBacktrace:\n{}", backtrace))]
    MissingMetaClientConfig { backtrace: Backtrace },

    #[snafu(display("Missing grpc environment.\nBacktrace:\n{}", backtrace))]
    MissingEnv { backtrace: Backtrace },

    #[snafu(display("Missing runtimes.\nBacktrace:\n{}", backtrace))]
    MissingRuntimes { backtrace: Backtrace },

    #[snafu(display(
        "Missing local endpoint when forwarder enabled.\nBacktrace:\n{}",
        backtrace
    ))]
    MissingLocalEndpoint { backtrace: Backtrace },

    #[snafu(display("Invalid local endpoint when forwarder enabled, err:{}", source,))]
    InvalidLocalEndpoint { source: GenericError },

    #[snafu(display("Missing instance.\nBacktrace:\n{}", backtrace))]
    MissingInstance { backtrace: Backtrace },

    #[snafu(display("Missing router.\nBacktrace:\n{}", backtrace))]
    MissingRouter { backtrace: Backtrace },

    #[snafu(display("Missing wals.\nBacktrace:\n{}", backtrace))]
    MissingWals { backtrace: Backtrace },

    #[snafu(display("Missing schema config provider.\nBacktrace:\n{}", backtrace))]
    MissingSchemaConfigProvider { backtrace: Backtrace },

    #[snafu(display("Missing timeout.\nBacktrace:\n{}", backtrace))]
    MissingTimeout { backtrace: Backtrace },

    #[snafu(display("Catalog name is not utf8.\nBacktrace:\n{}", backtrace))]
    ParseCatalogName {
        source: std::string::FromUtf8Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Schema name is not utf8.\nBacktrace:\n{}", backtrace))]
    ParseSchemaName {
        source: std::string::FromUtf8Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Fail to build forwarder, err:{}", source))]
    BuildForwarder { source: forward::Error },

    #[snafu(display(
        "Fail to build column schema from column: {}, err:{}",
        column_name,
        source
    ))]
    BuildColumnSchema {
        column_name: String,
        source: column_schema::Error,
    },

    #[snafu(display("Invalid column: {} schema, err:{}", column_name, source))]
    InvalidColumnSchema {
        column_name: String,
        source: column_schema::Error,
    },

    #[snafu(display("Invalid argument: {}", msg))]
    InvalidArgument { msg: String },

    #[snafu(display("Get schema config failed, err:{}", source))]
    GetSchemaConfig {
        source: schema_config_provider::Error,
    },
}

define_result!(Error);

/// Rpc services manages all grpc services of the server.
pub struct RpcServices<Q: QueryExecutor + 'static> {
    serve_addr: SocketAddr,
    rpc_server: StorageServiceServer<StorageServiceImpl<Q>>,
    meta_rpc_server: Option<MetaEventServiceServer<MetaServiceImpl<Q>>>,
    remote_engine_server: RemoteEngineServiceServer<RemoteEngineServiceImpl<Q>>,
    runtime: Arc<Runtime>,
    stop_tx: Option<Sender<()>>,
    join_handle: Option<JoinHandle<()>>,
}

impl<Q: QueryExecutor + 'static> RpcServices<Q> {
    pub async fn start(&mut self) -> Result<()> {
        let rpc_server = self.rpc_server.clone();
        let meta_rpc_server = self.meta_rpc_server.clone();
        let remote_engine_server = self.remote_engine_server.clone();
        let serve_addr = self.serve_addr;
        let (stop_tx, stop_rx) = oneshot::channel();
        let join_handle = self.runtime.spawn(async move {
            info!("Grpc server tries to listen on {}", serve_addr);

            let mut router = Server::builder().add_service(rpc_server);

            if let Some(s) = meta_rpc_server {
                info!("Grpc server serves meta rpc service");
                router = router.add_service(s);
            };

            info!("Grpc server serves remote engine rpc service");
            router = router.add_service(remote_engine_server);

            router
                .serve_with_shutdown(serve_addr, stop_rx.map(drop))
                .await
                .unwrap_or_else(|e| {
                    panic!("Grpc server listens failed, err:{e:?}");
                });
        });
        self.join_handle = Some(join_handle);
        self.stop_tx = Some(stop_tx);
        Ok(())
    }

    pub async fn shutdown(&mut self) {
        if let Some(stop_tx) = self.stop_tx.take() {
            let res = stop_tx.send(());
            warn!("Send stop signal, send_res:{:?}", res);
        }

        if let Some(join_handle) = self.join_handle.take() {
            let join_res = join_handle.await;
            warn!("Finish join with serve task, join_res:{:?}", join_res);
        }
    }
}

pub struct Builder<Q> {
    endpoint: String,
    timeout: Option<Duration>,
    resp_compress_min_length: usize,
    local_endpoint: Option<String>,
    runtimes: Option<Arc<EngineRuntimes>>,
    instance: Option<InstanceRef<Q>>,
    router: Option<RouterRef>,
    cluster: Option<ClusterRef>,
    opened_wals: Option<OpenedWals>,
    schema_config_provider: Option<SchemaConfigProviderRef>,
    forward_config: Option<forward::Config>,
    auto_create_table: bool,
    hotspot_config: Option<hotspot::Config>,
}

impl<Q> Builder<Q> {
    pub fn new() -> Self {
        Self {
            endpoint: "0.0.0.0:8381".to_string(),
            timeout: None,
            resp_compress_min_length: 81920,
            local_endpoint: None,
            runtimes: None,
            instance: None,
            router: None,
            cluster: None,
            opened_wals: None,
            schema_config_provider: None,
            forward_config: None,
            auto_create_table: true,
            hotspot_config: None,
        }
    }

    pub fn endpoint(mut self, endpoint: String) -> Self {
        self.endpoint = endpoint;
        self
    }

    pub fn resp_compress_min_length(mut self, threshold: usize) -> Self {
        self.resp_compress_min_length = threshold;
        self
    }

    pub fn local_endpoint(mut self, endpoint: String) -> Self {
        self.local_endpoint = Some(endpoint);

        self
    }

    pub fn runtimes(mut self, runtimes: Arc<EngineRuntimes>) -> Self {
        self.runtimes = Some(runtimes);
        self
    }

    pub fn instance(mut self, instance: InstanceRef<Q>) -> Self {
        self.instance = Some(instance);
        self
    }

    pub fn router(mut self, router: RouterRef) -> Self {
        self.router = Some(router);
        self
    }

    // Cluster is an optional field for building [RpcServices].
    pub fn cluster(mut self, cluster: Option<ClusterRef>) -> Self {
        self.cluster = cluster;
        self
    }

    pub fn opened_wals(mut self, opened_wals: OpenedWals) -> Self {
        self.opened_wals = Some(opened_wals);
        self
    }

    pub fn schema_config_provider(mut self, provider: SchemaConfigProviderRef) -> Self {
        self.schema_config_provider = Some(provider);
        self
    }

    pub fn forward_config(mut self, config: forward::Config) -> Self {
        self.forward_config = Some(config);
        self
    }

    pub fn hotspot_config(mut self, config: hotspot::Config) -> Self {
        self.hotspot_config = Some(config);
        self
    }

    pub fn timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn auto_create_table(mut self, auto_create_table: bool) -> Self {
        self.auto_create_table = auto_create_table;
        self
    }
}

impl<Q: QueryExecutor + 'static> Builder<Q> {
    pub fn build(self) -> Result<RpcServices<Q>> {
        let runtimes = self.runtimes.context(MissingRuntimes)?;
        let instance = self.instance.context(MissingInstance)?;
        let router = self.router.context(MissingRouter)?;
        let opened_wals = self.opened_wals.context(MissingWals)?;
        let schema_config_provider = self
            .schema_config_provider
            .context(MissingSchemaConfigProvider)?;

        let meta_rpc_server = self.cluster.map(|v| {
            let builder = meta_event_service::Builder {
                cluster: v,
                instance: instance.clone(),
                runtime: runtimes.meta_runtime.clone(),
                opened_wals,
            };
            MetaEventServiceServer::new(builder.build())
        });

        let remote_engine_server = {
            let service = RemoteEngineServiceImpl {
                instance: instance.clone(),
                runtimes: runtimes.clone(),
            };
            RemoteEngineServiceServer::new(service)
        };

        let forward_config = self.forward_config.unwrap_or_default();
        let hotspot_config = self.hotspot_config.unwrap_or_default();
        let proxy_runtime = runtimes.bg_runtime.clone();
        let runtime = runtimes.bg_runtime.clone();
        let proxy = Proxy::try_new(
            router,
            instance,
            forward_config,
            self.local_endpoint.context(MissingLocalEndpoint)?,
            self.resp_compress_min_length,
            self.auto_create_table,
            schema_config_provider,
            hotspot_config,
            proxy_runtime,
        )
        .box_err()
        .context(Internal {
            msg: "fail to init proxy",
        })?;
        let storage_service = StorageServiceImpl {
            proxy: Arc::new(proxy),
            runtimes,
            timeout: self.timeout,
        };
        let rpc_server = StorageServiceServer::new(storage_service);

        let serve_addr = self.endpoint.parse().context(InvalidRpcServeAddr)?;

        Ok(RpcServices {
            serve_addr,
            rpc_server,
            meta_rpc_server,
            remote_engine_server,
            runtime,
            stop_tx: None,
            join_handle: None,
        })
    }
}
