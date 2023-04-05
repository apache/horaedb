// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

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
    error::GenericError,
    runtime::{JoinHandle, Runtime},
};
use futures::FutureExt;
use log::{info, warn};
use query_engine::executor::Executor as QueryExecutor;
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
    proxy::{forward, Proxy},
    schema_config_provider::{self},
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

    #[snafu(display("Missing instance.\nBacktrace:\n{}", backtrace))]
    MissingInstance { backtrace: Backtrace },

    #[snafu(display("Missing wals.\nBacktrace:\n{}", backtrace))]
    MissingWals { backtrace: Backtrace },

    #[snafu(display("Missing timeout.\nBacktrace:\n{}", backtrace))]
    MissingTimeout { backtrace: Backtrace },

    #[snafu(display("Missing proxy.\nBacktrace:\n{}", backtrace))]
    MissingProxy { backtrace: Backtrace },

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
        "Fail to build column schema from column:{}, err:{}",
        column_name,
        source
    ))]
    BuildColumnSchema {
        column_name: String,
        source: column_schema::Error,
    },

    #[snafu(display("Invalid column:{} schema, err:{}", column_name, source))]
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
    runtimes: Option<Arc<EngineRuntimes>>,
    instance: Option<InstanceRef<Q>>,
    cluster: Option<ClusterRef>,
    opened_wals: Option<OpenedWals>,
    proxy: Option<Arc<Proxy<Q>>>,
}

impl<Q> Builder<Q> {
    pub fn new() -> Self {
        Self {
            endpoint: "0.0.0.0:8381".to_string(),
            timeout: None,
            runtimes: None,
            instance: None,
            cluster: None,
            opened_wals: None,
            proxy: None,
        }
    }

    pub fn endpoint(mut self, endpoint: String) -> Self {
        self.endpoint = endpoint;
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

    // Cluster is an optional field for building [RpcServices].
    pub fn cluster(mut self, cluster: Option<ClusterRef>) -> Self {
        self.cluster = cluster;
        self
    }

    pub fn opened_wals(mut self, opened_wals: OpenedWals) -> Self {
        self.opened_wals = Some(opened_wals);
        self
    }

    pub fn timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn proxy(mut self, proxy: Arc<Proxy<Q>>) -> Self {
        self.proxy = Some(proxy);
        self
    }
}

impl<Q: QueryExecutor + 'static> Builder<Q> {
    pub fn build(self) -> Result<RpcServices<Q>> {
        let runtimes = self.runtimes.context(MissingRuntimes)?;
        let instance = self.instance.context(MissingInstance)?;
        let opened_wals = self.opened_wals.context(MissingWals)?;
        let proxy = self.proxy.context(MissingProxy)?;

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
                instance,
                runtimes: runtimes.clone(),
            };
            RemoteEngineServiceServer::new(service)
        };

        let bg_runtime = runtimes.bg_runtime.clone();
        let storage_service = StorageServiceImpl {
            proxy,
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
            runtime: bg_runtime,
            stop_tx: None,
            join_handle: None,
        })
    }
}
