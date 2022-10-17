// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Grpc services

use std::{
    collections::{BTreeMap, HashMap},
    net::{AddrParseError, SocketAddr},
    stringify,
    sync::Arc,
    time::Instant,
};

use async_trait::async_trait;
use catalog::schema::{CloseOptions, OpenOptions, OpenTableRequest};
use ceresdbproto::{
    common::ResponseHeader,
    meta_event::{
        meta_event_service_server::{MetaEventService, MetaEventServiceServer},
        ChangeShardRoleRequest, ChangeShardRoleResponse, CloseShardRequest, CloseShardResponse,
        CreateTableOnShardRequest, CreateTableOnShardResponse, DropTableOnShardRequest,
        DropTableOnShardResponse, MergeShardsRequest, MergeShardsResponse, OpenShardRequest,
        OpenShardResponse, SplitShardRequest, SplitShardResponse,
    },
    prometheus::{PrometheusQueryRequest, PrometheusQueryResponse},
    storage::{
        storage_service_server::{StorageService, StorageServiceServer},
        value::Value,
        QueryRequest, QueryResponse, RouteRequest, RouteResponse, WriteMetric, WriteRequest,
        WriteResponse,
    },
};
use cluster::{config::SchemaConfig, ClusterRef};
use common_types::{
    column_schema::{self, ColumnSchema},
    datum::DatumKind,
    schema::{Builder as SchemaBuilder, Error as SchemaError, Schema, TSID_COLUMN},
};
use common_util::{
    define_result,
    runtime::{JoinHandle, Runtime},
    time::InstantExt,
};
use futures::{
    stream::{self, BoxStream, StreamExt},
    FutureExt,
};
use http::StatusCode;
use log::{error, info, warn};
use paste::paste;
use query_engine::executor::Executor as QueryExecutor;
use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};
use sql::plan::CreateTablePlan;
use table_engine::{
    engine::{CloseTableRequest, EngineRuntimes},
    table::{SchemaId, TableId},
};
use tokio::sync::{
    mpsc,
    oneshot::{self, Sender},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    metadata::{KeyAndValueRef, MetadataMap},
    transport::Server,
};

use crate::{
    consts,
    error::{ErrNoCause, ErrWithCause, Result as ServerResult, ServerError},
    grpc::metrics::GRPC_HANDLER_DURATION_HISTOGRAM_VEC,
    instance::InstanceRef,
    route::{Router, RouterRef},
    schema_config_provider::{self, SchemaConfigProviderRef},
};

mod metrics;
mod prom_query;
mod query;
mod route;
mod write;

#[derive(Debug, Snafu)]
pub enum Error {
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

    #[snafu(display("Missing router.\nBacktrace:\n{}", backtrace))]
    MissingRouter { backtrace: Backtrace },

    #[snafu(display("Missing schema config provider.\nBacktrace:\n{}", backtrace))]
    MissingSchemaConfigProvider { backtrace: Backtrace },

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

    #[snafu(display("Fail to build table schema for metric: {}, err:{}", metric, source))]
    BuildTableSchema { metric: String, source: SchemaError },

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

const STREAM_QUERY_CHANNEL_LEN: usize = 20;

define_result!(Error);

/// Rpc request header
#[derive(Debug, Default)]
pub struct RequestHeader {
    metas: HashMap<String, Vec<u8>>,
}

impl From<&MetadataMap> for RequestHeader {
    fn from(meta: &MetadataMap) -> Self {
        let metas = meta
            .iter()
            .filter_map(|kv| match kv {
                KeyAndValueRef::Ascii(key, val) => {
                    // TODO: The value may be encoded in base64, which is not expected.
                    Some((key.to_string(), val.as_encoded_bytes().to_vec()))
                }
                KeyAndValueRef::Binary(key, val) => {
                    warn!(
                        "Binary header is not supported yet and will be omit, key:{:?}, val:{:?}",
                        key, val
                    );
                    None
                }
            })
            .collect();

        Self { metas }
    }
}

impl RequestHeader {
    pub fn get(&self, key: &str) -> Option<&[u8]> {
        self.metas.get(key).map(|v| v.as_slice())
    }
}

pub struct HandlerContext<'a, Q> {
    #[allow(dead_code)]
    header: RequestHeader,
    router: RouterRef,
    instance: InstanceRef<Q>,
    catalog: String,
    schema: String,
    schema_config: Option<&'a SchemaConfig>,
}

impl<'a, Q> HandlerContext<'a, Q> {
    fn new(
        header: RequestHeader,
        router: Arc<dyn Router + Sync + Send>,
        instance: InstanceRef<Q>,
        schema_config_provider: &'a SchemaConfigProviderRef,
    ) -> Result<Self> {
        let default_catalog = instance.catalog_manager.default_catalog_name();
        let default_schema = instance.catalog_manager.default_schema_name();

        let catalog = header
            .get(consts::CATALOG_HEADER)
            .map(|v| String::from_utf8(v.to_vec()))
            .transpose()
            .context(ParseCatalogName)?
            .unwrap_or_else(|| default_catalog.to_string());

        let schema = header
            .get(consts::TENANT_HEADER)
            .map(|v| String::from_utf8(v.to_vec()))
            .transpose()
            .context(ParseSchemaName)?
            .unwrap_or_else(|| default_schema.to_string());

        let schema_config = schema_config_provider
            .schema_config(&schema)
            .context(GetSchemaConfig)?;

        Ok(Self {
            header,
            router,
            instance,
            catalog,
            schema,
            schema_config,
        })
    }

    #[inline]
    fn catalog(&self) -> &str {
        &self.catalog
    }

    #[inline]
    fn tenant(&self) -> &str {
        &self.schema
    }
}

/// Rpc services manages all grpc services of the server.
pub struct RpcServices<Q: QueryExecutor + 'static> {
    serve_addr: SocketAddr,
    rpc_server: StorageServiceServer<StorageServiceImpl<Q>>,
    meta_rpc_server: Option<MetaEventServiceServer<MetaServiceImpl<Q>>>,
    runtime: Arc<Runtime>,
    stop_tx: Option<Sender<()>>,
    join_handle: Option<JoinHandle<()>>,
}

impl<Q: QueryExecutor + 'static> RpcServices<Q> {
    pub async fn start(&mut self) -> Result<()> {
        let rpc_server = self.rpc_server.clone();
        let meta_rpc_server = self.meta_rpc_server.clone();
        let serve_addr = self.serve_addr;
        let (stop_tx, stop_rx) = oneshot::channel();
        let join_handle = self.runtime.spawn(async move {
            info!("Grpc server starts listening on {}", serve_addr);

            let mut router = Server::builder().add_service(rpc_server);

            if let Some(s) = meta_rpc_server {
                info!("Grpc server serves meta rpc service");
                router = router.add_service(s);
            };

            let serve_res = router
                .serve_with_shutdown(serve_addr, stop_rx.map(drop))
                .await;

            warn!("Grpc server stops serving, exit result:{:?}", serve_res);
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
    runtimes: Option<Arc<EngineRuntimes>>,
    instance: Option<InstanceRef<Q>>,
    router: Option<RouterRef>,
    cluster: Option<ClusterRef>,
    schema_config_provider: Option<SchemaConfigProviderRef>,
}

impl<Q> Builder<Q> {
    pub fn new() -> Self {
        Self {
            endpoint: "0.0.0.0:8381".to_string(),
            runtimes: None,
            instance: None,
            router: None,
            cluster: None,
            schema_config_provider: None,
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

    pub fn router(mut self, router: RouterRef) -> Self {
        self.router = Some(router);
        self
    }

    // Cluster is an optional field for building [RpcServices].
    pub fn cluster(mut self, cluster: Option<ClusterRef>) -> Self {
        self.cluster = cluster;
        self
    }

    pub fn schema_config_provider(mut self, provider: SchemaConfigProviderRef) -> Self {
        self.schema_config_provider = Some(provider);
        self
    }
}

impl<Q: QueryExecutor + 'static> Builder<Q> {
    pub fn build(self) -> Result<RpcServices<Q>> {
        let runtimes = self.runtimes.context(MissingRuntimes)?;
        let instance = self.instance.context(MissingInstance)?;
        let router = self.router.context(MissingRouter)?;
        let schema_config_provider = self
            .schema_config_provider
            .context(MissingSchemaConfigProvider)?;

        let meta_rpc_server = self.cluster.map(|v| {
            let meta_service = MetaServiceImpl {
                cluster: v,
                instance: instance.clone(),
                runtime: runtimes.meta_runtime.clone(),
            };
            MetaEventServiceServer::new(meta_service)
        });

        let bg_runtime = runtimes.bg_runtime.clone();
        let storage_service = StorageServiceImpl {
            router,
            instance,
            runtimes,
            schema_config_provider,
        };
        let rpc_server = StorageServiceServer::new(storage_service);

        let serve_addr = self.endpoint.parse().context(InvalidRpcServeAddr)?;

        Ok(RpcServices {
            serve_addr,
            rpc_server,
            meta_rpc_server,
            runtime: bg_runtime,
            stop_tx: None,
            join_handle: None,
        })
    }
}

fn build_err_header(err: ServerError) -> ResponseHeader {
    ResponseHeader {
        code: err.code().as_u16() as u32,
        error: err.error_message(),
    }
}

fn build_ok_header() -> ResponseHeader {
    ResponseHeader {
        code: StatusCode::OK.as_u16() as u32,
        ..Default::default()
    }
}

#[derive(Clone)]
struct MetaServiceImpl<Q: QueryExecutor + 'static> {
    cluster: ClusterRef,
    instance: InstanceRef<Q>,
    runtime: Arc<Runtime>,
}

#[async_trait]
impl<Q: QueryExecutor + 'static> MetaEventService for MetaServiceImpl<Q> {
    // TODO: use macro to remove the boilerplate codes.
    async fn open_shard(
        &self,
        request: tonic::Request<OpenShardRequest>,
    ) -> std::result::Result<tonic::Response<OpenShardResponse>, tonic::Status> {
        let cluster = self.cluster.clone();
        let catalog_manager = self.instance.catalog_manager.clone();
        let table_engine = self.instance.table_engine.clone();

        let handle = self.runtime.spawn(async move {
            // FIXME: Data race about the operations on the shards should be taken into
            // considerations.

            let request = request.into_inner();
            let tables_of_shard = cluster
                .open_shard(&request)
                .await
                .map_err(|e| Box::new(e) as _)
                .context(ErrWithCause {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    msg: "fail to open shards in cluster",
                })?;

            let default_catalog_name = catalog_manager.default_catalog_name();
            let default_catalog = catalog_manager
                .catalog_by_name(default_catalog_name)
                .map_err(|e| Box::new(e) as _)
                .context(ErrWithCause {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    msg: "fail to get default catalog",
                })?
                .context(ErrNoCause {
                    code: StatusCode::NOT_FOUND,
                    msg: "default catalog is not found",
                })?;

            let shard_info = tables_of_shard.shard_info;
            let opts = OpenOptions { table_engine };
            for table in tables_of_shard.tables {
                let schema = default_catalog
                    .schema_by_name(&table.schema_name)
                    .map_err(|e| Box::new(e) as _)
                    .with_context(|| ErrWithCause {
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: format!("fail to get schema of table, table_info:{:?}", table),
                    })?
                    .with_context(|| ErrNoCause {
                        code: StatusCode::NOT_FOUND,
                        msg: format!("schema of table is not found, table_info:{:?}", table),
                    })?;

                let open_request = OpenTableRequest {
                    catalog_name: default_catalog_name.to_string(),
                    schema_name: table.schema_name,
                    schema_id: SchemaId::from(table.schema_id),
                    table_name: table.name.clone(),
                    table_id: TableId::new(table.id),
                    engine: table_engine::ANALYTIC_ENGINE_TYPE.to_string(),
                    shard_id: shard_info.id,
                };
                schema
                    .open_table(open_request.clone(), opts.clone())
                    .await
                    .map_err(|e| Box::new(e) as _)
                    .with_context(|| ErrWithCause {
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: format!("fail to open table, open_request:{:?}", open_request),
                    })?
                    .with_context(|| ErrNoCause {
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: format!("no table is opened, open_request:{:?}", open_request),
                    })?;
            }

            Ok(())
        });

        let res = handle
            .await
            .map_err(|e| Box::new(e) as _)
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "fail to join task",
            });

        let mut resp = OpenShardResponse::default();
        match res {
            Ok(Ok(_)) => {
                resp.header = Some(build_ok_header());
            }
            Ok(Err(e)) | Err(e) => {
                resp.header = Some(build_err_header(e));
            }
        };

        Ok(tonic::Response::new(resp))
    }

    async fn close_shard(
        &self,
        request: tonic::Request<CloseShardRequest>,
    ) -> std::result::Result<tonic::Response<CloseShardResponse>, tonic::Status> {
        let cluster = self.cluster.clone();
        let catalog_manager = self.instance.catalog_manager.clone();
        let table_engine = self.instance.table_engine.clone();

        let handle = self.runtime.spawn(async move {
            let request = request.into_inner();
            let tables_of_shard = cluster
                .close_shard(&request)
                .await
                .map_err(|e| Box::new(e) as _)
                .context(ErrWithCause {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    msg: "fail to close shards in cluster",
                })?;

            let default_catalog_name = catalog_manager.default_catalog_name();
            let default_catalog = catalog_manager
                .catalog_by_name(default_catalog_name)
                .map_err(|e| Box::new(e) as _)
                .context(ErrWithCause {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    msg: "fail to get default catalog",
                })?
                .context(ErrNoCause {
                    code: StatusCode::NOT_FOUND,
                    msg: "default catalog is not found",
                })?;

            let opts = CloseOptions { table_engine };
            for table in tables_of_shard.tables {
                let schema = default_catalog
                    .schema_by_name(&table.schema_name)
                    .map_err(|e| Box::new(e) as _)
                    .with_context(|| ErrWithCause {
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: format!("fail to get schema of table, table_info:{:?}", table),
                    })?
                    .with_context(|| ErrNoCause {
                        code: StatusCode::NOT_FOUND,
                        msg: format!("schema of table is not found, table_info:{:?}", table),
                    })?;

                let close_request = CloseTableRequest {
                    catalog_name: default_catalog_name.to_string(),
                    schema_name: table.schema_name,
                    schema_id: SchemaId::from(table.schema_id),
                    table_name: table.name.clone(),
                    table_id: TableId::new(table.id),
                    engine: table_engine::ANALYTIC_ENGINE_TYPE.to_string(),
                };
                schema
                    .close_table(close_request.clone(), opts.clone())
                    .await
                    .map_err(|e| Box::new(e) as _)
                    .with_context(|| ErrWithCause {
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: format!("fail to close table, close_request:{:?}", close_request),
                    })?;
            }

            Ok(())
        });

        let res = handle
            .await
            .map_err(|e| Box::new(e) as _)
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "fail to join task",
            });

        let mut resp = CloseShardResponse::default();
        match res {
            Ok(Ok(_)) => {
                resp.header = Some(build_ok_header());
            }
            Ok(Err(e)) | Err(e) => {
                resp.header = Some(build_err_header(e));
            }
        };

        Ok(tonic::Response::new(resp))
    }

    async fn create_table_on_shard(
        &self,
        request: tonic::Request<CreateTableOnShardRequest>,
    ) -> std::result::Result<tonic::Response<CreateTableOnShardResponse>, tonic::Status> {
        let cluster = self.cluster.clone();
        let handle = self.runtime.spawn(async move {
            let request = request.into_inner();
            cluster
                .create_table_on_shard(&request)
                .await
                .map_err(|e| Box::new(e) as _)
                .context(ErrWithCause {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    msg: format!(
                        "fail to create table on shard in cluster, req:{:?}",
                        request
                    ),
                })
        });

        let res = handle
            .await
            .map_err(|e| Box::new(e) as _)
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "fail to join task",
            });

        let mut resp = CreateTableOnShardResponse::default();
        match res {
            Ok(Ok(_)) => {
                resp.header = Some(build_ok_header());
            }
            Ok(Err(e)) | Err(e) => {
                resp.header = Some(build_err_header(e));
            }
        };

        Ok(tonic::Response::new(resp))
    }

    async fn drop_table_on_shard(
        &self,
        request: tonic::Request<DropTableOnShardRequest>,
    ) -> std::result::Result<tonic::Response<DropTableOnShardResponse>, tonic::Status> {
        let cluster = self.cluster.clone();
        let handle = self.runtime.spawn(async move {
            let request = request.into_inner();
            cluster
                .drop_table_on_shard(&request)
                .await
                .map_err(|e| Box::new(e) as _)
                .context(ErrWithCause {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    msg: format!("fail to drop table on shard in cluster, req:{:?}", request),
                })
        });

        let res = handle
            .await
            .map_err(|e| Box::new(e) as _)
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "fail to join task",
            });

        let mut resp = DropTableOnShardResponse::default();
        match res {
            Ok(Ok(_)) => {
                resp.header = Some(build_ok_header());
            }
            Ok(Err(e)) | Err(e) => {
                resp.header = Some(build_err_header(e));
            }
        };

        Ok(tonic::Response::new(resp))
    }

    async fn split_shard(
        &self,
        request: tonic::Request<SplitShardRequest>,
    ) -> std::result::Result<tonic::Response<SplitShardResponse>, tonic::Status> {
        info!("Receive split shard request:{:?}", request);
        return Err(tonic::Status::new(tonic::Code::Unimplemented, ""));
    }

    async fn merge_shards(
        &self,
        request: tonic::Request<MergeShardsRequest>,
    ) -> std::result::Result<tonic::Response<MergeShardsResponse>, tonic::Status> {
        info!("Receive merge shards request:{:?}", request);
        return Err(tonic::Status::new(tonic::Code::Unimplemented, ""));
    }

    async fn change_shard_role(
        &self,
        request: tonic::Request<ChangeShardRoleRequest>,
    ) -> std::result::Result<tonic::Response<ChangeShardRoleResponse>, tonic::Status> {
        info!("Receive change shard role request:{:?}", request);
        return Err(tonic::Status::new(tonic::Code::Unimplemented, ""));
    }
}

struct StorageServiceImpl<Q: QueryExecutor + 'static> {
    router: Arc<dyn Router + Send + Sync>,
    instance: InstanceRef<Q>,
    runtimes: Arc<EngineRuntimes>,
    schema_config_provider: SchemaConfigProviderRef,
}

impl<Q: QueryExecutor + 'static> Clone for StorageServiceImpl<Q> {
    fn clone(&self) -> Self {
        Self {
            router: self.router.clone(),
            instance: self.instance.clone(),
            runtimes: self.runtimes.clone(),
            schema_config_provider: self.schema_config_provider.clone(),
        }
    }
}

macro_rules! handle_request {
    ($mod_name: ident, $handle_fn: ident, $req_ty: ident, $resp_ty: ident) => {
        paste! {
            async fn [<$mod_name _internal>] (
                &self,
                request: tonic::Request<$req_ty>,
            ) -> std::result::Result<tonic::Response<$resp_ty>, tonic::Status> {
                let begin_instant = Instant::now();

                let router = self.router.clone();
                let header = RequestHeader::from(request.metadata());
                let instance = self.instance.clone();

                // The future spawned by tokio cannot be executed by other executor/runtime, so

                let runtime = match stringify!($mod_name) {
                    "query" => &self.runtimes.read_runtime,
                    "write" => &self.runtimes.write_runtime,
                    _ => &self.runtimes.bg_runtime,
                };

                let schema_config_provider = self.schema_config_provider.clone();
                // we need to pass the result via channel
                let join_handle = runtime.spawn(async move {
                    let handler_ctx =
                        HandlerContext::new(header, router, instance, &schema_config_provider)
                            .map_err(|e| Box::new(e) as _)
                            .context(ErrWithCause {
                                code: StatusCode::BAD_REQUEST,
                                msg: "Invalid header",
                            })?;
                    $mod_name::$handle_fn(&handler_ctx, request.into_inner())
                        .await
                        .map_err(|e| {
                            error!(
                                "Failed to handle request, mod:{}, handler:{}, err:{}",
                                stringify!($mod_name),
                                stringify!($handle_fn),
                                e
                            );
                            e
                        })
                });

                GRPC_HANDLER_DURATION_HISTOGRAM_VEC
                    .$handle_fn
                    .observe(begin_instant.saturating_elapsed().as_secs_f64());

                let res = join_handle
                    .await
                    .map_err(|e| Box::new(e) as _)
                    .context(ErrWithCause {
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: "fail to join the spawn task",
                    });

                let resp = match res {
                    Ok(Ok(v)) => v,
                    Ok(Err(e)) | Err(e) => {
                        let mut resp = $resp_ty::default();
                        let header = build_err_header(e);
                        resp.header = Some(header);
                        resp
                    },
                };

                Ok(tonic::Response::new(resp))
            }
        }
    };
}

impl<Q: QueryExecutor + 'static> StorageServiceImpl<Q> {
    handle_request!(route, handle_route, RouteRequest, RouteResponse);

    handle_request!(write, handle_write, WriteRequest, WriteResponse);

    handle_request!(query, handle_query, QueryRequest, QueryResponse);

    handle_request!(
        prom_query,
        handle_query,
        PrometheusQueryRequest,
        PrometheusQueryResponse
    );

    async fn stream_write_internal(
        &self,
        request: tonic::Request<tonic::Streaming<WriteRequest>>,
    ) -> ServerResult<WriteResponse> {
        let begin_instant = Instant::now();
        let router = self.router.clone();
        let header = RequestHeader::from(request.metadata());
        let instance = self.instance.clone();
        let schema_config_provider = self.schema_config_provider.clone();

        let handler_ctx = HandlerContext::new(header, router, instance, &schema_config_provider)
            .map_err(|e| Box::new(e) as _)
            .context(ErrWithCause {
                code: StatusCode::BAD_REQUEST,
                msg: "Invalid header",
            })?;

        let mut total_success = 0;
        let mut resp = WriteResponse::default();
        let mut has_err = false;
        let mut stream = request.into_inner();
        while let Some(req) = stream.next().await {
            let write_req = req.map_err(|e| Box::new(e) as _).context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "Failed to fetch request",
            })?;

            let write_result = write::handle_write(
                &handler_ctx,
                write_req,
            )
            .await
            .map_err(|e| {
                error!("Failed to handle request, mod:stream_write, handler:handle_stream_write, err:{}", e);
                e
            });

            match write_result {
                Ok(write_resp) => total_success += write_resp.success,
                Err(e) => {
                    resp.header = Some(build_err_header(e));
                    has_err = true;
                    break;
                }
            }
        }

        if !has_err {
            resp.header = Some(build_ok_header());
            resp.success = total_success as u32;
        }

        GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .handle_stream_write
            .observe(begin_instant.saturating_elapsed().as_secs_f64());

        ServerResult::Ok(resp)
    }

    async fn stream_query_internal(
        &self,
        request: tonic::Request<QueryRequest>,
    ) -> ServerResult<ReceiverStream<ServerResult<QueryResponse>>> {
        let begin_instant = Instant::now();
        let router = self.router.clone();
        let header = RequestHeader::from(request.metadata());
        let instance = self.instance.clone();
        let schema_config_provider = self.schema_config_provider.clone();

        let (tx, rx) = mpsc::channel(STREAM_QUERY_CHANNEL_LEN);
        let _: JoinHandle<ServerResult<()>> = self.runtimes.read_runtime.spawn(async move {
            let handler_ctx = HandlerContext::new(header, router, instance, &schema_config_provider)
                .map_err(|e| Box::new(e) as _)
                .context(ErrWithCause {
                    code: StatusCode::BAD_REQUEST,
                    msg: "Invalid header",
                })?;

            let query_req = request.into_inner();
            let output = query::fetch_query_output(&handler_ctx, &query_req)
                    .await
                    .map_err(|e| {
                        error!("Failed to handle request, mod:stream_query, handler:handle_stream_query, err:{}", e);
                        e
                    })?;
            if let Some(batch) = query::get_record_batch(&output) {
                for i in 0..batch.len() {
                    let resp = query::convert_records(&batch[i..i + 1]);
                    if tx.send(resp).await.is_err() {
                        error!("Failed to send handler result, mod:stream_query, handler:handle_stream_query");
                        break;
                    }
                }
            } else {
                let resp = QueryResponse {
                    header: Some(build_ok_header()),
                    ..Default::default()
                };

                if tx.send(ServerResult::Ok(resp)).await.is_err() {
                    error!(
                        "Failed to send handler result, mod:stream_query, handler:handle_stream_query"
                    );
                }
            }

            Ok(())
        });

        GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .handle_stream_query
            .observe(begin_instant.saturating_elapsed().as_secs_f64());

        ServerResult::Ok(ReceiverStream::new(rx))
    }
}

#[async_trait]
impl<Q: QueryExecutor + 'static> StorageService for StorageServiceImpl<Q> {
    type StreamQueryStream = BoxStream<'static, std::result::Result<QueryResponse, tonic::Status>>;

    async fn route(
        &self,
        request: tonic::Request<RouteRequest>,
    ) -> std::result::Result<tonic::Response<RouteResponse>, tonic::Status> {
        self.route_internal(request).await
    }

    async fn write(
        &self,
        request: tonic::Request<WriteRequest>,
    ) -> std::result::Result<tonic::Response<WriteResponse>, tonic::Status> {
        self.write_internal(request).await
    }

    async fn query(
        &self,
        request: tonic::Request<QueryRequest>,
    ) -> std::result::Result<tonic::Response<QueryResponse>, tonic::Status> {
        self.query_internal(request).await
    }

    async fn prom_query(
        &self,
        request: tonic::Request<PrometheusQueryRequest>,
    ) -> std::result::Result<tonic::Response<PrometheusQueryResponse>, tonic::Status> {
        self.prom_query_internal(request).await
    }

    async fn stream_write(
        &self,
        request: tonic::Request<tonic::Streaming<WriteRequest>>,
    ) -> std::result::Result<tonic::Response<WriteResponse>, tonic::Status> {
        let resp = match self.stream_write_internal(request).await {
            Ok(resp) => resp,
            Err(e) => WriteResponse {
                header: Some(build_err_header(e)),
                ..Default::default()
            },
        };
        Ok(tonic::Response::new(resp))
    }

    async fn stream_query(
        &self,
        request: tonic::Request<QueryRequest>,
    ) -> std::result::Result<tonic::Response<Self::StreamQueryStream>, tonic::Status> {
        match self.stream_query_internal(request).await {
            Ok(stream) => {
                let new_stream: Self::StreamQueryStream = Box::pin(stream.map(|res| match res {
                    Ok(resp) => Ok(resp),
                    Err(e) => {
                        let resp = QueryResponse {
                            header: Some(build_err_header(e)),
                            ..Default::default()
                        };
                        Ok(resp)
                    }
                }));

                Ok(tonic::Response::new(new_stream))
            }
            Err(e) => {
                let resp = QueryResponse {
                    header: Some(build_err_header(e)),
                    ..Default::default()
                };
                let stream = stream::once(async { Ok(resp) });
                Ok(tonic::Response::new(Box::pin(stream)))
            }
        }
    }
}

/// Create CreateTablePlan from a write metric.
// The caller must ENSURE that the HandlerContext's schema_config is not None.
pub fn write_metric_to_create_table_plan<Q: QueryExecutor + 'static>(
    ctx: &HandlerContext<Q>,
    write_metric: &WriteMetric,
) -> Result<CreateTablePlan> {
    let schema_config = ctx.schema_config.unwrap();
    Ok(CreateTablePlan {
        engine: schema_config.default_engine_type.clone(),
        if_not_exists: true,
        table: write_metric.metric.clone(),
        table_schema: build_schema_from_metric(schema_config, write_metric)?,
        options: HashMap::default(),
    })
}

fn build_column_schema(
    column_name: &str,
    data_type: DatumKind,
    is_tag: bool,
) -> Result<ColumnSchema> {
    let builder = column_schema::Builder::new(column_name.to_string(), data_type)
        .is_nullable(true)
        .is_tag(is_tag);

    builder.build().context(BuildColumnSchema { column_name })
}

fn build_schema_from_metric(schema_config: &SchemaConfig, metric: &WriteMetric) -> Result<Schema> {
    let WriteMetric {
        metric: table_name,
        field_names,
        tag_names,
        entries: write_entries,
    } = metric;

    let mut schema_builder =
        SchemaBuilder::with_capacity(field_names.len()).auto_increment_column_id(true);

    ensure!(
        !write_entries.is_empty(),
        InvalidArgument {
            msg: format!("Empty write entires to write table:{}", table_name),
        }
    );

    let mut name_column_map: BTreeMap<_, ColumnSchema> = BTreeMap::new();
    for write_entry in write_entries {
        // parse tags
        for tag in &write_entry.tags {
            let name_index = tag.name_index as usize;
            ensure!(
                name_index < tag_names.len(),
                InvalidArgument {
                    msg: format!(
                        "tag index {} is not found in tag_names:{:?}, table:{}",
                        name_index, tag_names, table_name,
                    ),
                }
            );

            let tag_name = &tag_names[name_index];

            let tag_value = tag
                .value
                .as_ref()
                .with_context(|| InvalidArgument {
                    msg: format!(
                        "Tag({}) value is needed, table_name:{} ",
                        tag_name, table_name
                    ),
                })?
                .value
                .as_ref()
                .with_context(|| InvalidArgument {
                    msg: format!(
                        "Tag({}) value type is not supported, table_name:{}",
                        tag_name, table_name
                    ),
                })?;

            let data_type = try_get_data_type_from_value(tag_value)?;

            if let Some(column_schema) = name_column_map.get(tag_name) {
                ensure_data_type_compatible(table_name, tag_name, true, data_type, column_schema)?;
            }
            let column_schema = build_column_schema(tag_name, data_type, true)?;
            name_column_map.insert(tag_name, column_schema);
        }

        // parse fields
        for field_group in &write_entry.field_groups {
            for field in &field_group.fields {
                if (field.name_index as usize) < field_names.len() {
                    let field_name = &field_names[field.name_index as usize];
                    let field_value = field
                        .value
                        .as_ref()
                        .with_context(|| InvalidArgument {
                            msg: format!(
                                "Field({}) value is needed, table:{}",
                                field_name, table_name
                            ),
                        })?
                        .value
                        .as_ref()
                        .with_context(|| InvalidArgument {
                            msg: format!(
                                "Field({}) value type is not supported, table:{}",
                                field_name, table_name
                            ),
                        })?;

                    let data_type = try_get_data_type_from_value(field_value)?;

                    if let Some(column_schema) = name_column_map.get(field_name) {
                        ensure_data_type_compatible(
                            table_name,
                            field_name,
                            false,
                            data_type,
                            column_schema,
                        )?;
                    }

                    let column_schema = build_column_schema(field_name, data_type, false)?;
                    name_column_map.insert(field_name, column_schema);
                }
            }
        }
    }

    // Timestamp column will be the last column
    let timestamp_column_schema = column_schema::Builder::new(
        schema_config.default_timestamp_column_name.clone(),
        DatumKind::Timestamp,
    )
    .is_nullable(false)
    .build()
    .context(InvalidColumnSchema {
        column_name: TSID_COLUMN,
    })?;

    // Use (timestamp, tsid) as primary key.
    let tsid_column_schema =
        column_schema::Builder::new(TSID_COLUMN.to_string(), DatumKind::UInt64)
            .is_nullable(false)
            .build()
            .context(InvalidColumnSchema {
                column_name: TSID_COLUMN,
            })?;

    schema_builder = schema_builder
        .enable_tsid_primary_key(true)
        .add_key_column(timestamp_column_schema)
        .with_context(|| BuildTableSchema { metric: table_name })?
        .add_key_column(tsid_column_schema)
        .with_context(|| BuildTableSchema { metric: table_name })?;

    for col in name_column_map.into_values() {
        schema_builder = schema_builder
            .add_normal_column(col)
            .with_context(|| BuildTableSchema { metric: table_name })?;
    }

    schema_builder.build().context(BuildTableSchema {
        metric: &metric.metric,
    })
}

fn ensure_data_type_compatible(
    table_name: &str,
    column_name: &str,
    is_tag: bool,
    data_type: DatumKind,
    column_schema: &ColumnSchema,
) -> Result<()> {
    ensure!(
        column_schema.is_tag == is_tag,
        InvalidArgument {
            msg: format!(
                "Duplicated column: {} in fields and tags for table: {}",
                column_name, table_name,
            ),
        }
    );
    ensure!(
        column_schema.data_type == data_type,
        InvalidArgument {
            msg: format!(
                "Column: {} in table: {} data type is not same, expected: {}, actual: {}",
                column_name, table_name, column_schema.data_type, data_type,
            ),
        }
    );
    Ok(())
}

fn try_get_data_type_from_value(value: &Value) -> Result<DatumKind> {
    match value {
        Value::Float64Value(_) => Ok(DatumKind::Double),
        Value::StringValue(_) => Ok(DatumKind::String),
        Value::Int64Value(_) => Ok(DatumKind::Int64),
        Value::Float32Value(_) => Ok(DatumKind::Float),
        Value::Int32Value(_) => Ok(DatumKind::Int32),
        Value::Int16Value(_) => Ok(DatumKind::Int16),
        Value::Int8Value(_) => Ok(DatumKind::Int8),
        Value::BoolValue(_) => Ok(DatumKind::Boolean),
        Value::Uint64Value(_) => Ok(DatumKind::UInt64),
        Value::Uint32Value(_) => Ok(DatumKind::UInt32),
        Value::Uint16Value(_) => Ok(DatumKind::UInt16),
        Value::Uint8Value(_) => Ok(DatumKind::UInt8),
        Value::TimestampValue(_) => Ok(DatumKind::Timestamp),
        Value::VarbinaryValue(_) => Ok(DatumKind::Varbinary),
    }
}

#[cfg(test)]
mod tests {
    use ceresdbproto::storage::{value, Field, FieldGroup, Tag, Value, WriteEntry, WriteMetric};
    use cluster::config::SchemaConfig;
    use common_types::datum::DatumKind;

    use super::*;

    const TAG1: &str = "host";
    const TAG2: &str = "idc";
    const FIELD1: &str = "cpu";
    const FIELD2: &str = "memory";
    const FIELD3: &str = "log";
    const FIELD4: &str = "ping_ok";
    const METRIC: &str = "pod_system_metric";
    const TIMESTAMP_COLUMN: &str = "custom_timestamp";

    fn make_tag(name_index: u32, val: &str) -> Tag {
        Tag {
            name_index,
            value: Some(Value {
                value: Some(value::Value::StringValue(val.to_string())),
            }),
        }
    }

    fn make_field(name_index: u32, val: value::Value) -> Field {
        Field {
            name_index,
            value: Some(Value { value: Some(val) }),
        }
    }

    fn generate_write_metric() -> WriteMetric {
        let tag1 = make_tag(0, "test.host");
        let tag2 = make_tag(1, "test.idc");
        let tags = vec![tag1, tag2];

        let field1 = make_field(0, value::Value::Float64Value(100.0));
        let field2 = make_field(1, value::Value::Float64Value(1024.0));
        let field3 = make_field(2, value::Value::StringValue("test log".to_string()));
        let field4 = make_field(3, value::Value::BoolValue(true));

        let field_group1 = FieldGroup {
            timestamp: 1000,
            fields: vec![field1.clone(), field4],
        };
        let field_group2 = FieldGroup {
            timestamp: 2000,
            fields: vec![field1, field2],
        };
        let field_group3 = FieldGroup {
            timestamp: 3000,
            fields: vec![field3],
        };

        let write_entry = WriteEntry {
            tags,
            field_groups: vec![field_group1, field_group2, field_group3],
        };

        let tag_names = vec![TAG1.to_string(), TAG2.to_string()];
        let field_names = vec![
            FIELD1.to_string(),
            FIELD2.to_string(),
            FIELD3.to_string(),
            FIELD4.to_string(),
        ];

        WriteMetric {
            metric: METRIC.to_string(),
            tag_names,
            field_names,
            entries: vec![write_entry],
        }
    }

    #[test]
    fn test_build_schema_from_metric() {
        let schema_config = SchemaConfig {
            auto_create_tables: true,
            default_timestamp_column_name: TIMESTAMP_COLUMN.to_string(),
            ..SchemaConfig::default()
        };
        let write_metric = generate_write_metric();

        let schema = build_schema_from_metric(&schema_config, &write_metric);
        assert!(schema.is_ok());

        let schema = schema.unwrap();

        assert_eq!(8, schema.num_columns());
        assert_eq!(2, schema.num_key_columns());
        assert_eq!(TIMESTAMP_COLUMN, schema.timestamp_name());
        let tsid = schema.tsid_column();
        assert!(tsid.is_some());

        let key_columns = schema.key_columns();
        assert_eq!(2, key_columns.len());
        assert_eq!(TIMESTAMP_COLUMN, key_columns[0].name);
        assert_eq!("tsid", key_columns[1].name);

        let columns = schema.normal_columns();
        assert_eq!(6, columns.len());

        // sorted by column names because of btree
        assert_eq!(FIELD1, columns[0].name);
        assert!(!columns[0].is_tag);
        assert_eq!(DatumKind::Double, columns[0].data_type);
        assert_eq!(TAG1, columns[1].name);
        assert!(columns[1].is_tag);
        assert_eq!(DatumKind::String, columns[1].data_type);
        assert_eq!(TAG2, columns[2].name);
        assert!(columns[2].is_tag);
        assert_eq!(DatumKind::String, columns[2].data_type);
        assert_eq!(FIELD3, columns[3].name);
        assert!(!columns[3].is_tag);
        assert_eq!(DatumKind::String, columns[3].data_type);
        assert_eq!(FIELD2, columns[4].name);
        assert!(!columns[4].is_tag);
        assert_eq!(DatumKind::Double, columns[4].data_type);
        assert_eq!(FIELD4, columns[5].name);
        assert!(!columns[5].is_tag);
        assert_eq!(DatumKind::Boolean, columns[5].data_type);

        for column in columns {
            assert!(column.is_nullable);
        }
    }
}
