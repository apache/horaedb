// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// Storage rpc service implement.

use std::{
    collections::HashMap,
    stringify,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use ceresdbproto::storage::{
    storage_service_server::StorageService, PrometheusQueryRequest, PrometheusQueryResponse,
    RouteRequest, RouteResponse, SqlQueryRequest, SqlQueryResponse, WriteRequest, WriteResponse,
};
use common_util::{error::BoxError, time::InstantExt};
use futures::stream::{self, BoxStream, StreamExt};
use http::StatusCode;
use interpreters::interpreter::Output;
use log::{error, warn};
use paste::paste;
use query_engine::executor::Executor as QueryExecutor;
use router::{Router, RouterRef};
use snafu::ResultExt;
use table_engine::engine::EngineRuntimes;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::{KeyAndValueRef, MetadataMap};

use self::{
    error::Error,
    sql_query::{QueryResponseBuilder, QueryResponseWriter},
};
use crate::{
    grpc::{
        forward::ForwarderRef,
        hotspot::HotspotRecorder,
        metrics::GRPC_HANDLER_DURATION_HISTOGRAM_VEC,
        storage_service::error::{ErrNoCause, ErrWithCause, Result},
    },
    instance::InstanceRef,
    schema_config_provider::SchemaConfigProviderRef,
};

pub(crate) mod error;
mod prom_query;
mod route;
mod sql_query;
pub(crate) mod write;

const STREAM_QUERY_CHANNEL_LEN: usize = 20;

/// Rpc request header
/// Tenant/token will be saved in header in future
#[allow(dead_code)]
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
    #[allow(dead_code)]
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
    schema_config_provider: &'a SchemaConfigProviderRef,
    forwarder: Option<ForwarderRef>,
    timeout: Option<Duration>,
    resp_compress_min_length: usize,
    auto_create_table: bool,
}

impl<'a, Q> HandlerContext<'a, Q> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        header: RequestHeader,
        router: Arc<dyn Router + Sync + Send>,
        instance: InstanceRef<Q>,
        schema_config_provider: &'a SchemaConfigProviderRef,
        forwarder: Option<ForwarderRef>,
        timeout: Option<Duration>,
        resp_compress_min_length: usize,
        auto_create_table: bool,
    ) -> Self {
        // catalog is not exposed to protocol layer
        let catalog = instance.catalog_manager.default_catalog_name().to_string();

        Self {
            header,
            router,
            instance,
            catalog,
            schema_config_provider,
            forwarder,
            timeout,
            resp_compress_min_length,
            auto_create_table,
        }
    }

    #[inline]
    fn catalog(&self) -> &str {
        &self.catalog
    }
}

#[derive(Clone)]
pub struct StorageServiceImpl<Q: QueryExecutor + 'static> {
    pub router: Arc<dyn Router + Send + Sync>,
    pub instance: InstanceRef<Q>,
    pub runtimes: Arc<EngineRuntimes>,
    pub schema_config_provider: SchemaConfigProviderRef,
    pub forwarder: Option<ForwarderRef>,
    pub timeout: Option<Duration>,
    pub resp_compress_min_length: usize,
    pub auto_create_table: bool,
    pub hotspot_recorder: Arc<HotspotRecorder>,
}

macro_rules! handle_request {
    ($mod_name: ident, $handle_fn: ident, $req_ty: ident, $resp_ty: ident, $hotspot_record_fn: ident) => {
        paste! {
            async fn [<$mod_name _internal>] (
                &self,
                request: tonic::Request<$req_ty>,
            ) -> std::result::Result<tonic::Response<$resp_ty>, tonic::Status> {
                let instant = Instant::now();

                let router = self.router.clone();
                let header = RequestHeader::from(request.metadata());
                let instance = self.instance.clone();
                let forwarder = self.forwarder.clone();
                let timeout = self.timeout;
                let resp_compress_min_length = self.resp_compress_min_length;
                let auto_create_table = self.auto_create_table;
                let hotspot_recorder = self.hotspot_recorder.clone();

                // The future spawned by tokio cannot be executed by other executor/runtime, so
                let runtime = match stringify!($mod_name) {
                    "sql_query" | "prom_query" => &self.runtimes.read_runtime,
                    "write" => &self.runtimes.write_runtime,
                    _ => &self.runtimes.bg_runtime,
                };

                let schema_config_provider = self.schema_config_provider.clone();
                // we need to pass the result via channel
                let join_handle = runtime.spawn(async move {
                    let req = request.into_inner();
                    if req.context.is_none() {
                        ErrNoCause {
                            code: StatusCode::BAD_REQUEST,
                            msg: "database is not set",
                        }
                        .fail()?
                    }

                    // record hotspot
                    hotspot_recorder.$hotspot_record_fn(&req);

                    let handler_ctx =
                        HandlerContext::new(header, router, instance, &schema_config_provider, forwarder, timeout, resp_compress_min_length, auto_create_table);
                    $mod_name::$handle_fn(&handler_ctx, req)
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

                let res = join_handle
                    .await
                    .box_err()
                    .context(ErrWithCause {
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: "fail to join the spawn task",
                    });

                let resp = match res {
                    Ok(Ok(v)) => v,
                    Ok(Err(e)) | Err(e) => {
                        let mut resp = $resp_ty::default();
                        let header = error::build_err_header(e);
                        resp.header = Some(header);
                        resp
                    },
                };

                GRPC_HANDLER_DURATION_HISTOGRAM_VEC
                    .$handle_fn
                    .observe(instant.saturating_elapsed().as_secs_f64());
                Ok(tonic::Response::new(resp))
            }
        }
    };
}

impl<Q: QueryExecutor + 'static> StorageServiceImpl<Q> {
    // `RequestContext` is ensured in `handle_request` macro, so handler
    // can just use it with unwrap()

    handle_request!(
        route,
        handle_route,
        RouteRequest,
        RouteResponse,
        inc_route_reqs
    );

    handle_request!(
        write,
        handle_write,
        WriteRequest,
        WriteResponse,
        inc_write_reqs
    );

    handle_request!(
        sql_query,
        handle_query,
        SqlQueryRequest,
        SqlQueryResponse,
        inc_sql_query_reqs
    );

    handle_request!(
        prom_query,
        handle_query,
        PrometheusQueryRequest,
        PrometheusQueryResponse,
        inc_prom_query_reqs
    );

    async fn stream_write_internal(
        &self,
        request: tonic::Request<tonic::Streaming<WriteRequest>>,
    ) -> Result<WriteResponse> {
        let begin_instant = Instant::now();
        let router = self.router.clone();
        let header = RequestHeader::from(request.metadata());
        let instance = self.instance.clone();
        let schema_config_provider = self.schema_config_provider.clone();
        let handler_ctx = HandlerContext::new(
            header,
            router,
            instance,
            &schema_config_provider,
            self.forwarder.clone(),
            self.timeout,
            self.resp_compress_min_length,
            self.auto_create_table,
        );

        let mut total_success = 0;
        let mut resp = WriteResponse::default();
        let mut has_err = false;
        let mut stream = request.into_inner();
        while let Some(req) = stream.next().await {
            let write_req = req.box_err().context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "failed to fetch request",
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
                    resp.header = Some(error::build_err_header(e));
                    has_err = true;
                    break;
                }
            }
        }

        if !has_err {
            resp.header = Some(error::build_ok_header());
            resp.success = total_success;
        }

        GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .handle_stream_write
            .observe(begin_instant.saturating_elapsed().as_secs_f64());

        Ok(resp)
    }

    async fn stream_sql_query_internal(
        &self,
        request: tonic::Request<SqlQueryRequest>,
    ) -> Result<ReceiverStream<Result<SqlQueryResponse>>> {
        let begin_instant = Instant::now();
        let router = self.router.clone();
        let header = RequestHeader::from(request.metadata());
        let instance = self.instance.clone();
        let schema_config_provider = self.schema_config_provider.clone();
        let forwarder = self.forwarder.clone();
        let timeout = self.timeout;
        let resp_compress_min_length = self.resp_compress_min_length;
        let auto_create_table = self.auto_create_table;

        let (tx, rx) = mpsc::channel(STREAM_QUERY_CHANNEL_LEN);
        self.runtimes.read_runtime.spawn(async move {
            let handler_ctx = HandlerContext::new(
                header,
                router,
                instance,
                &schema_config_provider,
                forwarder,
                timeout,
                resp_compress_min_length,
                auto_create_table
            );
            let query_req = request.into_inner();
            let output = sql_query::fetch_query_output(&handler_ctx, &query_req)
                .await
                .map_err(|e| {
                    error!("Failed to handle request, mod:stream_query, handler:handle_stream_query, err:{}", e);
                    e
                })?;
            match output {
                Output::AffectedRows(rows) => {
                    let resp = QueryResponseBuilder::with_ok_header().build_with_affected_rows(rows);
                    if tx.send(Ok(resp)).await.is_err() {
                        error!("Failed to send affected rows resp in stream query");
                    }
                }
                Output::Records(batches) => {
                    for batch in &batches {
                        let resp = {
                            let mut writer = QueryResponseWriter::new(resp_compress_min_length);
                            writer.write(batch)?;
                            writer.finish()
                        };

                        if tx.send(resp).await.is_err() {
                            error!("Failed to send record batches resp in stream query");
                            break;
                        }
                    }
                }
            }

            Ok::<(), Error>(())
        });

        GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .handle_stream_query
            .observe(begin_instant.saturating_elapsed().as_secs_f64());

        Ok(ReceiverStream::new(rx))
    }
}

#[async_trait]
impl<Q: QueryExecutor + 'static> StorageService for StorageServiceImpl<Q> {
    type StreamSqlQueryStream =
        BoxStream<'static, std::result::Result<SqlQueryResponse, tonic::Status>>;

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

    async fn stream_write(
        &self,
        request: tonic::Request<tonic::Streaming<WriteRequest>>,
    ) -> std::result::Result<tonic::Response<WriteResponse>, tonic::Status> {
        let resp = match self.stream_write_internal(request).await {
            Ok(resp) => resp,
            Err(e) => WriteResponse {
                header: Some(error::build_err_header(e)),
                ..Default::default()
            },
        };
        Ok(tonic::Response::new(resp))
    }

    async fn sql_query(
        &self,
        request: tonic::Request<SqlQueryRequest>,
    ) -> std::result::Result<tonic::Response<SqlQueryResponse>, tonic::Status> {
        self.sql_query_internal(request).await
    }

    async fn stream_sql_query(
        &self,
        request: tonic::Request<SqlQueryRequest>,
    ) -> std::result::Result<tonic::Response<Self::StreamSqlQueryStream>, tonic::Status> {
        match self.stream_sql_query_internal(request).await {
            Ok(stream) => {
                let new_stream: Self::StreamSqlQueryStream =
                    Box::pin(stream.map(|res| match res {
                        Ok(resp) => Ok(resp),
                        Err(e) => {
                            let resp = SqlQueryResponse {
                                header: Some(error::build_err_header(e)),
                                ..Default::default()
                            };
                            Ok(resp)
                        }
                    }));

                Ok(tonic::Response::new(new_stream))
            }
            Err(e) => {
                let resp = SqlQueryResponse {
                    header: Some(error::build_err_header(e)),
                    ..Default::default()
                };
                let stream = stream::once(async { Ok(resp) });
                Ok(tonic::Response::new(Box::pin(stream)))
            }
        }
    }

    async fn prom_query(
        &self,
        request: tonic::Request<PrometheusQueryRequest>,
    ) -> std::result::Result<tonic::Response<PrometheusQueryResponse>, tonic::Status> {
        self.prom_query_internal(request).await
    }
}
