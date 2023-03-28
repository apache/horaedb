// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

pub(crate) mod error;
#[allow(dead_code)]
mod header;

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use ceresdbproto::{
    common::ResponseHeader,
    storage::{
        storage_service_server::StorageService, PrometheusQueryRequest, PrometheusQueryResponse,
        RouteRequest, RouteResponse, SqlQueryRequest, SqlQueryResponse, WriteRequest,
        WriteResponse,
    },
};
use common_util::time::InstantExt;
use futures::{stream, stream::BoxStream, StreamExt};
use http::StatusCode;
use query_engine::executor::Executor as QueryExecutor;
use table_engine::engine::EngineRuntimes;

use crate::{
    grpc::metrics::GRPC_HANDLER_DURATION_HISTOGRAM_VEC,
    proxy::{Context, Proxy},
};

#[derive(Clone)]
pub struct StorageServiceImpl<Q: QueryExecutor + 'static> {
    pub proxy: Arc<Proxy<Q>>,
    pub runtimes: Arc<EngineRuntimes>,
    pub timeout: Option<Duration>,
}

#[async_trait]
impl<Q: QueryExecutor + 'static> StorageService for StorageServiceImpl<Q> {
    type StreamSqlQueryStream = BoxStream<'static, Result<SqlQueryResponse, tonic::Status>>;

    async fn route(
        &self,
        req: tonic::Request<RouteRequest>,
    ) -> Result<tonic::Response<RouteResponse>, tonic::Status> {
        let begin_instant = Instant::now();

        let resp = self.route_internal(req).await;

        GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .handle_route
            .observe(begin_instant.saturating_elapsed().as_secs_f64());

        resp
    }

    async fn write(
        &self,
        req: tonic::Request<WriteRequest>,
    ) -> Result<tonic::Response<WriteResponse>, tonic::Status> {
        let begin_instant = Instant::now();

        let resp = self.write_internal(req).await;

        GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .handle_write
            .observe(begin_instant.saturating_elapsed().as_secs_f64());

        resp
    }

    async fn sql_query(
        &self,
        req: tonic::Request<SqlQueryRequest>,
    ) -> Result<tonic::Response<SqlQueryResponse>, tonic::Status> {
        let begin_instant = Instant::now();

        let resp = self.sql_query_internal(req).await;

        GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .handle_sql_query
            .observe(begin_instant.saturating_elapsed().as_secs_f64());

        resp
    }

    async fn prom_query(
        &self,
        req: tonic::Request<PrometheusQueryRequest>,
    ) -> Result<tonic::Response<PrometheusQueryResponse>, tonic::Status> {
        let begin_instant = Instant::now();

        let resp = self.prom_query_internal(req).await;

        GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .handle_prom_query
            .observe(begin_instant.saturating_elapsed().as_secs_f64());

        resp
    }

    async fn stream_write(
        &self,
        req: tonic::Request<tonic::Streaming<WriteRequest>>,
    ) -> Result<tonic::Response<WriteResponse>, tonic::Status> {
        let begin_instant = Instant::now();

        let resp = self.stream_write_internal(req).await;

        GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .handle_stream_write
            .observe(begin_instant.saturating_elapsed().as_secs_f64());

        resp
    }

    async fn stream_sql_query(
        &self,
        req: tonic::Request<SqlQueryRequest>,
    ) -> Result<tonic::Response<Self::StreamSqlQueryStream>, tonic::Status> {
        let begin_instant = Instant::now();
        let proxy = self.proxy.clone();
        let ctx = Context {
            runtime: self.runtimes.read_runtime.clone(),
            timeout: self.timeout,
        };
        let stream = Self::stream_sql_query_internal(ctx, proxy, req).await;

        GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .handle_stream_sql_query
            .observe(begin_instant.saturating_elapsed().as_secs_f64());

        stream
    }
}

// TODO: Use macros to simplify duplicate code
impl<Q: QueryExecutor + 'static> StorageServiceImpl<Q> {
    async fn route_internal(
        &self,
        req: tonic::Request<RouteRequest>,
    ) -> Result<tonic::Response<RouteResponse>, tonic::Status> {
        let req = req.into_inner();
        let proxy = self.proxy.clone();
        let ctx = Context {
            runtime: self.runtimes.meta_runtime.clone(),
            timeout: self.timeout,
        };

        let join_handle = self
            .runtimes
            .meta_runtime
            .spawn(async move { proxy.handle_route(ctx, req).await });

        let resp = match join_handle.await {
            Ok(v) => v,
            Err(e) => RouteResponse {
                header: Some(error::build_err_header(
                    StatusCode::INTERNAL_SERVER_ERROR.as_u16() as u32,
                    format!("fail to join the spawn task, err:{e:?}"),
                )),
                ..Default::default()
            },
        };

        Ok(tonic::Response::new(resp))
    }

    async fn write_internal(
        &self,
        req: tonic::Request<WriteRequest>,
    ) -> Result<tonic::Response<WriteResponse>, tonic::Status> {
        let req = req.into_inner();
        let proxy = self.proxy.clone();
        let ctx = Context {
            runtime: self.runtimes.write_runtime.clone(),
            timeout: self.timeout,
        };

        let join_handle = self.runtimes.write_runtime.spawn(async move {
            if req.context.is_none() {
                return WriteResponse {
                    header: Some(error::build_err_header(
                        StatusCode::BAD_REQUEST.as_u16() as u32,
                        "database is not set".to_string(),
                    )),
                    ..Default::default()
                };
            }

            proxy.handle_write(ctx, req).await
        });

        let resp = match join_handle.await {
            Ok(v) => v,
            Err(e) => WriteResponse {
                header: Some(error::build_err_header(
                    StatusCode::INTERNAL_SERVER_ERROR.as_u16() as u32,
                    format!("fail to join the spawn task, err:{e:?}"),
                )),
                ..Default::default()
            },
        };

        Ok(tonic::Response::new(resp))
    }

    async fn sql_query_internal(
        &self,
        req: tonic::Request<SqlQueryRequest>,
    ) -> Result<tonic::Response<SqlQueryResponse>, tonic::Status> {
        let req = req.into_inner();
        let proxy = self.proxy.clone();
        let ctx = Context {
            runtime: self.runtimes.read_runtime.clone(),
            timeout: self.timeout,
        };
        let join_handle = self.runtimes.read_runtime.spawn(async move {
            if req.context.is_none() {
                return SqlQueryResponse {
                    header: Some(error::build_err_header(
                        StatusCode::BAD_REQUEST.as_u16() as u32,
                        "database is not set".to_string(),
                    )),
                    ..Default::default()
                };
            }

            proxy.handle_sql_query(ctx, req).await
        });

        let resp = match join_handle.await {
            Ok(v) => v,
            Err(e) => SqlQueryResponse {
                header: Some(error::build_err_header(
                    StatusCode::INTERNAL_SERVER_ERROR.as_u16() as u32,
                    format!("fail to join the spawn task, err:{e:?}"),
                )),
                ..Default::default()
            },
        };

        Ok(tonic::Response::new(resp))
    }

    async fn prom_query_internal(
        &self,
        req: tonic::Request<PrometheusQueryRequest>,
    ) -> Result<tonic::Response<PrometheusQueryResponse>, tonic::Status> {
        let req = req.into_inner();
        let proxy = self.proxy.clone();
        let ctx = Context {
            runtime: self.runtimes.read_runtime.clone(),
            timeout: self.timeout,
        };
        let join_handle = self.runtimes.read_runtime.spawn(async move {
            if req.context.is_none() {
                return PrometheusQueryResponse {
                    header: Some(error::build_err_header(
                        StatusCode::BAD_REQUEST.as_u16() as u32,
                        "database is not set".to_string(),
                    )),
                    ..Default::default()
                };
            }

            proxy.handle_prom_query(ctx, req).await
        });

        let resp = match join_handle.await {
            Ok(v) => v,
            Err(e) => PrometheusQueryResponse {
                header: Some(error::build_err_header(
                    StatusCode::INTERNAL_SERVER_ERROR.as_u16() as u32,
                    format!("fail to join the spawn task, err:{e:?}"),
                )),
                ..Default::default()
            },
        };
        Ok(tonic::Response::new(resp))
    }

    async fn stream_write_internal(
        &self,
        req: tonic::Request<tonic::Streaming<WriteRequest>>,
    ) -> Result<tonic::Response<WriteResponse>, tonic::Status> {
        let mut total_success = 0;

        let mut stream = req.into_inner();
        let proxy = self.proxy.clone();
        let ctx = Context {
            runtime: self.runtimes.write_runtime.clone(),
            timeout: self.timeout,
        };

        let join_handle = self.runtimes.write_runtime.spawn(async move {
            let mut resp = WriteResponse::default();
            let mut has_err = false;

            while let Some(req) = stream.next().await {
                let write_req = match req {
                    Ok(v) => v,
                    Err(e) => {
                        return WriteResponse {
                            header: Some(error::build_err_header(
                                StatusCode::INTERNAL_SERVER_ERROR.as_u16() as u32,
                                format!("fail to fetch request, err:{e:?}"),
                            )),
                            ..Default::default()
                        };
                    }
                };

                let write_resp = proxy.handle_write(ctx.clone(), write_req).await;

                if let Some(header) = write_resp.header {
                    if header.code != StatusCode::OK.as_u16() as u32 {
                        resp.header = Some(header);
                        has_err = true;
                        break;
                    }
                }
                total_success += write_resp.success;
            }

            if !has_err {
                resp.header = Some(ResponseHeader {
                    code: StatusCode::OK.as_u16() as u32,
                    ..Default::default()
                });
                resp.success = total_success;
            }

            resp
        });

        let resp = match join_handle.await {
            Ok(v) => v,
            Err(e) => WriteResponse {
                header: Some(error::build_err_header(
                    StatusCode::INTERNAL_SERVER_ERROR.as_u16() as u32,
                    format!("fail to join the spawn task, err:{e:?}"),
                )),
                ..Default::default()
            },
        };

        Ok(tonic::Response::new(resp))
    }

    async fn stream_sql_query_internal(
        ctx: Context,
        proxy: Arc<Proxy<Q>>,
        req: tonic::Request<SqlQueryRequest>,
    ) -> Result<
        tonic::Response<BoxStream<'static, Result<SqlQueryResponse, tonic::Status>>>,
        tonic::Status,
    > {
        let query_req = req.into_inner();

        let runtime = ctx.runtime.clone();
        let join_handle = runtime.spawn(async move {
            if query_req.context.is_none() {
                return stream::once(async move {
                    Ok(SqlQueryResponse {
                        header: Some(error::build_err_header(
                            StatusCode::BAD_REQUEST.as_u16() as u32,
                            "database is not set".to_string(),
                        )),
                        ..Default::default()
                    })
                })
                .boxed();
            }

            proxy
                .handle_stream_sql_query(ctx, query_req)
                .await
                .map(Ok)
                .boxed()
        });

        let resp = match join_handle.await {
            Ok(v) => v,
            Err(e) => stream::once(async move {
                Ok(SqlQueryResponse {
                    header: Some(error::build_err_header(
                        StatusCode::INTERNAL_SERVER_ERROR.as_u16() as u32,
                        format!("fail to join the spawn task, err:{e:?}"),
                    )),
                    ..Default::default()
                })
            })
            .boxed(),
        };

        Ok(tonic::Response::new(resp))
    }
}
