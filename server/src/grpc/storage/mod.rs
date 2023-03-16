// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

mod error;
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
use futures::{stream::BoxStream, StreamExt};
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
        self.route_internal(req).await
    }

    async fn write(
        &self,
        req: tonic::Request<WriteRequest>,
    ) -> Result<tonic::Response<WriteResponse>, tonic::Status> {
        self.write_internal(req).await
    }

    async fn sql_query(
        &self,
        req: tonic::Request<SqlQueryRequest>,
    ) -> Result<tonic::Response<SqlQueryResponse>, tonic::Status> {
        self.query_internal(req).await
    }

    async fn prom_query(
        &self,
        req: tonic::Request<PrometheusQueryRequest>,
    ) -> Result<tonic::Response<PrometheusQueryResponse>, tonic::Status> {
        self.prom_query_internal(req).await
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
        let proxy = self.proxy.clone();
        let ctx = Context {
            runtime: self.runtimes.read_runtime.clone(),
            timeout: self.timeout,
        };
        let stream = Self::stream_query_internal(ctx, proxy, req).await;

        Ok(tonic::Response::new(stream.map(Ok).boxed()))
    }
}

impl<Q: QueryExecutor + 'static> StorageServiceImpl<Q> {
    async fn route_internal(
        &self,
        req: tonic::Request<RouteRequest>,
    ) -> Result<tonic::Response<RouteResponse>, tonic::Status> {
        let req = req.into_inner();
        let ctx = Context {
            runtime: self.runtimes.meta_runtime.clone(),
            timeout: self.timeout,
        };
        let resp = self.proxy.handle_route(ctx, req).await;

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

    async fn query_internal(
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
        let mut resp = WriteResponse::default();
        let mut has_err = false;
        let mut stream = req.into_inner();
        let ctx = Context {
            runtime: self.runtimes.write_runtime.clone(),
            timeout: self.timeout,
        };
        while let Some(req) = stream.next().await {
            let write_req = match req {
                Ok(v) => v,
                Err(e) => {
                    return Ok(tonic::Response::new(WriteResponse {
                        header: Some(error::build_err_header(
                            StatusCode::INTERNAL_SERVER_ERROR.as_u16() as u32,
                            format!("fail to fetch request, err:{e:?}"),
                        )),
                        ..Default::default()
                    }));
                }
            };

            let write_resp = self.proxy.handle_write(ctx.clone(), write_req).await;

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

        Ok(tonic::Response::new(resp))
    }

    async fn stream_query_internal(
        ctx: Context,
        proxy: Arc<Proxy<Q>>,
        req: tonic::Request<SqlQueryRequest>,
    ) -> BoxStream<'static, SqlQueryResponse> {
        let begin_instant = Instant::now();
        let query_req = req.into_inner();

        let resp = proxy.handle_stream_sql_query(ctx, query_req).await;

        GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .handle_stream_query
            .observe(begin_instant.saturating_elapsed().as_secs_f64());

        resp
    }
}
