// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

mod error;
mod metrics;

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use ceresdbproto::storage::{
    storage_service_server::StorageService, PrometheusQueryRequest, PrometheusQueryResponse,
    RouteRequest, RouteResponse, SqlQueryRequest, SqlQueryResponse, WriteRequest, WriteResponse,
};
use futures::stream::BoxStream;
use http::StatusCode;
use query_engine::executor::Executor as QueryExecutor;
use table_engine::engine::EngineRuntimes;

use crate::proxy::{Context, Proxy};

#[derive(Clone)]
pub struct StorageServiceImpl<Q: QueryExecutor + 'static> {
    pub proxy: Arc<Proxy<Q>>,
    pub runtimes: Arc<EngineRuntimes>,
    pub timeout: Option<Duration>,
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

    async fn sql_query(
        &self,
        request: tonic::Request<SqlQueryRequest>,
    ) -> std::result::Result<tonic::Response<SqlQueryResponse>, tonic::Status> {
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
        self.stream_write_internal(request).await
    }

    async fn stream_sql_query(
        &self,
        _request: tonic::Request<SqlQueryRequest>,
    ) -> std::result::Result<tonic::Response<Self::StreamSqlQueryStream>, tonic::Status> {
        todo!()
    }
}

impl<Q: QueryExecutor + 'static> StorageServiceImpl<Q> {
    async fn route_internal(
        &self,
        req: tonic::Request<RouteRequest>,
    ) -> std::result::Result<tonic::Response<RouteResponse>, tonic::Status> {
        let req = req.into_inner();
        let resp = self.proxy.handle_route(Context::default(), req).await;

        Ok(tonic::Response::new(resp))
    }

    async fn write_internal(
        &self,
        req: tonic::Request<WriteRequest>,
    ) -> std::result::Result<tonic::Response<WriteResponse>, tonic::Status> {
        let req = req.into_inner();
        let proxy = self.proxy.clone();
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

            proxy.handle_write(Context::default(), req).await
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
    ) -> std::result::Result<tonic::Response<SqlQueryResponse>, tonic::Status> {
        let req = req.into_inner();
        let proxy = self.proxy.clone();
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

            proxy.handle_sql_query(Context::default(), req).await
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
        _req: tonic::Request<PrometheusQueryRequest>,
    ) -> std::result::Result<tonic::Response<PrometheusQueryResponse>, tonic::Status> {
        todo!()
    }

    async fn stream_write_internal(
        &self,
        _request: tonic::Request<tonic::Streaming<WriteRequest>>,
    ) -> std::result::Result<tonic::Response<WriteResponse>, tonic::Status> {
        todo!()
    }
}
