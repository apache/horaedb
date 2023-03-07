// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

mod error;
mod metrics;

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use ceresdbproto::storage::{
    storage_service_server::StorageService, PrometheusQueryRequest, PrometheusQueryResponse,
    RouteRequest, RouteResponse, SqlQueryRequest, SqlQueryResponse, WriteRequest, WriteResponse,
};
use common_util::error::BoxError;
use futures::stream::{self, BoxStream, StreamExt};
use http::StatusCode;
use query_engine::executor::Executor as QueryExecutor;
use snafu::ResultExt;
use table_engine::engine::EngineRuntimes;
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    grpc::{
        storage::error::{ErrWithCause, Result},
        storage_service::RequestHeader,
    },
    proxy::{Context, Proxy},
    schema_config_provider::SchemaConfigProviderRef,
};

#[derive(Clone)]
pub struct StorageServiceImpl<Q: QueryExecutor + 'static> {
    pub proxy: Arc<Proxy<Q>>,
    pub runtimes: Arc<EngineRuntimes>,
    pub schema_config_provider: SchemaConfigProviderRef,
    pub timeout: Option<Duration>,
    pub resp_compress_min_length: usize,
}

impl<Q: QueryExecutor + 'static> StorageServiceImpl<Q> {
    pub fn new() -> Self {
        todo!()
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
}

impl<Q: QueryExecutor + 'static> StorageServiceImpl<Q> {
    async fn route_internal(
        &self,
        req: tonic::Request<RouteRequest>,
    ) -> std::result::Result<tonic::Response<RouteResponse>, tonic::Status> {
        let req = req.into_inner();
        let ret = self
            .proxy
            .handle_route(Context::default(), req)
            .await
            .box_err()
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "fail to join the spawn task",
            });

        let mut resp = RouteResponse::default();
        match ret {
            Err(e) => {
                resp.header = Some(error::build_err_header(e));
            }
            Ok(v) => {
                resp.header = Some(error::build_ok_header());
                resp.routes = v.routes;
            }
        }
        Ok(tonic::Response::new(resp))
    }

    async fn write_internal(
        &self,
        _req: tonic::Request<WriteRequest>,
    ) -> std::result::Result<tonic::Response<WriteResponse>, tonic::Status> {
        todo!()
    }

    async fn query_internal(
        &self,
        _req: tonic::Request<SqlQueryRequest>,
    ) -> std::result::Result<tonic::Response<SqlQueryResponse>, tonic::Status> {
        todo!()
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

    async fn stream_sql_query_internal(
        &self,
        _request: tonic::Request<SqlQueryRequest>,
    ) -> Result<ReceiverStream<Result<SqlQueryResponse>>> {
        todo!()
    }
}
