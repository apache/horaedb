// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! This module implements [write][1] and [query][2] for InfluxDB.
//! [1]: https://docs.influxdata.com/influxdb/v1.8/tools/api/#write-http-endpoint
//! [2]: https://docs.influxdata.com/influxdb/v1.8/tools/api/#query-http-endpoint

use std::sync::Arc;

use bytes::Bytes;
use handlers::{error::Result, query::QueryRequest};
use query_engine::executor::Executor as QueryExecutor;
use warp::{reject, reply, Rejection, Reply};

use crate::{
    context::RequestContext, handlers, instance::InstanceRef,
    schema_config_provider::SchemaConfigProviderRef,
};

pub struct Influxdb<Q> {
    instance: InstanceRef<Q>,
    #[allow(dead_code)]
    schema_config_provider: SchemaConfigProviderRef,
}

/// Line protocol
pub struct WriteRequest {
    pub payload: String,
}

impl From<Bytes> for WriteRequest {
    fn from(bytes: Bytes) -> Self {
        WriteRequest {
            payload: String::from_utf8_lossy(&bytes).to_string(),
        }
    }
}

#[allow(dead_code)]
type WriteResponse = String;

impl<Q: QueryExecutor + 'static> Influxdb<Q> {
    pub fn new(instance: InstanceRef<Q>, schema_config_provider: SchemaConfigProviderRef) -> Self {
        Self {
            instance,
            schema_config_provider,
        }
    }

    async fn query(
        &self,
        ctx: RequestContext,
        req: QueryRequest,
    ) -> Result<handlers::query::Response> {
        handlers::query::handle_query(&ctx, self.instance.clone(), req)
            .await
            .map(handlers::query::convert_output)
    }

    async fn write(&self, _ctx: RequestContext, _req: WriteRequest) -> Result<WriteResponse> {
        todo!()
    }
}

// TODO: Request and response type don't match influxdb's API now.
pub async fn query<Q: QueryExecutor + 'static>(
    ctx: RequestContext,
    db: Arc<Influxdb<Q>>,
    req: QueryRequest,
) -> std::result::Result<impl Reply, Rejection> {
    db.query(ctx, req)
        .await
        .map_err(reject::custom)
        .map(|v| reply::json(&v))
}

// TODO: Request and response type don't match influxdb's API now.
#[allow(dead_code)]
pub async fn write<Q: QueryExecutor + 'static>(
    ctx: RequestContext,
    db: Arc<Influxdb<Q>>,
    req: WriteRequest,
) -> std::result::Result<impl Reply, Rejection> {
    db.write(ctx, req).await.map_err(reject::custom)
}
