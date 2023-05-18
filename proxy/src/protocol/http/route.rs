// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use ceresdbproto::storage::RouteRequest;
use query_engine::executor::Executor as QueryExecutor;
use router::endpoint::Endpoint;
use serde::Serialize;

use crate::{context::RequestContext, error::Result, Proxy};

#[derive(Serialize)]
pub struct RouteResponse {
    routes: Vec<RouteItem>,
}

#[derive(Serialize)]
pub struct RouteItem {
    pub table: String,
    pub endpoint: Option<Endpoint>,
}

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    pub async fn handle_http_route(
        &self,
        ctx: &RequestContext,
        table: String,
    ) -> Result<RouteResponse> {
        if table.is_empty() {
            return Ok(RouteResponse { routes: vec![] });
        }

        let route_req = RouteRequest {
            context: Some(ceresdbproto::storage::RequestContext {
                database: ctx.schema.clone(),
            }),
            tables: vec![table.to_string()],
        };

        let routes = self.route(route_req).await?;

        let routes = routes
            .into_iter()
            .map(|route| RouteItem {
                table: route.table,
                endpoint: route.endpoint.map(|endpoint| endpoint.into()),
            })
            .collect();

        Ok(RouteResponse { routes })
    }
}
