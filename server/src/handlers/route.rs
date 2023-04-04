// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! route request handler

use std::time::Instant;

use ceresdbproto::storage::RouteRequest;
use common_util::time::InstantExt;
use log::debug;
use router::endpoint::Endpoint;

use crate::handlers::{error::RouteHandler, prelude::*};

#[derive(Serialize)]
pub struct RouteResponse {
    routes: Vec<RouteItem>,
}

#[derive(Serialize)]
pub struct RouteItem {
    pub table: String,
    pub endpoint: Option<Endpoint>,
}

pub async fn handle_route<Q: QueryExecutor + 'static>(
    ctx: &RequestContext,
    _: InstanceRef<Q>,
    table: &str,
) -> Result<RouteResponse> {
    if table.is_empty() {
        return Ok(RouteResponse { routes: vec![] });
    }

    let begin_instant = Instant::now();
    let route_req = RouteRequest {
        context: Some(ceresdbproto::storage::RequestContext {
            database: ctx.schema.clone(),
        }),
        tables: vec![table.to_string()],
    };

    let routes = ctx.router.route(route_req).await.context(RouteHandler {
        table: table.to_string(),
    })?;

    let mut route_items = Vec::with_capacity(1);
    for route in routes {
        route_items.push(RouteItem {
            table: route.table,
            endpoint: route.endpoint.map(|endpoint| endpoint.into()),
        });
    }

    Ok(RouteResponse {
        routes: route_items,
    })
}
