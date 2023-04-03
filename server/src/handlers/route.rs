// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! route request handler

use std::time::Instant;

use ceresdbproto::storage::RouteRequest;
use common_util::time::InstantExt;
use log::debug;
use router::endpoint::Endpoint;

use crate::handlers::{error::RouteHandler, prelude::*};

#[derive(Debug, Deserialize)]
pub struct RouteHttpRequest {
    pub tables: Vec<String>,
}

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
    req: &RouteHttpRequest,
) -> Result<RouteResponse> {
    if req.tables.is_empty() {
        return Ok(RouteResponse { routes: vec![] });
    }

    let begin_instant = Instant::now();
    let route_req = RouteRequest {
        context: Some(ceresdbproto::storage::RequestContext {
            database: ctx.schema.clone(),
        }),
        tables: req.tables.clone(),
    };

    let routes = ctx.router.route(route_req).await.context(RouteHandler {
        tables: req.tables.clone(),
    })?;

    let mut route_items = Vec::with_capacity(req.tables.len());
    for route in routes {
        route_items.push(RouteItem {
            table: route.table,
            endpoint: route.endpoint.map(|endpoint| endpoint.into()),
        });
    }

    debug!(
        "Route handler finished, tables:{:?}, cost:{}ms",
        req.tables,
        begin_instant.saturating_elapsed().as_millis(),
    );

    Ok(RouteResponse {
        routes: route_items,
    })
}
