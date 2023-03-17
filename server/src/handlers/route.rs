// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! route request handler

use std::{collections::HashMap, time::Instant};

use ceresdbproto::storage::RouteRequest;
use common_util::time::InstantExt;
use log::info;

use crate::handlers::{error::RouteHandler, prelude::*};

#[derive(Serialize)]
pub struct RouteResponse {
    routes: HashMap<String, String>,
}

pub async fn handle_route<Q: QueryExecutor + 'static>(
    ctx: &RequestContext,
    _: InstanceRef<Q>,
    table: String,
) -> Result<RouteResponse> {
    let mut route_map = HashMap::new();
    if table.is_empty() {
        return Ok(RouteResponse { routes: route_map });
    }

    let begin_instant = Instant::now();
    info!("Route handler try to find route for table:{table}");
    let req = RouteRequest {
        context: Some(ceresdbproto::storage::RequestContext {
            database: ctx.schema.clone(),
        }),
        tables: vec![table.clone()],
    };
    let routes = ctx.router.route(req).await.context(RouteHandler {
        table: table.clone(),
    })?;
    for route in routes {
        if let Some(endpoint) = route.endpoint {
            route_map.insert(route.table, format!("{}:{}", endpoint.ip, endpoint.port));
        }
    }
    info!(
        "Route handler finished, cost:{}ms, table:{}",
        begin_instant.saturating_elapsed().as_millis(),
        table
    );

    Ok(RouteResponse { routes: route_map })
}
