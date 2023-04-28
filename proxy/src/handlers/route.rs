// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! route request handler
use ceresdbproto::storage::RouteRequest;
use router::{endpoint::Endpoint, RouterRef};

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

pub async fn handle_route(
    ctx: RequestContext,
    router: RouterRef,
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

    let routes = router.route(route_req).await.context(RouteHandler {
        table: table.to_string(),
    })?;

    let routes = routes
        .into_iter()
        .map(|route| RouteItem {
            table: route.table,
            endpoint: route.endpoint.map(|endpoint| endpoint.into()),
        })
        .collect();

    Ok(RouteResponse { routes })
}
