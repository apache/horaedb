// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Route handler

use std::sync::Arc;

use ceresdbproto_deps::ceresdbproto::storage::{RouteRequest, RouteResponse};

use crate::{
    error::Result,
    grpc::{self, HandlerContext},
    route::Router,
};

pub async fn handle_route<Q>(
    ctx: &HandlerContext<'_, Q>,
    req: RouteRequest,
) -> Result<RouteResponse> {
    handle_route_sync(ctx.router.clone(), req, ctx.tenant())
}

fn handle_route_sync(
    router: Arc<dyn Router + Sync + Send>,
    req: RouteRequest,
    schema: &str,
) -> Result<RouteResponse> {
    let route_vec = router.route(schema, req)?;

    let mut resp = RouteResponse::new();
    resp.set_header(grpc::build_ok_header());
    resp.set_routes(route_vec.into());

    Ok(resp)
}
