// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Route handler

use ceresdbproto_deps::ceresdbproto::storage::{RouteRequest, RouteResponse};

use crate::{
    error::Result,
    grpc::{self, HandlerContext},
};

pub async fn handle_route<Q>(
    ctx: &HandlerContext<'_, Q>,
    req: RouteRequest,
) -> Result<RouteResponse> {
    let routes = ctx.router.route(ctx.tenant(), req).await?;

    let mut resp = RouteResponse::new();
    resp.set_header(grpc::build_ok_header());
    resp.set_routes(routes.into());

    Ok(resp)
}
