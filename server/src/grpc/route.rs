// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Route handler

use ceresdbproto::storage::{RouteRequest, RouteResponse};

use crate::{
    error::Result,
    grpc::{self, HandlerContext},
};

pub async fn handle_route<Q>(
    ctx: &HandlerContext<'_, Q>,
    req: RouteRequest,
) -> Result<RouteResponse> {
    let routes = ctx.router.route(ctx.tenant(), req).await?;

    let resp = RouteResponse {
        header: Some(grpc::build_ok_header()),
        routes,
    };

    Ok(resp)
}
