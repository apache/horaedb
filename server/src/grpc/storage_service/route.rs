// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Route handler

use std::collections::HashMap;

use ceresdbproto::storage::{Route, RouteRequest, RouteResponse};
use error::ErrNoCause;
use http::StatusCode;
use snafu::OptionExt;

use crate::grpc::storage_service::{
    error::{self, Result},
    HandlerContext,
};

pub async fn handle_route<Q>(
    ctx: &HandlerContext<'_, Q>,
    req: RouteRequest,
) -> Result<RouteResponse> {
    // TODO: the case sensitive mode with quoted is not supported now, all table
    // name will be converted to lowercase.

    // Get normalized metrics to original one's mapping.
    let normalized_to_origin: HashMap<_, _> = req
        .metrics
        .iter()
        .enumerate()
        .map(|(idx, metric)| (metric.to_ascii_lowercase(), idx))
        .collect();
    let mut origins = req.metrics;

    // Route using normalized metrics.
    let normalized_metrics: Vec<_> = normalized_to_origin
        .iter()
        .map(|(k, _)| k.clone())
        .collect();
    let req = RouteRequest {
        metrics: normalized_metrics,
    };

    // Replace the normalized metrics in response to origin ones to avoiding
    // exposing this behavior to client.
    let routes = ctx.router.route(ctx.tenant(), req).await?;
    let mut routes_with_origins = Vec::with_capacity(routes.len());
    let origins_len = origins.len();
    for route in routes {
        let idx = normalized_to_origin
            .get(&route.metric)
            .with_context(|| ErrNoCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!(
                    "unknown normalized metric name while finding its origin, metric:{}",
                    route.metric
                ),
            })?;

        let origin = origins.get_mut(*idx).with_context(|| ErrNoCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: format!(
                "impossible to find nothing through idx, idx:{}, origins len:{}",
                idx, origins_len
            ),
        })?;

        routes_with_origins.push(Route {
            metric: std::mem::take(origin),
            endpoint: route.endpoint,
            ext: route.ext,
        });
    }

    let resp = RouteResponse {
        header: Some(error::build_ok_header()),
        routes: routes_with_origins,
    };

    Ok(resp)
}
