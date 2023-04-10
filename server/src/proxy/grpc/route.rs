// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use ceresdbproto::storage::{RouteRequest, RouteResponse};
use common_util::error::BoxError;
use http::StatusCode;
use log::error;
use query_engine::executor::Executor as QueryExecutor;
use snafu::ResultExt;

use crate::proxy::{error, error::ErrWithCause, Context, Proxy};

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    pub async fn handle_route(&self, _ctx: Context, req: RouteRequest) -> RouteResponse {
        let routes = self
            .router
            .route(req)
            .await
            .box_err()
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "fail to route",
            });

        let mut resp = RouteResponse::default();
        match routes {
            Err(e) => {
                error!("Failed to handle route, err:{e}");
                resp.header = Some(error::build_err_header(e));
            }
            Ok(v) => {
                resp.header = Some(error::build_ok_header());
                resp.routes = v;
            }
        }
        resp
    }
}
