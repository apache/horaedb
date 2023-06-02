// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use ceresdbproto::storage::{RouteRequest, RouteResponse};
use query_engine::executor::Executor as QueryExecutor;

use crate::{error, Context, Proxy};

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    pub async fn handle_route(&self, _ctx: Context, req: RouteRequest) -> RouteResponse {
        let routes = self.route(req).await;

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
