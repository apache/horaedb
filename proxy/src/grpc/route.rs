// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use ceresdbproto::storage::{RouteRequest, RouteResponse};
use query_engine::{executor::Executor as QueryExecutor, physical_planner::PhysicalPlanner};

use crate::{error, metrics::GRPC_HANDLER_COUNTER_VEC, Context, Proxy};

impl<Q: QueryExecutor + 'static, P: PhysicalPlanner> Proxy<Q, P> {
    pub async fn handle_route(&self, _ctx: Context, req: RouteRequest) -> RouteResponse {
        let routes = self.route(req).await;

        let mut resp = RouteResponse::default();
        match routes {
            Err(e) => {
                GRPC_HANDLER_COUNTER_VEC.route_failed.inc();

                error!("Failed to handle route, err:{e}");
                resp.header = Some(error::build_err_header(e));
            }
            Ok(v) => {
                GRPC_HANDLER_COUNTER_VEC.route_succeeded.inc();

                resp.header = Some(error::build_ok_header());
                resp.routes = v;
            }
        }
        resp
    }
}
