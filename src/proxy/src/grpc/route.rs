// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use horaedbproto::storage::{RouteRequest as RouteRequestPb, RouteResponse};
use http::StatusCode;
use router::RouteRequest;

use crate::{
    error,
    error::{ErrNoCause, Result},
    metrics::GRPC_HANDLER_COUNTER_VEC,
    Context, Proxy,
};

impl Proxy {
    pub async fn handle_route(&self, ctx: Context, req: RouteRequestPb) -> RouteResponse {
        let request = RouteRequest::new(req, true);

        match self.handle_route_internal(ctx, request).await {
            Ok(v) => {
                GRPC_HANDLER_COUNTER_VEC.route_succeeded.inc();
                v
            }
            Err(e) => {
                error!("Failed to handle route, err:{e}");
                GRPC_HANDLER_COUNTER_VEC.route_failed.inc();
                RouteResponse {
                    header: Some(error::build_err_header(e)),
                    ..Default::default()
                }
            }
        }
    }

    async fn handle_route_internal(
        &self,
        ctx: Context,
        req: RouteRequest,
    ) -> Result<RouteResponse> {
        // Check if the tenant is authorized to access the database.
        if !self
            .auth
            .lock()
            .unwrap()
            .identify(ctx.tenant.clone(), ctx.access_token.clone())
        {
            return ErrNoCause {
                msg: format!("tenant: {:?} unauthorized", ctx.tenant),
                code: StatusCode::UNAUTHORIZED,
            }
            .fail();
        }

        let routes = self.route(req).await?;
        Ok(RouteResponse {
            header: Some(error::build_ok_header()),
            routes,
        })
    }
}
