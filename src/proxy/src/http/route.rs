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

use horaedbproto::storage::RouteRequest as RouteRequestPb;
use router::{endpoint::Endpoint, RouteRequest};
use serde::Serialize;

use crate::{context::RequestContext, error::Result, Proxy};

#[derive(Serialize)]
pub struct RouteResponse {
    routes: Vec<RouteItem>,
}

#[derive(Serialize)]
pub struct RouteItem {
    pub table: String,
    pub endpoint: Option<Endpoint>,
}

impl Proxy {
    pub async fn handle_http_route(
        &self,
        ctx: &RequestContext,
        table: String,
    ) -> Result<RouteResponse> {
        if table.is_empty() {
            return Ok(RouteResponse { routes: vec![] });
        }

        let req_pb = RouteRequestPb {
            context: Some(horaedbproto::storage::RequestContext {
                database: ctx.schema.clone(),
            }),
            tables: vec![table.to_string()],
        };

        let request = RouteRequest {
            route_with_cache: false,
            inner: req_pb,
        };

        let routes = self.route(request).await?;

        let routes = routes
            .into_iter()
            .map(|route| RouteItem {
                table: route.table,
                endpoint: route.endpoint.map(|endpoint| endpoint.into()),
            })
            .collect();

        Ok(RouteResponse { routes })
    }
}
