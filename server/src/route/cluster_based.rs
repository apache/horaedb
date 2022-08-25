// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! A router based on the [`cluster::Cluster`].

use async_trait::async_trait;
use ceresdbproto_deps::ceresdbproto::storage::{Route, RouteRequest};
use cluster::ClusterRef;
use log::warn;
use meta_client::types::RouteTablesRequest;
use snafu::ResultExt;

use crate::{
    config::Endpoint,
    error::{ErrWithCause, Result, StatusCode},
    route::Router,
};

pub struct ClusterBasedRouter {
    cluster: ClusterRef,
}

impl ClusterBasedRouter {
    pub fn new(cluster: ClusterRef) -> Self {
        Self { cluster }
    }
}

#[async_trait]
impl Router for ClusterBasedRouter {
    async fn route(&self, schema: &str, mut req: RouteRequest) -> Result<Vec<Route>> {
        let route_tables_req = RouteTablesRequest {
            schema_name: schema.to_string(),
            table_names: req.take_metrics().into(),
        };
        let route_resp = self
            .cluster
            .route_tables(&route_tables_req)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(ErrWithCause {
                code: StatusCode::InternalError,
                msg: "Fail to route tables by cluster",
            })?;

        let mut routes = Vec::with_capacity(route_resp.entries.len());

        // Now we pick up the nodes who own the leader shard for the route response.
        for (table_name, route_entry) in route_resp.entries {
            for node_shard in route_entry.node_shards {
                if node_shard.shard_info.is_leader() {
                    let mut route = Route::default();
                    let endpoint: Endpoint = match node_shard.endpoint.parse() {
                        Ok(v) => v,
                        Err(msg) => {
                            warn!(
                                "Ignore this endpoint for parsing failed:{}, endpoint:{}",
                                msg, node_shard.endpoint
                            );
                            continue;
                        }
                    };
                    route.set_metric(table_name.clone());
                    route.set_endpoint(endpoint.into());

                    routes.push(route);
                }
            }
        }

        Ok(routes)
    }
}
