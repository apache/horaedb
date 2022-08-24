// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! A router based on the [`cluster::Cluster`].

#![allow(dead_code)]

use async_trait::async_trait;
use ceresdbproto_deps::ceresdbproto::storage::{Endpoint, Route, RouteRequest};
use cluster::ClusterRef;
use log::warn;
use meta_client::types::RouteTablesRequest;
use snafu::ResultExt;

use crate::{
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

// Parse the raw endpoint which should be the form: <domain_name>:<port>
//
// Returns `None` if fail to parse.
fn try_parse_endpoint(raw: &str) -> Option<Endpoint> {
    let (domain, raw_port) = raw.split_once(":")?;
    let port: u16 = raw_port.parse().map(|p| Some(p)).unwrap_or(None)?;

    let mut endpoint = Endpoint::default();
    endpoint.set_ip(domain.to_string());
    endpoint.set_port(port as u32);
    Some(endpoint)
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
                    let endpoint = match try_parse_endpoint(&node_shard.endpoint) {
                        Some(v) => v,
                        None => {
                            warn!("Fail to parse endpoint:{}", node_shard.endpoint);
                            continue;
                        }
                    };
                    route.set_metric(table_name.clone());
                    route.set_endpoint(endpoint);

                    routes.push(route);
                }
            }
        }

        Ok(routes)
    }
}
