// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! A router based on the [`cluster::Cluster`].

use async_trait::async_trait;
use ceresdbproto::storage::{Route, RouteRequest};
use cluster::ClusterRef;
use common_util::error::BoxError;
use meta_client::types::RouteTablesRequest;
use snafu::ResultExt;

use crate::{endpoint::Endpoint, OtherWithCause, ParseEndpoint, Result, Router};

pub struct ClusterBasedRouter {
    cluster: ClusterRef,
}

impl ClusterBasedRouter {
    pub fn new(cluster: ClusterRef) -> Self {
        Self { cluster }
    }
}

/// Make a route according to the table name and the raw endpoint.
fn make_route(table_name: &str, endpoint: &str) -> Result<Route> {
    let endpoint: Endpoint = endpoint.parse().context(ParseEndpoint { endpoint })?;

    Ok(Route {
        table: table_name.to_string(),
        endpoint: Some(endpoint.into()),
        ..Default::default()
    })
}

#[async_trait]
impl Router for ClusterBasedRouter {
    async fn route(&self, schema: &str, req: RouteRequest) -> Result<Vec<Route>> {
        let route_tables_req = RouteTablesRequest {
            schema_name: schema.to_string(),
            table_names: req.tables,
        };
        let route_resp = self
            .cluster
            .route_tables(&route_tables_req)
            .await
            .box_err()
            .with_context(|| OtherWithCause {
                msg: format!(
                    "Failed to route tables by cluster, req:{:?}",
                    route_tables_req
                ),
            })?;

        let mut routes = Vec::with_capacity(route_resp.entries.len());

        // Now we pick up the nodes who own the leader shard for the route response.
        for (table_name, route_entry) in route_resp.entries {
            for node_shard in route_entry.node_shards {
                if node_shard.shard_info.is_leader() {
                    let route = make_route(&table_name, &node_shard.endpoint)?;
                    routes.push(route);
                }
            }
        }

        Ok(routes)
    }
}
