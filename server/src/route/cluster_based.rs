// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! A router based on the [`cluster::Cluster`].

use async_trait::async_trait;
use ceresdbproto_deps::ceresdbproto::storage::{Route, RouteRequest};
use cluster::ClusterRef;
use common_types::table::TableName;
use log::warn;
use meta_client::types::{RouteTablesRequest, RouteTablesResponse};
use snafu::ResultExt;

use crate::{
    config::Endpoint,
    error::{ErrWithCause, Result, StatusCode},
    route::{hash, Router},
};

pub struct ClusterBasedRouter {
    cluster: ClusterRef,
}

impl ClusterBasedRouter {
    pub fn new(cluster: ClusterRef) -> Self {
        Self { cluster }
    }

    /// For missing tables in the topology, the Router will choose random nodes
    /// for them so that some requests such as create table, can also find a
    /// node to be served.
    async fn route_for_missing_tables(
        &self,
        queried_tables: &[TableName],
        route_resp: &RouteTablesResponse,
        route_result: &mut Vec<Route>,
    ) -> Result<()> {
        if !route_resp.contains_missing_table(queried_tables) {
            return Ok(());
        }

        let cluster_nodes = self
            .cluster
            .fetch_nodes()
            .await
            .map_err(|e| Box::new(e) as _)
            .context(ErrWithCause {
                code: StatusCode::InternalError,
                msg: "fail to fetch cluster nodes",
            })?;

        if cluster_nodes.is_empty() {
            warn!("Cluster has no nodes for route response");
            return Ok(());
        }

        // Check wether some tables are missing, and pick some nodes for them if any.
        for table_name in queried_tables {
            if route_resp.entries.contains_key(table_name) {
                continue;
            }

            // The cluster_nodes has been ensured not empty.
            let random_idx = hash::hash_metric(table_name) as usize % cluster_nodes.len();
            let random_node = &cluster_nodes[random_idx];

            let route = make_route(table_name, &random_node.endpoint)?;
            route_result.push(route);
        }

        Ok(())
    }
}

/// Make a route according the table name and the raw endpoint.
fn make_route(table_name: &str, endpoint: &str) -> Result<Route> {
    let mut route = Route::default();
    let endpoint: Endpoint = endpoint.parse().with_context(|| ErrWithCause {
        code: StatusCode::InternalError,
        msg: format!("Failed to parse endpoint:{}", endpoint),
    })?;
    route.set_metric(table_name.to_string());
    route.set_endpoint(endpoint.into());

    Ok(route)
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

        self.route_for_missing_tables(&route_tables_req.table_names, &route_resp, &mut routes)
            .await?;
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
