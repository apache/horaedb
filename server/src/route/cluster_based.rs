// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! A router based on the [`cluster::Cluster`].

use async_trait::async_trait;
use ceresdbproto::storage::{Route, RouteRequest};
use cluster::ClusterRef;
use common_types::table::TableName;
use http::StatusCode;
use log::warn;
use meta_client::types::{NodeShard, RouteTablesRequest, RouteTablesResponse};
use snafu::{OptionExt, ResultExt};

use crate::{
    config::Endpoint,
    error::{ErrNoCause, ErrWithCause, Result},
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
        if route_resp.contains_all_tables(queried_tables) {
            return Ok(());
        }

        let cluster_nodes_resp = self
            .cluster
            .fetch_nodes()
            .await
            .map_err(|e| Box::new(e) as _)
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "Failed to fetch cluster nodes",
            })?;

        if cluster_nodes_resp.cluster_nodes.is_empty() {
            warn!("Cluster has no nodes for route response");
            return Ok(());
        }

        // Check whether some tables are missing, and pick some nodes for them if any.
        for table_name in queried_tables {
            if route_resp.entries.contains_key(table_name) {
                continue;
            }

            let picked_node_shard =
                pick_node_for_table(table_name, &cluster_nodes_resp.cluster_nodes).with_context(
                    || ErrNoCause {
                        code: StatusCode::NOT_FOUND,
                        msg: format!(
                            "No valid node for table({}), cluster nodes:{:?}",
                            table_name, cluster_nodes_resp
                        ),
                    },
                )?;
            let route = make_route(table_name, &picked_node_shard.endpoint)?;
            route_result.push(route);
        }

        Ok(())
    }
}

/// Make a route according to the table name and the raw endpoint.
fn make_route(table_name: &str, endpoint: &str) -> Result<Route> {
    let endpoint: Endpoint = endpoint.parse().with_context(|| ErrWithCause {
        code: StatusCode::INTERNAL_SERVER_ERROR,
        msg: format!("Failed to parse endpoint:{}", endpoint),
    })?;

    Ok(Route {
        metric: table_name.to_string(),
        endpoint: Some(endpoint.into()),
        ..Default::default()
    })
}

#[async_trait]
impl Router for ClusterBasedRouter {
    async fn route(&self, schema: &str, req: RouteRequest) -> Result<Vec<Route>> {
        let route_tables_req = RouteTablesRequest {
            schema_name: schema.to_string(),
            table_names: req.metrics,
        };
        let route_resp = self
            .cluster
            .route_tables(&route_tables_req)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "Failed to route tables by cluster",
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

/// Pick a node for the table.
///
/// This pick logic ensures:
/// 1. The picked node has leader shard;
/// 2. The picked node is determined if `node_shards` doesn't change.
fn pick_node_for_table<'a>(
    table_name: &'_ str,
    node_shards: &'a [NodeShard],
) -> Option<&'a NodeShard> {
    if node_shards.is_empty() {
        return None;
    }

    // The cluster_nodes has been ensured not empty.
    let node_idx = hash::hash_table(table_name) as usize % node_shards.len();

    for idx in node_idx..(node_shards.len() + node_idx) {
        let idx = idx % node_shards.len();
        let node_shard = &node_shards[idx];
        if node_shard.shard_info.is_leader() {
            return Some(node_shard);
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use meta_client::types::{ShardInfo, ShardRole};

    use super::*;

    fn make_node_shards(leaderships: &[bool]) -> Vec<NodeShard> {
        leaderships
            .iter()
            .enumerate()
            .map(|(idx, is_leader)| {
                let role = if *is_leader {
                    ShardRole::Leader
                } else {
                    ShardRole::Follower
                };

                let shard_info = ShardInfo {
                    shard_id: 0,
                    role,
                    version: 0,
                };

                NodeShard {
                    endpoint: format!("test-domain:{}", idx + 100),
                    shard_info,
                }
            })
            .collect()
    }

    #[test]
    fn test_pick_node_for_table() {
        let cases = [
            (vec![false, false, false, false, true], true),
            (vec![false, false, false, false, false], false),
            (vec![], false),
            (vec![true, true, true, true], true),
        ];

        let table_names = ["aaa", "bbb", "***111abc", ""];

        for (leadership, picked) in cases {
            let node_shards = make_node_shards(&leadership);

            for table_name in table_names {
                let picked_node_shard = pick_node_for_table(table_name, &node_shards);
                assert_eq!(picked_node_shard.is_some(), picked);

                if picked {
                    let node_shard = picked_node_shard.unwrap();
                    assert!(node_shard.shard_info.is_leader());
                }

                // Pick again and check whether they are the same.
                let picked_again_node_shard = pick_node_for_table(table_name, &node_shards);
                assert_eq!(picked_node_shard, picked_again_node_shard);
            }
        }
    }
}
