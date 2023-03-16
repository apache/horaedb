// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! A router based on the [`cluster::Cluster`].

use std::time::Duration;

use async_trait::async_trait;
use ceresdbproto::storage::{Route, RouteRequest};
use cluster::ClusterRef;
use common_util::error::BoxError;
use meta_client::types::RouteTablesRequest;
use moka::future::Cache;
use snafu::ResultExt;

use crate::{endpoint::Endpoint, OtherWithCause, ParseEndpoint, Result, RouteCacheConfig, Router};

pub struct ClusterBasedRouter {
    cluster: ClusterRef,
    cache: Cache<String, Route>,
}

impl ClusterBasedRouter {
    pub fn new(cluster: ClusterRef, cache_config: RouteCacheConfig) -> Self {
        Self {
            cluster,
            cache: Cache::builder()
                .time_to_live(Duration::from_secs(cache_config.ttl))
                .time_to_idle(Duration::from_secs(cache_config.tti))
                .max_capacity(cache_config.capacity)
                .build(),
        }
    }
}

/// route table from local cache, return cache routes and  tables which are not
/// in cache
fn route_from_cache(
    cache: &Cache<String, Route>,
    tables: Vec<String>,
) -> (Vec<Route>, Vec<String>) {
    let mut routes = vec![];
    let mut miss = vec![];

    for table in tables {
        if let Some(route) = cache.get(&table) {
            routes.push(route);
        } else {
            miss.push(table.clone());
        }
    }

    (routes, miss)
}

/// Make a route according to the table name and the raw endpoint.
fn make_route(table_name: &str, endpoint: &str) -> Result<Route> {
    let endpoint: Endpoint = endpoint.parse().context(ParseEndpoint { endpoint })?;

    Ok(Route {
        table: table_name.to_string(),
        endpoint: Some(endpoint.into()),
    })
}

#[async_trait]
impl Router for ClusterBasedRouter {
    async fn route(&self, req: RouteRequest) -> Result<Vec<Route>> {
        let req_ctx = req.context.unwrap();

        // Firstly route table from local cache
        let (mut routes, miss) = route_from_cache(&self.cache, req.tables);

        let route_tables_req = RouteTablesRequest {
            schema_name: req_ctx.database,
            table_names: miss,
        };

        let route_resp = self
            .cluster
            .route_tables(&route_tables_req)
            .await
            .box_err()
            .with_context(|| OtherWithCause {
                msg: format!("Failed to route tables by cluster, req:{route_tables_req:?}"),
            })?;

        // Now we pick up the nodes who own the leader shard for the route response.
        for (table_name, route_entry) in route_resp.entries {
            for node_shard in route_entry.node_shards {
                if node_shard.shard_info.is_leader() {
                    let route = make_route(&table_name, &node_shard.endpoint)?;
                    self.cache.insert(table_name.clone(), route.clone()).await;
                    routes.push(route);
                }
            }
        }

        Ok(routes)
    }
}

#[cfg(test)]
mod tests {
    use std::thread::sleep;

    use super::*;

    #[tokio::test]
    async fn test_route_cache() {
        let cache: Cache<String, Route> = Cache::builder()
            .time_to_live(Duration::from_secs(4))
            .time_to_idle(Duration::from_secs(2))
            .max_capacity(2)
            .build();

        let tables = vec!["table1".to_string(), "table2".to_string()];

        // first case get two tables, miss 0
        let route = make_route("table0", "127.0.0.0:8831").unwrap();
        cache.insert("table0".to_string(), route).await;
        let route = make_route("table1", "127.0.0.1:8831").unwrap();
        cache.insert("table1".to_string(), route).await;
        let route = make_route("table2", "127.0.0.2:8831").unwrap();
        cache.insert("table2".to_string(), route).await;
        let (routes, miss) = route_from_cache(&cache, tables);
        assert_eq!(routes.len(), 2);
        assert_eq!(miss.len(), 0);
        sleep(Duration::from_secs(1));

        // try to get table1
        let tables = vec!["table1".to_string()];

        let (routes, miss) = route_from_cache(&cache, tables);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].table, "table1".to_string());
        assert_eq!(miss.len(), 0);

        // sleep 2, table2 will be evicted, and table1 in cache
        sleep(Duration::from_secs(2));
        let tables = vec!["table1".to_string(), "table2".to_string()];

        let (routes, miss) = route_from_cache(&cache, tables);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].table, "table1".to_string());
        assert_eq!(miss.len(), 1);
        assert_eq!(miss[0], "table2".to_string());
    }
}
