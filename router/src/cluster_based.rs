// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! A router based on the [`cluster::Cluster`].

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
    cache: Option<Cache<String, Route>>,
}

impl ClusterBasedRouter {
    pub fn new(cluster: ClusterRef, cache_config: RouteCacheConfig) -> Self {
        let cache = if cache_config.enable {
            Some(
                Cache::builder()
                    .time_to_live(cache_config.ttl.0)
                    .time_to_idle(cache_config.tti.0)
                    .max_capacity(cache_config.capacity)
                    .build(),
            )
        } else {
            None
        };

        Self { cluster, cache }
    }

    /// route table from local cache, return cache routes and tables which are
    /// not in cache
    fn route_from_cache(&self, tables: Vec<String>, routes: &mut Vec<Route>) -> Vec<String> {
        let mut miss = vec![];

        if let Some(cache) = &self.cache {
            for table in tables {
                if let Some(route) = cache.get(&table) {
                    routes.push(route.clone());
                } else {
                    miss.push(table.clone());
                }
            }
        } else {
            miss = tables;
        }

        miss
    }
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

        // Firstly route table from local cache.
        let mut routes = Vec::with_capacity(req.tables.len());
        let miss = self.route_from_cache(req.tables, &mut routes);

        if miss.is_empty() {
            return Ok(routes);
        }

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
                    if let Some(cache) = &self.cache {
                        // There may be data race here, and it is acceptable currently.
                        cache.insert(table_name.clone(), route.clone()).await;
                    }
                    routes.push(route);
                }
            }
        }
        return Ok(routes);
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc, thread::sleep, time::Duration};

    use ceresdbproto::{
        meta_event::{
            CloseShardRequest, CloseTableOnShardRequest, CreateTableOnShardRequest,
            DropTableOnShardRequest, OpenShardRequest, OpenTableOnShardRequest,
        },
        storage::RequestContext,
    };
    use cluster::{Cluster, ClusterNodesResp};
    use common_util::config::ReadableDuration;
    use meta_client::types::{
        NodeShard, RouteEntry, RouteTablesResponse, ShardInfo, ShardRole::Leader, TableInfo,
        TablesOfShard,
    };

    use super::*;

    struct MockClusterImpl {}

    #[async_trait]
    impl Cluster for MockClusterImpl {
        async fn start(&self) -> cluster::Result<()> {
            unimplemented!();
        }

        async fn stop(&self) -> cluster::Result<()> {
            unimplemented!();
        }

        async fn open_shard(&self, _req: &OpenShardRequest) -> cluster::Result<TablesOfShard> {
            unimplemented!();
        }

        async fn close_shard(&self, _req: &CloseShardRequest) -> cluster::Result<TablesOfShard> {
            unimplemented!();
        }

        async fn create_table_on_shard(
            &self,
            _req: &CreateTableOnShardRequest,
        ) -> cluster::Result<()> {
            unimplemented!();
        }

        async fn drop_table_on_shard(&self, _req: &DropTableOnShardRequest) -> cluster::Result<()> {
            unimplemented!();
        }

        async fn open_table_on_shard(&self, _req: &OpenTableOnShardRequest) -> cluster::Result<()> {
            unimplemented!();
        }

        async fn close_table_on_shard(
            &self,
            _req: &CloseTableOnShardRequest,
        ) -> cluster::Result<()> {
            unimplemented!();
        }

        async fn route_tables(
            &self,
            req: &RouteTablesRequest,
        ) -> cluster::Result<RouteTablesResponse> {
            let mut entries = HashMap::new();
            for table in &req.table_names {
                entries.insert(
                    table.clone(),
                    RouteEntry {
                        table: TableInfo {
                            id: 0,
                            name: table.clone(),
                            schema_name: String::from("public"),
                            schema_id: 0,
                        },
                        node_shards: vec![NodeShard {
                            endpoint: String::from("127.0.0.1:8831"),
                            shard_info: ShardInfo {
                                id: 0,
                                role: Leader,
                                version: 100,
                            },
                        }],
                    },
                );
            }

            Ok(RouteTablesResponse {
                cluster_topology_version: 0,
                entries,
            })
        }

        async fn fetch_nodes(&self) -> cluster::Result<ClusterNodesResp> {
            unimplemented!();
        }
    }

    #[tokio::test]
    async fn test_route_cache() {
        let mock_cluster = MockClusterImpl {};

        let config = RouteCacheConfig {
            enable: true,
            ttl: ReadableDuration::from(Duration::from_secs(4)),
            tti: ReadableDuration::from(Duration::from_secs(2)),
            capacity: 2,
        };
        let router = ClusterBasedRouter::new(Arc::new(mock_cluster), config);

        let table1 = "table1";
        let table2 = "table2";

        // first case get two tables, no one miss
        let tables = vec![table1.to_string(), table2.to_string()];
        let result = router
            .route(RouteRequest {
                context: Some(RequestContext {
                    database: String::from("public"),
                }),
                tables: tables.clone(),
            })
            .await;
        assert_eq!(result.unwrap().len(), 2);

        let mut routes = Vec::with_capacity(tables.len());
        let miss = router.route_from_cache(tables, &mut routes);
        assert_eq!(routes.len(), 2);
        assert_eq!(miss.len(), 0);
        sleep(Duration::from_secs(1));

        // try to get table1
        let tables = vec![table1.to_string()];
        let mut routes = Vec::with_capacity(tables.len());
        let miss = router.route_from_cache(tables, &mut routes);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].table, table1.to_string());
        assert_eq!(miss.len(), 0);

        // sleep 1.5s, table2 will be evicted, and table1 in cache
        sleep(Duration::from_millis(1500));
        let tables = vec![table1.to_string(), table2.to_string()];
        let mut routes = Vec::with_capacity(tables.len());
        let miss = router.route_from_cache(tables, &mut routes);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].table, table1.to_string());
        assert_eq!(miss.len(), 1);
        assert_eq!(miss[0], table2.to_string());
    }
}
