// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use async_trait::async_trait;
use ceresdbproto::meta_event::{
    CloseShardRequest, CreateTableOnShardRequest, DropTableOnShardRequest, OpenShardRequest,
};
use common_util::runtime::{JoinHandle, Runtime};
use log::{error, info, warn};
use meta_client::{
    types::{
        GetNodesRequest, GetTablesOfShardsRequest, RouteTablesRequest, RouteTablesResponse,
        ShardInfo, TableInfo, TablesOfShard,
    },
    MetaClientRef,
};
use snafu::{ensure, OptionExt, ResultExt};
use tokio::{
    sync::mpsc::{self, Sender},
    time,
};

use crate::{
    config::ClusterConfig, shard_tables_cache::ShardTablesCache, topology::ClusterTopology,
    Cluster, ClusterNodesNotFound, ClusterNodesResp, MetaClientFailure, OpenShard,
    OpenShardWithCause, Result, ShardNotFound, TableNotFound,
};

/// ClusterImpl is an implementation of [`Cluster`] based [`MetaClient`].
///
/// Its functions are to:
///  - Handle the some action from the CeresMeta;
///  - Handle the heartbeat between ceresdb-server and CeresMeta;
///  - Provide the cluster topology.
pub struct ClusterImpl {
    inner: Arc<Inner>,
    runtime: Arc<Runtime>,
    config: ClusterConfig,
    heartbeat_handle: Mutex<Option<JoinHandle<()>>>,
    stop_heartbeat_tx: Mutex<Option<Sender<()>>>,
}

impl ClusterImpl {
    pub fn new(
        shard_tables_cache: ShardTablesCache,
        meta_client: MetaClientRef,
        config: ClusterConfig,
        runtime: Arc<Runtime>,
    ) -> Result<Self> {
        let inner = Inner::new(shard_tables_cache, meta_client)?;

        Ok(Self {
            inner: Arc::new(inner),
            runtime,
            config,
            heartbeat_handle: Mutex::new(None),
            stop_heartbeat_tx: Mutex::new(None),
        })
    }

    fn start_heartbeat_loop(&self) {
        let interval = self.heartbeat_interval();
        let error_wait_lease = self.error_wait_lease();
        let inner = self.inner.clone();
        let (tx, mut rx) = mpsc::channel(1);

        let handle = self.runtime.spawn(async move {
            loop {
                let shard_infos = inner.shard_tables_cache.all_shard_infos();
                info!("Node heartbeat to meta, shard infos:{:?}", shard_infos);

                let resp = inner.meta_client.send_heartbeat(shard_infos).await;
                let wait = match resp {
                    Ok(()) => interval,
                    Err(e) => {
                        error!("Send heartbeat to meta failed, err:{}", e);
                        error_wait_lease
                    }
                };

                if time::timeout(wait, rx.recv()).await.is_ok() {
                    warn!("Receive exit command and exit heartbeat loop");
                    break;
                }
            }
        });

        *self.stop_heartbeat_tx.lock().unwrap() = Some(tx);
        *self.heartbeat_handle.lock().unwrap() = Some(handle);
    }

    // Register node every 2/3 lease
    fn heartbeat_interval(&self) -> Duration {
        Duration::from_millis(self.config.meta_client.lease.as_millis() * 2 / 3)
    }

    fn error_wait_lease(&self) -> Duration {
        self.config.meta_client.lease.0 / 2
    }

    pub fn shard_tables_cache(&self) -> &ShardTablesCache {
        &self.inner.shard_tables_cache
    }
}

struct Inner {
    shard_tables_cache: ShardTablesCache,
    meta_client: MetaClientRef,
    topology: RwLock<ClusterTopology>,
}

impl Inner {
    fn new(shard_tables_cache: ShardTablesCache, meta_client: MetaClientRef) -> Result<Self> {
        Ok(Self {
            shard_tables_cache,
            meta_client,
            topology: Default::default(),
        })
    }

    async fn route_tables(&self, req: &RouteTablesRequest) -> Result<RouteTablesResponse> {
        // TODO: we should use self.topology to cache the route result to reduce the
        // pressure on the CeresMeta.
        let route_resp = self
            .meta_client
            .route_tables(req.clone())
            .await
            .context(MetaClientFailure)?;

        Ok(route_resp)
    }

    async fn fetch_nodes(&self) -> Result<ClusterNodesResp> {
        {
            let topology = self.topology.read().unwrap();
            let cached_node_topology = topology.nodes();
            if let Some(cached_node_topology) = cached_node_topology {
                return Ok(ClusterNodesResp {
                    cluster_topology_version: cached_node_topology.version,
                    cluster_nodes: cached_node_topology.nodes,
                });
            }
        }

        let req = GetNodesRequest::default();
        let resp = self
            .meta_client
            .get_nodes(req)
            .await
            .context(MetaClientFailure)?;

        let version = resp.cluster_topology_version;
        let nodes = Arc::new(resp.node_shards);
        let updated = self
            .topology
            .write()
            .unwrap()
            .maybe_update_nodes(nodes.clone(), version);

        let resp = if updated {
            ClusterNodesResp {
                cluster_topology_version: version,
                cluster_nodes: nodes,
            }
        } else {
            let topology = self.topology.read().unwrap();
            // The fetched topology is outdated, and we will use the cache.
            let cached_node_topology =
                topology.nodes().context(ClusterNodesNotFound { version })?;
            ClusterNodesResp {
                cluster_topology_version: cached_node_topology.version,
                cluster_nodes: cached_node_topology.nodes,
            }
        };

        Ok(resp)
    }

    async fn open_shard(&self, req: &OpenShardRequest) -> Result<TablesOfShard> {
        let shard_info = req.shard.as_ref().context(OpenShard {
            shard_id: 0u32,
            msg: "missing shard info in the request",
        })?;

        if let Some(tables_of_shard) = self.shard_tables_cache.get(shard_info.id) {
            if tables_of_shard.shard_info.version == shard_info.version {
                info!(
                    "No need to open the exactly same shard again, shard_info:{:?}",
                    shard_info
                );
                return Ok(tables_of_shard);
            }
            ensure!(
                tables_of_shard.shard_info.version < shard_info.version,
                OpenShard {
                    shard_id: shard_info.id,
                    msg: format!("open a shard with a smaller version, curr_shard_info:{:?}, new_shard_info:{:?}", tables_of_shard.shard_info, shard_info),
                }
            );
        }

        let req = GetTablesOfShardsRequest {
            shard_ids: vec![shard_info.id],
        };

        let mut resp = self
            .meta_client
            .get_tables_of_shards(req)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(OpenShardWithCause {
                shard_id: shard_info.id,
            })?;

        ensure!(
            resp.tables_by_shard.len() == 1,
            OpenShard {
                shard_id: shard_info.id,
                msg: "expect only one shard tables"
            }
        );

        let tables_of_shard = resp
            .tables_by_shard
            .remove(&shard_info.id)
            .context(OpenShard {
                shard_id: shard_info.id,
                msg: "shard tables are missing from the response",
            })?;

        self.shard_tables_cache
            .insert_or_update(tables_of_shard.clone());

        Ok(tables_of_shard)
    }

    fn close_shard(&self, req: &CloseShardRequest) -> Result<TablesOfShard> {
        self.shard_tables_cache
            .remove(req.shard_id)
            .with_context(|| ShardNotFound {
                msg: format!("close non-existent shard, shard_id:{}", req.shard_id),
            })
    }

    fn create_table_on_shard(&self, req: &CreateTableOnShardRequest) -> Result<()> {
        let update_shard_info = req.update_shard_info.clone().context(ShardNotFound {
            msg: "update shard info is missing in CreateTableOnShardRequest",
        })?;
        let curr_shard_info = update_shard_info.curr_shard_info.context(ShardNotFound {
            msg: "current shard info is missing in UpdateShardInfo",
        })?;
        let table_info = req.table_info.clone().context(TableNotFound {
            msg: "table info is missing in CreateTableOnShardRequest",
        })?;

        self.shard_tables_cache.try_insert_table_to_shard(
            update_shard_info.prev_version,
            ShardInfo::from(curr_shard_info),
            TableInfo::from(table_info),
        )
    }

    fn drop_table_on_shard(&self, req: &DropTableOnShardRequest) -> Result<()> {
        let update_shard_info = req.update_shard_info.clone().context(ShardNotFound {
            msg: "update shard info is missing in DropTableOnShardRequest",
        })?;
        let curr_shard_info = update_shard_info.curr_shard_info.context(ShardNotFound {
            msg: "current shard info is missing in UpdateShardInfo",
        })?;
        let table_info = req.table_info.clone().context(TableNotFound {
            msg: "table info is missing in CreateTableOnShardRequest",
        })?;

        self.shard_tables_cache.try_remove_table_from_shard(
            update_shard_info.prev_version,
            ShardInfo::from(curr_shard_info),
            TableInfo::from(table_info),
        )
    }
}

#[async_trait]
impl Cluster for ClusterImpl {
    async fn start(&self) -> Result<()> {
        info!("Cluster is starting with config:{:?}", self.config);

        // start the background loop for sending heartbeat.
        self.start_heartbeat_loop();

        info!("Cluster has started");
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        info!("Cluster is stopping");

        {
            let tx = self.stop_heartbeat_tx.lock().unwrap().take();
            if let Some(tx) = tx {
                let _ = tx.send(()).await;
            }
        }

        {
            let handle = self.heartbeat_handle.lock().unwrap().take();
            if let Some(handle) = handle {
                let _ = handle.await;
            }
        }

        info!("Cluster has stopped");
        Ok(())
    }

    async fn open_shard(&self, req: &OpenShardRequest) -> Result<TablesOfShard> {
        self.inner.open_shard(req).await
    }

    async fn close_shard(&self, req: &CloseShardRequest) -> Result<TablesOfShard> {
        self.inner.close_shard(req)
    }

    async fn create_table_on_shard(&self, req: &CreateTableOnShardRequest) -> Result<()> {
        self.inner.create_table_on_shard(req)
    }

    async fn drop_table_on_shard(&self, req: &DropTableOnShardRequest) -> Result<()> {
        self.inner.drop_table_on_shard(req)
    }

    async fn route_tables(&self, req: &RouteTablesRequest) -> Result<RouteTablesResponse> {
        self.inner.route_tables(req).await
    }

    async fn fetch_nodes(&self) -> Result<ClusterNodesResp> {
        self.inner.fetch_nodes().await
    }
}
