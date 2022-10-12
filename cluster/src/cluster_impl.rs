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
    types::{GetNodesRequest, GetShardTablesRequest, RouteTablesRequest, RouteTablesResponse},
    MetaClientRef,
};
use snafu::{ensure, OptionExt, ResultExt};
use tokio::{
    sync::mpsc::{self, Sender},
    time,
};

use crate::{
    config::ClusterConfig, table_manager::TableManager, topology::ClusterTopology, CloseShardOpts,
    Cluster, ClusterNodesNotFound, ClusterNodesResp, MetaClientFailure, OpenShard, OpenShardOpts,
    OpenShardWithCause, Result,
};

/// ClusterImpl is an implementation of [`Cluster`] based [`MetaClient`].
///
/// Its functions are to:
/// * Receive and handle events from meta cluster by [`MetaClient`];
/// * Send heartbeat to meta cluster;
/// * Manipulate resources via [TableEngine];
pub struct ClusterImpl {
    inner: Arc<Inner>,
    runtime: Arc<Runtime>,
    config: ClusterConfig,
    heartbeat_handle: Mutex<Option<JoinHandle<()>>>,
    stop_heartbeat_tx: Mutex<Option<Sender<()>>>,
}

impl ClusterImpl {
    pub fn new(
        meta_client: MetaClientRef,
        config: ClusterConfig,
        runtime: Arc<Runtime>,
    ) -> Result<Self> {
        let inner = Inner::new(meta_client)?;

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
                let shards_info = inner.table_manager.get_shards_infos();
                info!("Node heartbeat to meta, shards info:{:?}", shards_info);

                let resp = inner.meta_client.send_heartbeat(shards_info).await;
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

    pub fn table_manager(&self) -> &TableManager {
        &self.inner.table_manager
    }
}

struct Inner {
    table_manager: TableManager,
    meta_client: MetaClientRef,
    topology: RwLock<ClusterTopology>,
}

impl Inner {
    fn new(meta_client: MetaClientRef) -> Result<Self> {
        Ok(Self {
            table_manager: TableManager::default(),
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

    async fn open_shard(&self, req: &OpenShardRequest, _opts: OpenShardOpts) -> Result<()> {
        let shard_info = req.shard.as_ref().context(OpenShard {
            shard_id: 0u32,
            msg: "missing shard info in the request",
        })?;

        if self.table_manager.contains_shard(shard_info.shard_id) {
            OpenShard {
                shard_id: shard_info.shard_id,
                msg: "shard is already opened",
            }
            .fail()?;
        }

        let get_shard_tables_req = GetShardTablesRequest {
            shard_ids: vec![shard_info.shard_id],
        };

        let resp = self
            .meta_client
            .get_tables(get_shard_tables_req)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(OpenShardWithCause {
                shard_id: shard_info.shard_id,
            })?;

        ensure!(
            resp.shard_tables.len() == 1,
            OpenShard {
                shard_id: shard_info.shard_id,
                msg: "expect only one shard tables"
            }
        );

        let _shard_tables = resp
            .shard_tables
            .get(&shard_info.shard_id)
            .context(OpenShard {
                shard_id: shard_info.shard_id,
                msg: "shard tables are missing from the response",
            })?;

        self.table_manager.update_table_info(&resp.shard_tables);

        todo!("Do real open table");
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

    async fn open_shard(&self, req: &OpenShardRequest, opts: OpenShardOpts) -> Result<()> {
        self.inner.open_shard(req, opts).await
    }

    async fn close_shard(&self, _req: &CloseShardRequest, _opts: CloseShardOpts) -> Result<()> {
        todo!()
    }

    async fn create_table_on_shard(&self, _req: &CreateTableOnShardRequest) -> Result<()> {
        todo!();
    }

    async fn drop_table_on_shard(&self, _req: &DropTableOnShardRequest) -> Result<()> {
        todo!();
    }

    async fn route_tables(&self, req: &RouteTablesRequest) -> Result<RouteTablesResponse> {
        self.inner.route_tables(req).await
    }

    async fn fetch_nodes(&self) -> Result<ClusterNodesResp> {
        self.inner.fetch_nodes().await
    }
}
