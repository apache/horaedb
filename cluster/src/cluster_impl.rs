// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use async_trait::async_trait;
use common_types::table::ShardId;
use common_util::{
    error::BoxError,
    runtime::{JoinHandle, Runtime},
};
use etcd_client::ConnectOptions;
use log::{error, info, warn};
use meta_client::{
    types::{
        GetNodesRequest, GetTablesOfShardsRequest, RouteTablesRequest, RouteTablesResponse,
        ShardInfo,
    },
    MetaClientRef,
};
use snafu::{ensure, OptionExt, ResultExt};
use tokio::{
    sync::mpsc::{self, Sender},
    time,
};

use crate::{
    config::ClusterConfig,
    shard_lock_manager::{ShardLockManager, ShardLockManagerRef},
    shard_set::{Shard, ShardRef, ShardSet},
    topology::ClusterTopology,
    Cluster, ClusterNodesNotFound, ClusterNodesResp, EtcdClientFailureWithCause, InvalidArguments,
    MetaClientFailure, OpenShard, OpenShardWithCause, Result, ShardNotFound,
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
    shard_lock_manager: ShardLockManagerRef,
}

impl ClusterImpl {
    pub async fn try_new(
        node_name: String,
        shard_set: ShardSet,
        meta_client: MetaClientRef,
        config: ClusterConfig,
        runtime: Arc<Runtime>,
    ) -> Result<Self> {
        if let Err(e) = config.etcd_client.validate() {
            return InvalidArguments { msg: e }.fail();
        }

        let inner = Arc::new(Inner::new(shard_set, meta_client)?);
        let connect_options = ConnectOptions::from(&config.etcd_client);
        let etcd_client =
            etcd_client::Client::connect(&config.etcd_client.server_addrs, Some(connect_options))
                .await
                .context(EtcdClientFailureWithCause {
                    msg: "failed to connect to etcd",
                })?;

        let shard_lock_key_prefix = Self::shard_lock_key_prefix(
            &config.etcd_client.root_path,
            &config.meta_client.cluster_name,
        )?;
        let shard_lock_manager = ShardLockManager::new(
            shard_lock_key_prefix,
            node_name,
            etcd_client,
            config.etcd_client.shard_lock_lease_ttl_sec,
            config.etcd_client.shard_lock_lease_check_interval.0,
            config.etcd_client.rpc_timeout(),
            runtime.clone(),
        );
        Ok(Self {
            inner,
            runtime,
            config,
            heartbeat_handle: Mutex::new(None),
            stop_heartbeat_tx: Mutex::new(None),
            shard_lock_manager: Arc::new(shard_lock_manager),
        })
    }

    fn start_heartbeat_loop(&self) {
        let interval = self.heartbeat_interval();
        let error_wait_lease = self.error_wait_lease();
        let inner = self.inner.clone();
        let (tx, mut rx) = mpsc::channel(1);

        let handle = self.runtime.spawn(async move {
            loop {
                let shards = inner.shard_set.all_shards();
                let mut shard_infos = Vec::with_capacity(shards.len());
                for shard in shards {
                    let shard_info = shard.shard_info();
                    shard_infos.push(shard_info);
                }
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

    fn shard_lock_key_prefix(root_path: &str, cluster_name: &str) -> Result<String> {
        ensure!(
            root_path.starts_with('/'),
            InvalidArguments {
                msg: "root_path is required to start with /",
            }
        );

        ensure!(
            !cluster_name.is_empty(),
            InvalidArguments {
                msg: "cluster_name is required non-empty",
            }
        );

        const SHARD_LOCK_KEY: &str = "shards";
        Ok(format!("{root_path}/{cluster_name}/{SHARD_LOCK_KEY}"))
    }
}

struct Inner {
    shard_set: ShardSet,
    meta_client: MetaClientRef,
    topology: RwLock<ClusterTopology>,
}

impl Inner {
    fn new(shard_set: ShardSet, meta_client: MetaClientRef) -> Result<Self> {
        Ok(Self {
            shard_set,
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

    async fn open_shard(&self, shard_info: &ShardInfo) -> Result<ShardRef> {
        if let Some(shard) = self.shard_set.get(shard_info.id) {
            let cur_shard_info = shard.shard_info();
            if cur_shard_info.version == shard_info.version {
                info!(
                    "No need to open the exactly same shard again, shard_info:{:?}",
                    shard_info
                );
                return Ok(shard);
            }
            ensure!(
                cur_shard_info.version < shard_info.version,
                OpenShard {
                    shard_id: shard_info.id,
                    msg: format!("open a shard with a smaller version, curr_shard_info:{cur_shard_info:?}, new_shard_info:{shard_info:?}"),
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
            .box_err()
            .with_context(|| OpenShardWithCause {
                msg: format!("shard_info:{shard_info:?}"),
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

        let shard_id = tables_of_shard.shard_info.id;
        let shard = Arc::new(Shard::new(tables_of_shard));
        self.shard_set.insert(shard_id, shard.clone());

        Ok(shard)
    }

    fn shard(&self, shard_id: ShardId) -> Option<ShardRef> {
        self.shard_set.get(shard_id)
    }

    fn close_shard(&self, shard_id: ShardId) -> Result<ShardRef> {
        self.shard_set
            .remove(shard_id)
            .with_context(|| ShardNotFound {
                msg: format!("close non-existent shard, shard_id:{shard_id}"),
            })
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

    async fn open_shard(&self, shard_info: &ShardInfo) -> Result<ShardRef> {
        self.inner.open_shard(shard_info).await
    }

    fn shard(&self, shard_id: ShardId) -> Option<ShardRef> {
        self.inner.shard(shard_id)
    }

    async fn close_shard(&self, shard_id: ShardId) -> Result<ShardRef> {
        self.inner.close_shard(shard_id)
    }

    async fn route_tables(&self, req: &RouteTablesRequest) -> Result<RouteTablesResponse> {
        self.inner.route_tables(req).await
    }

    async fn fetch_nodes(&self) -> Result<ClusterNodesResp> {
        self.inner.fetch_nodes().await
    }

    fn shard_lock_manager(&self) -> ShardLockManagerRef {
        self.shard_lock_manager.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_shard_lock_key_prefix() {
        let cases = vec![
            (
                ("/ceresdb", "defaultCluster"),
                Some("/ceresdb/defaultCluster/shards"),
            ),
            (("", "defaultCluster"), None),
            (("vvv", "defaultCluster"), None),
            (("/x", ""), None),
        ];

        for ((root_path, cluster_name), expected) in cases {
            let actual = ClusterImpl::shard_lock_key_prefix(root_path, cluster_name);
            match expected {
                Some(expected) => assert_eq!(actual.unwrap(), expected),
                None => assert!(actual.is_err()),
            }
        }
    }
}
