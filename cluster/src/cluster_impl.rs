// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use async_trait::async_trait;
use common_types::table::TableName;
use common_util::runtime::{JoinHandle, Runtime};
use log::{error, info, warn};
use meta_client::{
    types::{
        ActionCmd, GetShardTablesRequest, RouteEntry, RouteTablesRequest, RouteTablesResponse,
    },
    EventHandler, MetaClient,
};
use snafu::ResultExt;
use tokio::{
    sync::mpsc::{self, Sender},
    time,
};

use crate::{
    config::ClusterConfig,
    table_manager::{ShardTableInfo, TableManager},
    topology::{ClusterTopology, RouteSlot},
    Cluster, MetaClientFailure, Result, StartMetaClient, TableManipulator,
};

/// ClusterImpl is an implementation of [`Cluster`] based [`MetaClient`].
///
/// Its functions are to:
/// * Receive and handle events from meta cluster by [`MetaClient`];
/// * Send heartbeat to meta cluster;
pub struct ClusterImpl {
    inner: Arc<Inner>,
    runtime: Arc<Runtime>,
    config: ClusterConfig,
    heartbeat_handle: Mutex<Option<JoinHandle<()>>>,
    stop_heartbeat_tx: Mutex<Option<Sender<()>>>,
}

impl ClusterImpl {
    pub fn new(
        meta_client: Arc<dyn MetaClient + Send + Sync>,
        table_manipulator: Arc<dyn TableManipulator + Send + Sync>,
        config: ClusterConfig,
        runtime: Arc<Runtime>,
    ) -> Result<Self> {
        let inner = Inner::new(meta_client, table_manipulator)?;

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
}

struct Inner {
    table_manager: TableManager,
    meta_client: Arc<dyn MetaClient + Send + Sync>,
    table_manipulator: Arc<dyn TableManipulator + Send + Sync>,
    topology: RwLock<ClusterTopology>,
}

#[async_trait]
impl EventHandler for Inner {
    fn name(&self) -> &str {
        "cluster_handler_for_meta_service"
    }

    async fn handle(
        &self,
        cmd: &ActionCmd,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Receive action command:{:?}", cmd);

        match cmd {
            ActionCmd::MetaOpenCmd(open_cmd) => {
                let resp = self
                    .meta_client
                    .get_tables(GetShardTablesRequest {
                        shard_ids: open_cmd.shard_ids.clone(),
                    })
                    .await
                    .context(MetaClientFailure)
                    .map_err(Box::new)?;

                for shard_tables in resp.shard_tables.values() {
                    for table in &shard_tables.tables {
                        self.table_manipulator
                            .open_table(&table.schema_name, &table.name, table.id)
                            .await?;
                    }
                }
                self.table_manager.update_table_info(&resp.shard_tables);

                Ok(())
            }
            ActionCmd::CreateTableCmd(cmd) => self
                .table_manager
                .add_shard_table(ShardTableInfo::from(cmd))
                .map_err(|e| Box::new(e) as _),
            ActionCmd::DropTableCmd(cmd) => {
                warn!("Drop table, schema:{}, table:{}", cmd.schema_name, cmd.name);

                self.table_manager.drop_table(&cmd.schema_name, &cmd.name);
                Ok(())
            }
            ActionCmd::MetaNoneCmd(_)
            | ActionCmd::MetaCloseCmd(_)
            | ActionCmd::MetaSplitCmd(_)
            | ActionCmd::MetaChangeRoleCmd(_) => {
                warn!("Nothing to do for cmd:{:?}", cmd);

                Ok(())
            }
        }
    }
}

fn make_route_update_patch(
    queried_tables: &[TableName],
    route_entries: &HashMap<TableName, RouteEntry>,
) -> HashMap<TableName, RouteSlot> {
    let mut slots = HashMap::with_capacity(queried_tables.len());

    for table_name in queried_tables {
        let slot = match route_entries.get(table_name) {
            Some(entry) => RouteSlot::Exist(entry.clone()),
            None => RouteSlot::NotExist,
        };
        slots.insert(table_name.clone(), slot);
    }

    slots
}

impl Inner {
    fn new(
        meta_client: Arc<dyn MetaClient + Send + Sync>,
        table_manipulator: Arc<dyn TableManipulator + Send + Sync>,
    ) -> Result<Self> {
        Ok(Self {
            table_manager: TableManager::default(),
            meta_client,
            table_manipulator,
            topology: Default::default(),
        })
    }

    async fn route_tables(&self, req: &RouteTablesRequest) -> Result<RouteTablesResponse> {
        let route_res = {
            let topology = self.topology.read().unwrap();
            topology.route_tables(&req.schema_name, &req.table_names)
        };

        if route_res.missing_tables.is_empty() {
            return Ok(RouteTablesResponse::from(route_res));
        }

        // TODO: Actually, only the missing tables' routing information needs fetching
        // from CeresMeta.

        // Some tables are not found in the cluster topology cache, and we need to fetch
        // them from CeresMeta.
        let route_resp = self
            .meta_client
            .route_tables(req.clone())
            .await
            .context(MetaClientFailure)?;

        // TODO: Besides such patching update, one background `CheckAndUpdate` job
        // should be supported to keep the topology cache latest.
        // TODO: Now the version is ignored because current topology is almost static.
        {
            // Apply the update patch to the topology cache.
            let update_patch = make_route_update_patch(&req.table_names, &route_resp.entries);
            let mut topology = self.topology.write().unwrap();
            topology.update_tables(
                &req.schema_name,
                update_patch,
                route_resp.cluster_topology_version,
            );
        }

        Ok(route_resp)
    }
}

#[async_trait]
impl Cluster for ClusterImpl {
    async fn start(&self) -> Result<()> {
        info!("Cluster is starting with config:{:?}", self.config);

        // register the event handler to meta client.
        self.inner
            .meta_client
            .register_event_handler(self.inner.clone())
            .await
            .context(MetaClientFailure)?;

        // start the meat_client after registering the event handler.
        self.inner
            .meta_client
            .start()
            .await
            .context(StartMetaClient)?;

        // start the background loop for sending heartbeat.
        self.start_heartbeat_loop();

        info!("Cluster has started");
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        info!("Cluster is stopping");

        if let Err(e) = self.inner.meta_client.stop().await {
            error!("Fail to stop meta client, err:{}", e);
        }

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

    async fn route_tables(&self, req: &RouteTablesRequest) -> Result<RouteTablesResponse> {
        self.inner.route_tables(req).await
    }
}
