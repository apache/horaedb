use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use common_util::runtime::Runtime;
use log::{debug, error, info};
use meta_client_v2::{ActionCmd, EventHandler, GetTablesRequest, MetaClient};
use snafu::ResultExt;
use tokio::time;

use crate::{
    config::ClusterConfig,
    table_manager::{ShardTableInfo, TableManager},
    Cluster, MetaClientFailure, Result, StartMetaClient, TableManipulator,
};

pub struct ClusterImpl {
    inner: Arc<Inner>,
    runtime: Arc<Runtime>,
    config: ClusterConfig,
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
        })
    }

    // Register node every 2/3 lease
    fn heartbeat_interval(&self) -> Duration {
        Duration::from_secs(self.config.meta_client_config.lease.as_millis() * 2 / 3)
    }

    fn error_wait_lease(&self) -> Duration {
        Duration::from_secs(self.config.meta_client_config.lease.as_millis() / 2)
    }
}

struct Inner {
    table_manager: TableManager,
    meta_client: Arc<dyn MetaClient + Send + Sync>,
    table_manipulator: Arc<dyn TableManipulator + Send + Sync>,
}

#[async_trait]
impl EventHandler for Inner {
    fn name(&self) -> &str {
        "cluster_handler_for_meta_service"
    }

    async fn handle(
        &self,
        event: &ActionCmd,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match event {
            ActionCmd::MetaOpenCmd(open_cmd) => {
                let resp = self
                    .meta_client
                    .get_tables(GetTablesRequest {
                        shard_ids: open_cmd.shard_ids.clone(),
                    })
                    .await
                    .context(MetaClientFailure)
                    .map_err(Box::new)?;

                debug!(
                    "EventHandler handle open cmd:{:?}, get tables:{:?}",
                    open_cmd, resp
                );

                for shard_tables in resp.tables_map.values() {
                    for table in &shard_tables.tables {
                        self.table_manipulator
                            .open_table(&table.schema_name, &table.name, table.id)
                            .await?;
                    }
                }
                self.table_manager.update_table_info(&resp.tables_map);

                Ok(())
            }
            ActionCmd::MetaNoneCmd(_) => Ok(()),
            ActionCmd::AddTableCmd(cmd) => self
                .table_manager
                .add_shard_table(ShardTableInfo::from(cmd))
                .map_err(|e| Box::new(e) as _),
            ActionCmd::DropTableCmd(cmd) => {
                self.table_manager.drop_table(&cmd.schema_name, &cmd.name);
                Ok(())
            }
            action_cmd => todo!("handle other action cmd:{:?}", action_cmd),
        }
    }
}

impl Inner {
    pub fn new(
        meta_client: Arc<dyn MetaClient + Send + Sync>,
        table_manipulator: Arc<dyn TableManipulator + Send + Sync>,
    ) -> Result<Self> {
        Ok(Self {
            table_manager: TableManager::default(),
            meta_client,
            table_manipulator,
        })
    }
}

#[async_trait]
impl Cluster for ClusterImpl {
    async fn start(&self) -> Result<()> {
        let inner = self.inner.clone();

        inner
            .meta_client
            .register_event_handler(inner.clone())
            .await
            .context(MetaClientFailure)?;
        inner.meta_client.start().await.context(StartMetaClient)?;

        let mut interval = time::interval(self.heartbeat_interval());
        let error_wait_lease = self.error_wait_lease();
        self.runtime.spawn(async move {
            loop {
                let shards_info = inner.table_manager.get_shards_infos();
                info!("Node heartbeat to meta, shards info:{:?}", shards_info);

                let resp = inner.meta_client.send_heartbeat(shards_info).await;
                match resp {
                    Ok(()) => {
                        interval.tick().await;
                    }
                    Err(e) => {
                        error!("Node heartbeat to meta failed, error:{}", e);
                        time::sleep(error_wait_lease).await;
                    }
                }
            }
        });

        Ok(())
    }
}
