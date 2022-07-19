use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use common_util::{define_result, runtime::Runtime};
use log::{debug, error, info};
use meta_client_v2::{ActionCmd, EventHandler, MetaClient, ShardId, TableId, TableInfo};
pub use meta_client_v2::{
    AllocSchemaIdRequest, AllocSchemaIdResponse, AllocTableIdRequest, AllocTableIdResponse,
    DropTableRequest, DropTableResponse, GetTablesRequest,
};
use snafu::{Backtrace, ResultExt, Snafu};
use tokio::time;

use crate::{
    config::ClusterConfig,
    table_manager::{ShardTableInfo, TableManager},
};

pub mod config;
mod table_manager;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(display("Build meta client failed, err:{}.", source))]
    BuildMetaClient { source: meta_client_v2::Error },

    #[snafu(display("Meta client start failed, err:{}.", source))]
    StartMetaClient { source: meta_client_v2::Error },

    #[snafu(display("Meta client execute failed, err:{}.", source))]
    MetaClientFailure { source: meta_client_v2::Error },

    #[snafu(display(
        "Shard not found in current node, shard_id:{}.\nBacktrace:\n{}",
        shard_id,
        backtrace
    ))]
    ShardNotFound {
        shard_id: ShardId,
        backtrace: Backtrace,
    },
}

define_result!(Error);

pub type ClusterRef = Arc<dyn Cluster + Send + Sync>;

pub type TableManipulatorRef = Arc<dyn TableManipulator + Send + Sync>;

#[async_trait]
pub trait TableManipulator {
    async fn open_table(
        &self,
        schema_name: &str,
        table_name: &str,
        table_id: TableId,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn close_table(
        &self,
        schema_name: &str,
        table_name: &str,
        table_id: TableId,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

// TODO: add more methods
/// Cluster manages tables and shard infos in cluster mode.
#[async_trait]
pub trait Cluster {
    async fn start(&self) -> Result<()>;
}

pub struct ClusterImpl {
    inner: Arc<ClusterImplInner>,
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
        let inner = ClusterImplInner::new(meta_client, table_manipulator)?;

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

struct ClusterImplInner {
    table_manager: TableManager,
    meta_client: Arc<dyn MetaClient + Send + Sync>,
    table_manipulator: Arc<dyn TableManipulator + Send + Sync>,
}

#[async_trait]
impl EventHandler for ClusterImplInner {
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
            ActionCmd::AddTableCmd(cmd) => {
                let table_info = TableInfo {
                    id: cmd.id,
                    name: cmd.name.clone(),
                    schema_id: cmd.schema_id,
                    schema_name: cmd.schema_name.clone(),
                };
                let shard_table = ShardTableInfo {
                    id: cmd.shard_id,
                    table_info: table_info,
                };
                self.table_manager
                    .add_shard_table(shard_table)
                    .map_err(|e| Box::new(e) as _)
            }
            ActionCmd::DropTableCmd(cmd) => {
                self.table_manager.drop_table(&cmd.schema_name, &cmd.name);
                Ok(())
            }
            action_cmd => todo!("handle other action cmd:{:?}", action_cmd),
        }
    }
}

impl ClusterImplInner {
    pub fn new(
        meta_client: Arc<dyn MetaClient + Send + Sync>,
        table_manipulator: Arc<dyn TableManipulator + Send + Sync>,
    ) -> Result<Self> {
        Ok(Self {
            table_manager: TableManager::new(),
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
                let shards_info = inner.table_manager.get_shards_info();
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
