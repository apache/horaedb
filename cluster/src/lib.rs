// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use catalog::manager::Manager;
use common_util::{define_result, runtime::Runtime};
use log::{error, info};
use meta_client_v2::{
    build_meta_client, ActionCmd, AllocSchemaIdRequest, AllocTableIdRequest, DropTableRequest,
    GetTablesRequest, MetaClient, NodeMetaInfo, SchemaId, ShardId, ShardInfo, TableId,
};
use snafu::{Backtrace, ResultExt, Snafu};
use tokio::{
    sync::{mpsc::Receiver, RwLock},
    time,
};

use crate::{config::ClusterConfig, table_manager::TableManager};

pub mod config;
mod table_manager;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(display("Build meta client failed, err:{}.", source))]
    BuildMetaClient {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Meta client start failed, err:{}.", source))]
    StartMetaClient {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Meta client start failed, err:{}.", source))]
    MetaClientFailure {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

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

#[async_trait]
pub trait Cluster {
    async fn alloc_schema_id(&self, _schema_name: String) -> Result<SchemaId>;

    async fn alloc_table_id(&self, _schema_name: String, _table_name: String) -> Result<TableId>;

    async fn drop_table(&self, _schema_name: String, _table_name: String) -> Result<()>;
}

pub struct ClusterImpl<M> {
    inner: Arc<ClusterImplInner<M>>,
    runtime: Arc<Runtime>,
}

impl<M: Manager + 'static> ClusterImpl<M> {
    pub fn new(config: ClusterConfig, catalog_manager: M, runtime: Arc<Runtime>) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(ClusterImplInner::new(
                config,
                catalog_manager,
                runtime.clone(),
            )?),
            runtime,
        })
    }

    pub async fn start(&self) -> Result<()> {
        let inner = self.inner.clone();
        inner
            .meta_client
            .start()
            .await
            .map_err(|e| Box::new(e) as _)
            .context(StartMetaClient)?;
        self.runtime.spawn(async move {
            inner.start_heartbeat().await;
        });

        Ok(())
    }
}

#[async_trait]
impl<M: Manager + 'static> Cluster for ClusterImpl<M> {
    async fn alloc_schema_id(&self, schema_name: String) -> Result<SchemaId> {
        self.inner.alloc_schema_id(schema_name).await
    }

    async fn alloc_table_id(&self, schema_name: String, table_name: String) -> Result<TableId> {
        self.inner.alloc_table_id(schema_name, table_name).await
    }

    async fn drop_table(&self, schema_name: String, table_name: String) -> Result<()> {
        self.inner.drop_table(schema_name, table_name).await
    }
}

struct ClusterImplInner<M> {
    meta_client: Arc<dyn MetaClient + Send + Sync>,
    catalog_manager: M,
    table_manager: TableManager,
    action_cmd_receiver: RwLock<Receiver<ActionCmd>>,

    config: ClusterConfig,
}

impl<M: Manager + 'static> ClusterImplInner<M> {
    pub fn new(config: ClusterConfig, catalog_manager: M, runtime: Arc<Runtime>) -> Result<Self> {
        let (sender, receiver) = tokio::sync::mpsc::channel(config.cmd_channel_buffer_size);
        let node_meta_info = NodeMetaInfo {
            node: config.node.clone(),
            zone: config.zone.clone(),
            idc: config.idc.clone(),
            binary_version: config.binary_version.clone(),
        };
        Ok(Self {
            meta_client: build_meta_client(
                config.meta_client_config.clone(),
                node_meta_info,
                runtime,
                Some(sender),
            )
            .map_err(|e| Box::new(e) as _)
            .context(BuildMetaClient)?,
            catalog_manager,
            table_manager: TableManager::new(),
            action_cmd_receiver: RwLock::new(receiver),
            config: config,
        })
    }

    // heartbeat
    async fn start_heartbeat(&self) {
        let mut interval = time::interval(self.heartbeat_interval());

        loop {
            let shards_info = self.get_shards_info();
            info!("Node heartbeat to meta, shards info:{:?}", shards_info);
            let resp = self.meta_client.send_heartbeat(shards_info).await;
            match resp {
                Ok(()) => {
                    interval.tick().await;
                }
                Err(e) => {
                    error!("Node heartbeat to meta failed, error:{}", e);
                    time::sleep(self.error_wait_lease()).await;
                }
            }
        }
    }

    async fn start_node_action_cmd(&self) {
        let action_cmd_receiver = &mut *self.action_cmd_receiver.write().await;
        // todo: handle error
        while let Some(action_cmd) = action_cmd_receiver.recv().await {
            info!(
                "Node action cmd from meta received, action_cmd:{:?}",
                action_cmd
            );
            match action_cmd {
                ActionCmd::OpenCmd(open_cmd) => {
                    let ret = self
                        .meta_client
                        .get_tables(GetTablesRequest {
                            shard_ids: open_cmd.shard_ids,
                        })
                        .await;
                    match ret {
                        Err(ref e) => error!("Get shard tables failed, ret:{:?}, err:{}", ret, e),
                        Ok(v) => {
                            self.table_manager.update_table_info(v.tables_map);
                            // todo: self.catalog_manager.open tables
                        }
                    }
                }
                // todo: other action cmd
                _ => todo!(),
            }
        }
        info!("Node action cmd receiver exit");
    }

    fn get_shards_info(&self) -> Vec<ShardInfo> {
        self.table_manager.get_shards_info()
    }

    // Register node every 2/3 lease
    fn heartbeat_interval(&self) -> Duration {
        Duration::from_secs(self.config.meta_client_config.lease.as_secs() * 2 / 3)
    }

    fn error_wait_lease(&self) -> Duration {
        Duration::from_secs(self.config.meta_client_config.lease.as_secs() / 2)
    }

    async fn alloc_schema_id(&self, schema_name: String) -> Result<SchemaId> {
        if let Some(v) = self.table_manager.get_schema_id(&schema_name) {
            Ok(v)
        } else {
            Ok(self
                .meta_client
                .alloc_schema_id(AllocSchemaIdRequest {
                    name: schema_name.clone(),
                })
                .await
                .map_err(|e| Box::new(e) as _)
                .context(MetaClientFailure)?
                .id)
        }
    }

    async fn alloc_table_id(&self, schema_name: String, table_name: String) -> Result<TableId> {
        if let Some(v) = self.table_manager.get_table_id(&schema_name, &table_name) {
            Ok(v)
        } else {
            let resp = self
                .meta_client
                .alloc_table_id(AllocTableIdRequest {
                    schema_name,
                    name: table_name,
                })
                .await
                .map_err(|e| Box::new(e) as _)
                .context(MetaClientFailure)?;
            self.table_manager.add_table(
                resp.shard_id,
                resp.schema_name,
                resp.name,
                resp.schema_id,
                resp.id,
            )?;
            Ok(resp.id)
        }
    }

    async fn drop_table(&self, schema_name: String, table_name: String) -> Result<()> {
        let _resp = self
            .meta_client
            .drop_table(DropTableRequest {
                schema_name: schema_name.clone(),
                name: table_name.clone(),
            })
            .await
            .map_err(|e| Box::new(e) as _)
            .context(MetaClientFailure)?;
        self.table_manager.drop_table(schema_name, table_name);
        Ok(())
    }
}
