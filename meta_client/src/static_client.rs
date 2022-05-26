// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Static meta client.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use log::info;

use crate::{
    ClusterView, ClusterViewConfig, ClusterViewRef, MetaClient, MetaClientConfig, MetaWatcherPtr,
    Node, Result, ShardView,
};

/// Static meta client.
pub struct StaticMetaClient {
    cluster_view: ClusterViewRef,
    watcher: Option<MetaWatcherPtr>,
}

impl StaticMetaClient {
    pub fn new(config: MetaClientConfig, watcher: Option<MetaWatcherPtr>) -> Self {
        let cluster_view = match new_cluster_view(&config.cluster_view) {
            Some(v) => v,
            None => cluster_view_without_meta(&config.node, config.port),
        };

        Self {
            cluster_view: Arc::new(cluster_view),
            watcher,
        }
    }
}

#[async_trait]
impl MetaClient for StaticMetaClient {
    async fn start(&self) -> Result<()> {
        info!(
            "File meta client is starting, cluster_view:{:?}",
            self.cluster_view
        );

        info!("File meta client invoke watcher");

        if let Some(w) = &self.watcher {
            w.on_change(self.cluster_view.clone()).await?;
        }

        info!("File meta client has started");

        Ok(())
    }

    fn get_cluster_view(&self) -> ClusterViewRef {
        self.cluster_view.clone()
    }
}

fn new_cluster_view(config: &ClusterViewConfig) -> Option<ClusterView> {
    if config.schema_shards.is_empty() {
        return None;
    }

    Some(config.to_cluster_view())
}

fn cluster_view_without_meta(addr: &str, port: u16) -> ClusterView {
    let shard_id = 0;
    let mut static_shards = HashMap::new();
    static_shards.insert(
        shard_id,
        ShardView {
            shard_id,
            node: Node {
                addr: addr.to_string(),
                port: u32::from(port),
            },
        },
    );
    let mut schema_shards = HashMap::new();
    schema_shards.insert(catalog::consts::DEFAULT_SCHEMA.to_string(), static_shards);
    ClusterView {
        schema_shards,
        schema_configs: HashMap::default(),
    }
}
