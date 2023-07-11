// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Implementation of operations on shards.

// TODO: Currently, only a specific operation (close wal region) is implemented,
// and it is expected to encapsulate more operations on **Shard** in the future.

use std::sync::Arc;

use async_trait::async_trait;
use common_types::table::ShardId;
use common_util::error::{BoxError, GenericResult};
use wal::manager::WalManagerRef;

#[async_trait]
pub trait WalRegionCloser: std::fmt::Debug + Send + Sync {
    async fn close_region(&self, shard_id: ShardId) -> GenericResult<()>;
}

pub type WalRegionCloserRef = Arc<dyn WalRegionCloser>;

#[derive(Debug)]
pub struct WalCloserAdapter {
    pub data_wal: WalManagerRef,
    pub manifest_wal: WalManagerRef,
}

#[async_trait]
impl WalRegionCloser for WalCloserAdapter {
    async fn close_region(&self, shard_id: ShardId) -> GenericResult<()> {
        let region_id = shard_id as u64;

        self.data_wal.close_region(region_id).await.box_err()?;
        self.manifest_wal.close_region(region_id).await.box_err()?;

        Ok(())
    }
}
