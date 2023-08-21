// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Implementation of operations on shards.

// TODO: Currently, only a specific operation (close wal region) is implemented,
// and it is expected to encapsulate more operations on **Shard** in the future.

use std::sync::Arc;

use async_trait::async_trait;
use common_types::table::ShardId;
use generic_error::{BoxError, GenericResult};
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
