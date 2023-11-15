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

//! Manage meta data of the engine

pub mod details;
pub mod meta_edit;
pub mod meta_snapshot;

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use common_types::table::ShardId;
use generic_error::GenericResult;
use table_engine::table::TableId;

use crate::{
    manifest::meta_edit::MetaEditRequest, space::SpaceId, table::data::TableDataExtraneousInfo,
};

#[derive(Debug)]
pub struct LoadRequest {
    pub space_id: SpaceId,
    pub table_id: TableId,
    pub shard_id: ShardId,
    pub extr_info: TableDataExtraneousInfo,
}

pub type SnapshotRequest = LoadRequest;
/// Manifest holds meta data of all tables.
#[async_trait]
pub trait Manifest: Send + Sync + fmt::Debug {
    /// Apply edit to table metas, store it to storage.
    async fn apply_edit(&self, request: MetaEditRequest) -> GenericResult<()>;

    /// Recover table metas from storage.
    async fn recover(&self, load_request: &LoadRequest) -> GenericResult<()>;

    async fn do_snapshot(&self, request: SnapshotRequest) -> GenericResult<()>;
}

pub type ManifestRef = Arc<dyn Manifest>;
