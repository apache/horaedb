// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Manage meta data of the engine

pub mod details;
pub mod meta_edit;
pub mod meta_snapshot;

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use common_types::table::ShardId;
use common_util::error::GenericResult;
use table_engine::table::TableId;

use crate::{manifest::meta_edit::MetaEditRequest, space::SpaceId};

#[derive(Debug, Clone)]
pub struct RecoverRequest {
    pub tables: Vec<TableInSpace>,
    pub shard_id: ShardId,
}

#[derive(Debug, Clone)]
pub struct TableInSpace {
    pub table_id: TableId,
    pub space_id: SpaceId,
}

#[derive(Debug)]
pub struct LoadRequest {
    pub space_id: SpaceId,
    pub table_id: TableId,
    pub shard_id: ShardId,
}

pub type SnapshotRequest = LoadRequest;

/// Manifest holds meta data of all tables.
#[async_trait]
pub trait Manifest: Send + Sync + fmt::Debug {
    /// Apply edit to table metas, store it to storage.
    async fn apply_edit(&self, request: MetaEditRequest) -> GenericResult<()>;

    /// Recover table metas from storage.
    async fn recover(&self, load_req: &RecoverRequest) -> GenericResult<Vec<RecoverResult>>;

    async fn do_snapshot(&self, request: SnapshotRequest) -> GenericResult<()>;
}

pub type ManifestRef = Arc<dyn Manifest>;

struct TableResult<T> {
    pub table_id: TableId,
    pub result: GenericResult<T>,
}

pub type RecoverResult = TableResult<()>;
