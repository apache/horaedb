// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use meta_client::types::{ShardId, ShardInfo, TableInfo, TablesOfShard};
use snafu::{ensure, OptionExt};

use crate::{Result, ShardVersionMismatch, TableAlreadyExists, TableNotFound, UpdateFrozenShard};

/// [ShardTablesCache] caches the information about tables and shards, and the
/// relationship between them is: one shard -> multiple tables.
#[derive(Debug, Default, Clone)]
pub struct ShardSet {
    inner: Arc<RwLock<Inner>>,
}

#[derive(Debug, Clone)]
pub struct TableWithShards {
    pub table_info: TableInfo,
    pub shard_infos: Vec<ShardInfo>,
}

pub struct UpdatedTableInfo {
    pub prev_version: u64,
    pub shard_info: ShardInfo,
    pub table_info: TableInfo,
}

impl ShardSet {
    // Fetch all the shard infos.
    pub fn all_shards(&self) -> Vec<ShardRef> {
        self.inner.read().unwrap().all_shard_infos()
    }

    // Get the shard by its id.
    pub fn get(&self, shard_id: ShardId) -> Option<ShardRef> {
        self.inner.read().unwrap().get(shard_id)
    }

    /// Remove the shard.
    pub fn remove(&self, shard_id: ShardId) -> Option<ShardRef> {
        self.inner.write().unwrap().remove(shard_id)
    }

    /// Insert the tables of one shard.
    pub fn insert(&self, shard_id: ShardId, shard: ShardRef) {
        self.inner.write().unwrap().insert(shard_id, shard)
    }
}

#[derive(Debug)]
pub struct Shard {
    /// Shard info
    pub shard_info: ShardInfo,

    /// Tables in shard
    pub tables: Vec<TableInfo>,

    /// Flag indicating that further updates are prohibited
    pub frozen: bool,
}

impl Shard {
    pub fn freeze(&mut self) {
        self.frozen = true;
    }

    pub fn try_insert_table(&mut self, updated_info: UpdatedTableInfo) -> Result<()> {
        let UpdatedTableInfo {
            prev_version: prev_shard_version,
            shard_info: curr_shard,
            table_info: new_table,
        } = updated_info;

        ensure!(
            !self.frozen,
            UpdateFrozenShard {
                shard_id: curr_shard.id,
            }
        );

        ensure!(
            self.shard_info.version == prev_shard_version,
            ShardVersionMismatch {
                shard_info: self.shard_info.clone(),
                expect_version: prev_shard_version,
            }
        );

        let table = self.tables.iter().find(|v| v.id == new_table.id);
        ensure!(
            table.is_none(),
            TableAlreadyExists {
                msg: "the table to insert has already existed",
            }
        );

        // Update tables of shard.
        self.shard_info = curr_shard;
        self.tables.push(new_table);

        Ok(())
    }

    pub fn try_remove_table(&mut self, updated_info: UpdatedTableInfo) -> Result<()> {
        let UpdatedTableInfo {
            prev_version: prev_shard_version,
            shard_info: curr_shard,
            table_info: new_table,
        } = updated_info;

        ensure!(
            !self.frozen,
            UpdateFrozenShard {
                shard_id: curr_shard.id,
            }
        );

        ensure!(
            self.shard_info.version == prev_shard_version,
            ShardVersionMismatch {
                shard_info: self.shard_info.clone(),
                expect_version: prev_shard_version,
            }
        );

        let table_idx = self
            .tables
            .iter()
            .position(|v| v.id == new_table.id)
            .with_context(|| TableNotFound {
                msg: format!("the table to remove is not found, table:{new_table:?}"),
            })?;

        // Update tables of shard.
        self.shard_info = curr_shard;
        self.tables.swap_remove(table_idx);

        Ok(())
    }
}

pub type ShardRef = Arc<tokio::sync::RwLock<Shard>>;

impl From<TablesOfShard> for Shard {
    fn from(value: TablesOfShard) -> Self {
        Shard {
            shard_info: value.shard_info,
            tables: value.tables,
            frozen: false,
        }
    }
}

#[derive(Debug, Default)]
struct Inner {
    // Tables organized by shard.
    // TODO: The shard roles should be also taken into considerations.
    shards: HashMap<ShardId, ShardRef>,
}

impl Inner {
    fn all_shard_infos(&self) -> Vec<ShardRef> {
        self.shards.values().cloned().collect()
    }

    fn get(&self, shard_id: ShardId) -> Option<ShardRef> {
        self.shards.get(&shard_id).cloned()
    }

    fn remove(&mut self, shard_id: ShardId) -> Option<ShardRef> {
        self.shards.remove(&shard_id)
    }

    fn insert(&mut self, shard_id: ShardId, shard: ShardRef) {
        self.shards.insert(shard_id, shard);
    }
}
