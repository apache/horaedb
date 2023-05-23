// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use meta_client::types::{ShardId, ShardInfo, ShardVersion, TableInfo, TablesOfShard};
use snafu::{ensure, OptionExt};

use crate::{
    Result, ShardNotFound, ShardVersionMismatch, TableAlreadyExists, TableNotFound,
    UpdateFrozenShard,
};

/// [ShardTablesCache] caches the information about tables and shards, and the
/// relationship between them is: one shard -> multiple tables.
#[derive(Debug, Default, Clone)]
pub struct ShardTablesCache {
    inner: Arc<RwLock<Inner>>,
}

#[derive(Debug, Clone)]
pub struct TableWithShards {
    pub table_info: TableInfo,
    pub shard_infos: Vec<ShardInfo>,
}

impl ShardTablesCache {
    pub fn find_table_by_name(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Option<TableWithShards> {
        self.inner
            .read()
            .unwrap()
            .find_table_by_name(catalog, schema, table)
    }

    // Fetch all the shard infos.
    pub fn all_shard_infos(&self) -> Vec<ShardInfo> {
        self.inner.read().unwrap().all_shard_infos()
    }

    // Get the shard by its id.
    pub fn get(&self, shard_id: ShardId) -> Option<TablesOfShard> {
        self.inner.read().unwrap().get(shard_id)
    }

    /// Remove the shard.
    pub fn remove(&self, shard_id: ShardId) -> Option<TablesOfShard> {
        self.inner.write().unwrap().remove(shard_id)
    }

    /// Insert the tables of one shard.
    pub fn insert(&self, tables_of_shard: TablesOfShard) {
        self.inner.write().unwrap().insert(tables_of_shard)
    }

    /// Freeze the shard.
    pub fn freeze(&self, shard_id: ShardId) -> Option<TablesOfShard> {
        self.inner.write().unwrap().freeze(shard_id)
    }

    /// Try to insert a new table to the shard with a newer version.
    ///
    /// It will fail if:
    ///  - the shard doesn't exist, or
    ///  - the shard version in the cache is not equal to the provided
    ///    `prev_shard_version`, or
    ///  - the table already exists.
    pub fn try_insert_table_to_shard(
        &self,
        prev_shard_version: ShardVersion,
        curr_shard: ShardInfo,
        new_table: TableInfo,
    ) -> Result<()> {
        self.inner.write().unwrap().try_insert_table_to_shard(
            prev_shard_version,
            curr_shard,
            new_table,
        )
    }

    /// Try to remove a table from the shard with a newer version.
    ///
    /// It will fail if:
    ///  - the shard doesn't exist, or
    ///  - the shard version in the cache is not equal to the provided
    ///    `prev_shard_version`, or
    ///  - the table doesn't exist.
    pub fn try_remove_table_from_shard(
        &self,
        prev_shard_version: ShardVersion,
        curr_shard: ShardInfo,
        new_table: TableInfo,
    ) -> Result<()> {
        self.inner.write().unwrap().try_remove_table_from_shard(
            prev_shard_version,
            curr_shard,
            new_table,
        )
    }
}

#[derive(Clone, Debug)]
struct TablesOfShardCacheEntry {
    entry: TablesOfShard,
    frozen: bool,
}

#[derive(Debug, Default)]
struct Inner {
    // Tables organized by shard.
    // TODO: The shard roles should be also taken into considerations.
    tables_by_shard: HashMap<ShardId, TablesOfShardCacheEntry>,
}

impl Inner {
    pub fn find_table_by_name(
        &self,
        _catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Option<TableWithShards> {
        // TODO: We need a more efficient way to find TableWithShards.
        let mut table_info = None;
        let mut shard_infos = vec![];
        for tables_of_shard in self.tables_by_shard.values() {
            if let Some(v) = tables_of_shard
                .entry
                .tables
                .iter()
                .find(|table| table.schema_name == schema_name && table.name == table_name)
            {
                if table_info.is_none() {
                    table_info = Some(v.clone());
                }

                shard_infos.push(tables_of_shard.entry.shard_info.clone());
            }
        }

        table_info.map(|v| TableWithShards {
            table_info: v,
            shard_infos,
        })
    }

    fn all_shard_infos(&self) -> Vec<ShardInfo> {
        self.tables_by_shard
            .values()
            .map(|v| v.entry.shard_info.clone())
            .collect()
    }

    fn get(&self, shard_id: ShardId) -> Option<TablesOfShard> {
        self.tables_by_shard.get(&shard_id).map(|v| v.entry.clone())
    }

    fn remove(&mut self, shard_id: ShardId) -> Option<TablesOfShard> {
        self.tables_by_shard.remove(&shard_id).map(|v| v.entry)
    }

    fn freeze(&mut self, shard_id: ShardId) -> Option<TablesOfShard> {
        let tables_of_shard = self.tables_by_shard.get_mut(&shard_id)?;
        tables_of_shard.frozen = true;

        Some(tables_of_shard.entry.clone())
    }

    fn insert(&mut self, tables_of_shard: TablesOfShard) {
        let shard_id = tables_of_shard.shard_info.id;
        let entry = TablesOfShardCacheEntry {
            entry: tables_of_shard,
            frozen: false,
        };
        self.tables_by_shard.insert(shard_id, entry);
    }

    fn try_insert_table_to_shard(
        &mut self,
        prev_shard_version: ShardVersion,
        curr_shard: ShardInfo,
        new_table: TableInfo,
    ) -> Result<()> {
        let tables_of_shard = self
            .tables_by_shard
            .get_mut(&curr_shard.id)
            .with_context(|| ShardNotFound {
                msg: format!(
                    "insert table to a non-existent shard, shard_id:{}",
                    curr_shard.id
                ),
            })?;

        ensure!(
            !tables_of_shard.frozen,
            UpdateFrozenShard {
                shard_id: curr_shard.id,
            }
        );

        ensure!(
            tables_of_shard.entry.shard_info.version == prev_shard_version,
            ShardVersionMismatch {
                shard_info: tables_of_shard.entry.shard_info.clone(),
                expect_version: prev_shard_version,
            }
        );

        let table = tables_of_shard
            .entry
            .tables
            .iter()
            .find(|v| v.id == new_table.id);
        ensure!(
            table.is_none(),
            TableAlreadyExists {
                msg: "the table to insert has already existed",
            }
        );

        // Update tables of shard.
        tables_of_shard.entry.shard_info = curr_shard;
        tables_of_shard.entry.tables.push(new_table);

        Ok(())
    }

    fn try_remove_table_from_shard(
        &mut self,
        prev_shard_version: ShardVersion,
        curr_shard: ShardInfo,
        new_table: TableInfo,
    ) -> Result<()> {
        let tables_of_shard = self
            .tables_by_shard
            .get_mut(&curr_shard.id)
            .with_context(|| ShardNotFound {
                msg: format!(
                    "remove table from a non-existent shard, shard_id:{}",
                    curr_shard.id
                ),
            })?;

        ensure!(
            !tables_of_shard.frozen,
            UpdateFrozenShard {
                shard_id: curr_shard.id,
            }
        );

        ensure!(
            tables_of_shard.entry.shard_info.version == prev_shard_version,
            ShardVersionMismatch {
                shard_info: tables_of_shard.entry.shard_info.clone(),
                expect_version: prev_shard_version,
            }
        );

        let table_idx = tables_of_shard
            .entry
            .tables
            .iter()
            .position(|v| v.id == new_table.id)
            .with_context(|| TableNotFound {
                msg: format!("the table to remove is not found, table:{new_table:?}"),
            })?;

        // Update tables of shard.
        tables_of_shard.entry.shard_info = curr_shard;
        tables_of_shard.entry.tables.swap_remove(table_idx);

        Ok(())
    }
}
