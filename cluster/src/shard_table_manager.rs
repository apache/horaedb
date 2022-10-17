// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use meta_client::types::{ShardId, ShardInfo, TableInfo, TablesOfShard};

/// [ShardTableManager] manages information about tables and shards, and the
/// relationship between them is: one shard -> multiple tables.
#[derive(Debug, Default, Clone)]
pub struct ShardTableManager {
    inner: Arc<RwLock<Inner>>,
}

#[derive(Debug, Clone)]
pub struct TableWithShards {
    pub table_info: TableInfo,
    pub shard_infos: Vec<ShardInfo>,
}

impl ShardTableManager {
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

    // Check whether the manager contains the shard.
    pub fn contains_shard(&self, shard_id: ShardId) -> bool {
        self.inner.read().unwrap().contains_shard(shard_id)
    }

    /// Remove the shard.
    pub fn remove_shard(&self, shard_id: ShardId) -> Option<TablesOfShard> {
        self.inner.write().unwrap().remove_shard(shard_id)
    }

    /// Update the tables of one shard.
    pub fn update_tables_of_shard(&self, tables_of_shard: TablesOfShard) {
        self.inner
            .write()
            .unwrap()
            .update_tables_of_shard(tables_of_shard)
    }

    /// Update the tables of multiple shards.
    pub fn update_tables_by_shard(&self, tables_by_shard: HashMap<ShardId, TablesOfShard>) {
        self.inner
            .write()
            .unwrap()
            .update_tables_by_shard(tables_by_shard)
    }
}

#[derive(Debug, Default)]
struct Inner {
    // Tables organized by shard.
    // TODO: take considerations about the role.
    tables_by_shard: HashMap<ShardId, TablesOfShard>,
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
                .tables
                .iter()
                .find(|table| table.schema_name == schema_name && table.name == table_name)
            {
                if table_info.is_none() {
                    table_info = Some(v.clone());
                }

                shard_infos.push(tables_of_shard.shard_info.clone());
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
            .map(|v| v.shard_info.clone())
            .collect()
    }

    fn contains_shard(&self, shard_id: ShardId) -> bool {
        self.tables_by_shard.contains_key(&shard_id)
    }

    fn remove_shard(&mut self, shard_id: ShardId) -> Option<TablesOfShard> {
        self.tables_by_shard.remove(&shard_id)
    }

    fn update_tables_of_shard(&mut self, tables_of_shard: TablesOfShard) {
        self.tables_by_shard
            .insert(tables_of_shard.shard_info.id, tables_of_shard);
    }

    fn update_tables_by_shard(&mut self, tables_by_shard: HashMap<ShardId, TablesOfShard>) {
        for (shard_id, tables) in tables_by_shard {
            self.tables_by_shard.insert(shard_id, tables);
        }
    }
}
