// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{BTreeMap, HashMap},
    sync::RwLock,
};

use meta_client_v2::{AddTableCmd, SchemaId, ShardId, ShardInfo, ShardTables, TableId, TableInfo};
use snafu::OptionExt;

use crate::{Result, ShardNotFound};

pub type TableName = String;
pub type SchemaName = String;

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct SchemaInfo {
    name: SchemaName,
    id: SchemaId,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ShardTableInfo {
    pub shard_id: ShardId,
    pub table_info: TableInfo,
}

impl From<&AddTableCmd> for ShardTableInfo {
    fn from(cmd: &AddTableCmd) -> Self {
        let table_info = TableInfo {
            id: cmd.id,
            name: cmd.name.clone(),
            schema_id: cmd.schema_id,
            schema_name: cmd.schema_name.clone(),
        };
        ShardTableInfo {
            shard_id: cmd.shard_id,
            table_info,
        }
    }
}

/// TableManager manages information about tables, shards, schemas and their
/// relationships:
/// * one shard -> multiple tables
/// * one schema -> multiple tables
#[derive(Debug, Default)]
pub struct TableManager {
    inner: RwLock<Inner>,
}

impl TableManager {
    pub fn get_shards_infos(&self) -> Vec<ShardInfo> {
        self.inner.read().unwrap().get_shards_infos()
    }

    pub fn add_shard_table(&self, shard_table: ShardTableInfo) -> Result<()> {
        self.inner.write().unwrap().add_shard_table(shard_table)
    }

    pub fn drop_table(&self, schema_name: &str, table_name: &str) {
        self.inner
            .write()
            .unwrap()
            .drop_table(schema_name, table_name)
    }

    pub fn update_table_info(&self, shard_table: &HashMap<ShardId, ShardTables>) {
        self.inner.write().unwrap().update_table_info(shard_table)
    }

    #[allow(dead_code)]
    pub fn get_schema_id(&self, schema_name: &str) -> Option<SchemaId> {
        self.inner.read().unwrap().get_schema_id(schema_name)
    }

    #[allow(dead_code)]
    pub fn get_table_id(&self, schema_name: &str, table_name: &str) -> Option<TableId> {
        self.inner
            .read()
            .unwrap()
            .get_table_id(schema_name, table_name)
    }
}

#[derive(Debug, Default)]
struct Inner {
    shard_infos: HashMap<ShardId, ShardInfo>,
    schema_infos: HashMap<String, SchemaInfo>,
    tables: BTreeMap<SchemaName, BTreeMap<TableName, ShardTableInfo>>,
}

impl Inner {
    fn get_shards_infos(&self) -> Vec<ShardInfo> {
        self.shard_infos.values().cloned().collect()
    }

    fn update_table_info(&mut self, shard_table: &HashMap<ShardId, ShardTables>) {
        for (shard_id, shard_tables) in shard_table {
            let shard_info = ShardInfo {
                shard_id: *shard_id,
                role: shard_tables.role,
            };
            self.shard_infos.insert(*shard_id, shard_info.clone());
            for table in &shard_tables.tables {
                self.schema_infos
                    .entry(table.schema_name.clone())
                    .or_insert(SchemaInfo {
                        name: table.schema_name.clone(),
                        id: table.schema_id,
                    });
                self.tables
                    .entry(table.schema_name.clone())
                    .or_insert_with(BTreeMap::new)
                    .insert(
                        table.name.clone(),
                        ShardTableInfo {
                            shard_id: shard_info.shard_id,
                            table_info: table.clone(),
                        },
                    );
            }
        }
    }

    fn find_shard_info(&self, shard_id: ShardId) -> Option<ShardInfo> {
        self.shard_infos
            .values()
            .find(|info| info.shard_id == shard_id)
            .cloned()
    }

    fn add_shard_table(&mut self, shard_table: ShardTableInfo) -> Result<()> {
        self.find_shard_info(shard_table.shard_id)
            .context(ShardNotFound {
                shard_id: shard_table.shard_id,
            })?;

        self.tables
            .entry(shard_table.table_info.schema_name.clone())
            .or_insert_with(BTreeMap::new)
            .insert(shard_table.table_info.name.clone(), shard_table);

        Ok(())
    }

    fn drop_table(&mut self, schema_name: &str, table_name: &str) {
        self.tables
            .get_mut(schema_name)
            .map(|v| v.remove(table_name));
    }

    fn get_schema_id(&self, schema_name: &str) -> Option<SchemaId> {
        self.schema_infos.get(schema_name).map(|v| v.id)
    }

    fn get_table_id(&self, schema_name: &str, table_name: &str) -> Option<TableId> {
        self.tables
            .get(schema_name)
            .and_then(|schema| schema.get(table_name).map(|v| v.table_info.id))
    }
}
