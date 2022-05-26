// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{BTreeMap, HashMap},
    sync::RwLock,
};

use meta_client_v2::{SchemaId, ShardId, ShardInfo, ShardTables, TableId, TableInfo};

use super::Result;
use crate::ShardNotFound;

struct SchemaInfo {
    name: String,
    id: SchemaId,
}

pub struct TableManager {
    inner: RwLock<TableManagerInner>,
}

impl TableManager {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(TableManagerInner {
                shards_info: Vec::new(),
                schemas_info: HashMap::new(),
                tables: BTreeMap::new(),
            }),
        }
    }

    pub fn get_shards_info(&self) -> Vec<ShardInfo> {
        self.inner.read().unwrap().get_shards_info()
    }

    pub fn add_table(
        &self,
        shard_id: ShardId,
        schema_name: String,
        table_name: String,
        schema_id: SchemaId,
        table_id: TableId,
    ) -> Result<()> {
        self.inner.write().unwrap().add_table(
            shard_id,
            schema_name,
            table_name,
            schema_id,
            table_id,
        )
    }

    pub fn drop_table(&self, schema_name: String, table_name: String) {
        self.inner
            .write()
            .unwrap()
            .drop_table(schema_name, table_name)
    }

    pub fn update_table_info(&self, shard_table: HashMap<ShardId, ShardTables>) {
        self.inner.write().unwrap().update_table_info(shard_table)
    }

    pub fn get_schema_id(&self, schema_name: &str) -> Option<SchemaId> {
        self.inner.read().unwrap().get_schema_id(schema_name)
    }

    pub fn get_table_id(&self, schema_name: &str, table_name: &str) -> Option<TableId> {
        self.inner
            .read()
            .unwrap()
            .get_table_id(schema_name, table_name)
    }
}

struct TableManagerInner {
    shards_info: Vec<ShardInfo>,
    schemas_info: HashMap<String, SchemaInfo>,
    // schema_name -> table_name -> (shard_info, table_info)
    tables: BTreeMap<String, BTreeMap<String, (ShardInfo, TableInfo)>>,
}

impl TableManagerInner {
    fn get_shards_info(&self) -> Vec<ShardInfo> {
        self.shards_info.clone()
    }

    fn update_table_info(&mut self, shard_table: HashMap<ShardId, ShardTables>) {
        for (shard_id, shard_tables) in shard_table {
            let shard_info = ShardInfo {
                shard_id,
                role: shard_tables.role,
            };
            for table in shard_tables.tables {
                self.schemas_info
                    .entry(table.schema_name.clone())
                    .or_insert(SchemaInfo {
                        name: table.schema_name.clone(),
                        id: table.schema_id,
                    });
                self.tables
                    .entry(table.schema_name.clone())
                    .or_insert_with(BTreeMap::new)
                    .insert(table.name.clone(), (shard_info.clone(), table));
            }
        }
    }

    fn add_table(
        &mut self,
        shard_id: ShardId,
        schema_name: String,
        table_name: String,
        schema_id: SchemaId,
        table_id: TableId,
    ) -> Result<()> {
        let mut shard_info = None;
        for shard in &self.shards_info {
            if shard.shard_id == shard_id {
                shard_info = Some(shard.clone());
                break;
            }
        }
        match shard_info {
            None => ShardNotFound { shard_id }.fail(),
            Some(v) => {
                self.tables
                    .entry(schema_name.clone())
                    .or_insert_with(BTreeMap::new)
                    .insert(
                        table_name.clone(),
                        (
                            v,
                            TableInfo {
                                id: table_id,
                                name: table_name,
                                schema_id,
                                schema_name,
                            },
                        ),
                    );
                Ok(())
            }
        }
    }

    fn drop_table(&mut self, schema_name: String, table_name: String) {
        self.tables
            .get_mut(&schema_name)
            .map(|v| v.remove(&table_name));
    }

    fn get_schema_id(&self, schema_name: &str) -> Option<SchemaId> {
        self.schemas_info.get(schema_name).map(|v| v.id)
    }

    fn get_table_id(&self, schema_name: &str, table_name: &str) -> Option<TableId> {
        self.tables
            .get(schema_name)
            .and_then(|schema| schema.get(table_name).map(|v| v.1.id))
    }
}
