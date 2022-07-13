use std::{
    collections::{BTreeMap, HashMap},
    sync::RwLock,
};

use meta_client_v2::{SchemaId, ShardId, ShardInfo, ShardTables, TableId, TableInfo};

use super::Result;
use crate::ShardNotFound;

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct SchemaInfo {
    name: String,
    id: SchemaId,
}

/// Manage table and shard correspondence information
pub struct TableManager {
    inner: RwLock<Inner>,
}

impl TableManager {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(Inner {
                shard_infos: HashMap::new(),
                schema_infos: HashMap::new(),
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

#[derive(Debug)]
#[allow(dead_code)]
struct ShardTableInfo {
    id: ShardId,
    table_info: TableInfo,
}

#[derive(Debug)]
struct Inner {
    shard_infos: HashMap<ShardId, ShardInfo>,
    schema_infos: HashMap<String, SchemaInfo>,
    // schema_name -> table_name -> shard_table_info
    tables: BTreeMap<String, BTreeMap<String, ShardTableInfo>>,
}

impl Inner {
    fn get_shards_info(&self) -> Vec<ShardInfo> {
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
                            id: shard_info.shard_id,
                            table_info: table.clone(),
                        },
                    );
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
        for shard in self.shard_infos.values() {
            if shard.shard_id == shard_id {
                shard_info = Some(shard);
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
                        ShardTableInfo {
                            id: v.shard_id,
                            table_info: TableInfo {
                                id: table_id,
                                name: table_name,
                                schema_id,
                                schema_name,
                            },
                        },
                    );
                Ok(())
            }
        }
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
