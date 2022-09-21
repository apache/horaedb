// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{Arc, RwLock},
};

use catalog::consts::{self, DEFAULT_CATALOG};
use common_types::{
    schema::{CatalogName, SchemaId, SchemaName},
    table::TableId,
};
use meta_client::types::{CreateTableCmd, ShardId, ShardInfo, ShardTables, TableInfo};
use table_engine::table::TableRef;

use crate::Result;

#[derive(Debug, Clone)]
pub struct SchemaInfo {
    pub name: SchemaName,
    pub id: SchemaId,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ShardTableInfo {
    pub shard_id: ShardId,
    pub table_info: TableInfo,
}

impl From<&CreateTableCmd> for ShardTableInfo {
    fn from(cmd: &CreateTableCmd) -> Self {
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
#[derive(Debug, Default, Clone)]
pub struct TableManager {
    inner: Arc<RwLock<Inner>>,
}

impl TableManager {
    pub fn get_shards_infos(&self) -> Vec<ShardInfo> {
        self.inner.read().unwrap().get_shards_infos()
    }

    pub fn add_shard_table(&self, table_info: TableInfo, table: TableRef) -> Result<()> {
        self.inner.write().unwrap().add_table(table_info, table);

        Ok(())
    }

    pub fn update_table_info(&self, shard_table: &HashMap<ShardId, ShardTables>) {
        self.inner.write().unwrap().update_table_info(shard_table)
    }

    pub fn get_catalog_name(&self, catalog: &str) -> Option<String> {
        if self
            .inner
            .read()
            .unwrap()
            .catalog_infos
            .contains_key(catalog)
        {
            Some(catalog.to_string())
        } else {
            None
        }
    }

    pub fn get_all_catalog_names(&self) -> Vec<String> {
        self.inner
            .read()
            .unwrap()
            .catalog_infos
            .keys()
            .cloned()
            .collect()
    }

    pub fn get_schema_id(&self, catalog: &str, schema: &str) -> Option<SchemaId> {
        self.inner
            .read()
            .unwrap()
            .catalog_infos
            .get(catalog)?
            .get(schema)
            .map(|info| info.id)
    }

    pub fn get_all_schema_infos(&self, catalog: &str) -> Vec<SchemaInfo> {
        if let Some(schemas) = self.inner.read().unwrap().catalog_infos.get(catalog) {
            schemas.values().cloned().collect()
        } else {
            vec![]
        }
    }

    // todo: should accept schema id as param instead of schema name?
    pub fn get_all_table_ref(&self, catalog: &str, schema: &str) -> Vec<TableRef> {
        let schema_id = if let Some(id) = self.inner.read().unwrap().get_schema_id(schema) {
            id
        } else {
            return vec![];
        };

        self.inner
            .read()
            .unwrap()
            .tables_by_token
            .iter()
            .filter_map(|(token, table)| {
                if token.catalog == catalog && token.schema == schema_id {
                    Some(table.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn table_by_name(&self, catalog: &str, schema: &str, table_name: &str) -> Option<TableRef> {
        let schema_id = self.inner.read().unwrap().get_schema_id(schema)?;

        self.inner
            .read()
            .unwrap()
            .tables_by_token
            .iter()
            .find(|(token, table)| {
                token.catalog == catalog && token.schema == schema_id && table.name() == table_name
            })
            .map(|(_, table)| table.clone())
    }
}

#[derive(Debug, Default)]
struct Inner {
    // shard infos
    shard_infos: HashMap<ShardId, ShardInfo>,

    // catalog/schema infos
    catalog_infos: HashMap<CatalogName, HashMap<SchemaName, SchemaInfo>>,

    // table handles
    tables_by_token: BTreeMap<TableToken, TableRef>,
    #[allow(dead_code)]
    tokens_by_shard: HashMap<ShardId, HashSet<TableToken>>,
}

#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Debug, Default)]
struct TableToken {
    // todo: change this to CatalogId
    catalog: String,
    schema: SchemaId,
    table: TableId,
}

impl TableToken {
    fn from_table_info(info: TableInfo) -> Self {
        Self {
            catalog: DEFAULT_CATALOG.to_string(),
            schema: info.schema_id,
            table: info.id,
        }
    }
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
                version: shard_tables.version,
            };
            self.shard_infos.insert(*shard_id, shard_info.clone());
            for table in &shard_tables.tables {
                self.catalog_infos
                    .get_mut(consts::DEFAULT_CATALOG)
                    .unwrap()
                    .entry(table.schema_name.clone())
                    .or_insert(SchemaInfo {
                        name: table.schema_name.clone(),
                        id: table.schema_id,
                    });

                todo!()
            }
        }
    }

    #[allow(dead_code)]
    fn find_shard_info(&self, shard_id: ShardId) -> Option<ShardInfo> {
        self.shard_infos
            .values()
            .find(|info| info.shard_id == shard_id)
            .cloned()
    }

    // TODO: also request `ShardId` here
    fn add_table(&mut self, table_info: TableInfo, table: TableRef) {
        self.tables_by_token
            .insert(TableToken::from_table_info(table_info), table);
    }

    // TODO: also request `ShardId` here
    #[allow(dead_code)]
    fn drop_table(&mut self, table_info: TableInfo) {
        self.tables_by_token
            .remove(&TableToken::from_table_info(table_info));
    }

    fn get_schema_id(&self, schema_name: &str) -> Option<SchemaId> {
        self.catalog_infos
            .get(consts::DEFAULT_CATALOG)
            .unwrap()
            .get(schema_name)
            .map(|v| v.id)
    }
}
