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

//! A volatile catalog implementation used for storing information about table
//! and schema in memory.

use std::{
    collections::HashMap,
    string::ToString,
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use catalog::{
    self, consts,
    manager::{self, Manager},
    schema::{
        self, CatalogMismatch, CreateOptions, CreateTableRequest, CreateTableWithCause,
        DifferentTableId, DropOptions, DropTableRequest, DropTableWithCause, NameRef, Schema,
        SchemaMismatch, SchemaRef,
    },
    Catalog, CatalogRef, CreateSchemaWithCause,
};
use cluster::shard_set::ShardSet;
use common_types::schema::SchemaName;
use generic_error::BoxError;
use logger::{debug, info};
use meta_client::{types::AllocSchemaIdRequest, MetaClientRef};
use snafu::{ensure, OptionExt, ResultExt};
use table_engine::table::{SchemaId, TableRef};
use tokio::sync::Mutex;

/// ManagerImpl manages multiple volatile catalogs.
pub struct ManagerImpl {
    catalogs: HashMap<String, Arc<CatalogImpl>>,
    shard_set: ShardSet,
    meta_client: MetaClientRef,
}

impl ManagerImpl {
    pub fn new(shard_set: ShardSet, meta_client: MetaClientRef) -> Self {
        let mut manager = ManagerImpl {
            catalogs: HashMap::new(),
            shard_set,
            meta_client,
        };

        manager.maybe_create_default_catalog();

        manager
    }
}

impl Manager for ManagerImpl {
    fn default_catalog_name(&self) -> NameRef {
        consts::DEFAULT_CATALOG
    }

    fn default_schema_name(&self) -> NameRef {
        consts::DEFAULT_SCHEMA
    }

    fn catalog_by_name(&self, name: NameRef) -> manager::Result<Option<CatalogRef>> {
        let catalog = self.catalogs.get(name).map(|v| v.clone() as CatalogRef);
        Ok(catalog)
    }

    fn all_catalogs(&self) -> manager::Result<Vec<CatalogRef>> {
        Ok(self
            .catalogs
            .values()
            .map(|v| v.clone() as CatalogRef)
            .collect())
    }
}

impl ManagerImpl {
    fn maybe_create_default_catalog(&mut self) {
        // TODO: we should delegate this operation to the [TableManager].
        // Try to get default catalog, create it if not exists.
        if self.catalogs.get(consts::DEFAULT_CATALOG).is_none() {
            // Default catalog is not exists, create and store it.
            self.create_catalog(consts::DEFAULT_CATALOG.to_string());
        };
    }

    fn create_catalog(&mut self, catalog_name: String) -> Arc<CatalogImpl> {
        let catalog = Arc::new(CatalogImpl {
            name: catalog_name.clone(),
            schemas: RwLock::new(HashMap::new()),
            shard_set: self.shard_set.clone(),
            meta_client: self.meta_client.clone(),
        });

        self.catalogs.insert(catalog_name, catalog.clone());

        catalog
    }
}

/// A volatile implementation for [`Catalog`].
///
/// The schema and table id are allocated (and maybe stored) by other components
/// so there is no recovering work for all the schemas and tables during
/// initialization.
struct CatalogImpl {
    /// Catalog name
    name: String,
    /// All the schemas belonging to the catalog.
    schemas: RwLock<HashMap<SchemaName, SchemaRef>>,
    shard_set: ShardSet,
    meta_client: MetaClientRef,
}

#[async_trait]
impl Catalog for CatalogImpl {
    fn name(&self) -> NameRef {
        &self.name
    }

    fn schema_by_name(&self, name: NameRef) -> catalog::Result<Option<SchemaRef>> {
        let schema = self.schemas.read().unwrap().get(name).cloned();
        Ok(schema)
    }

    async fn create_schema<'a>(&'a self, name: NameRef<'a>) -> catalog::Result<()> {
        {
            let schemas = self.schemas.read().unwrap();

            if schemas.get(name).is_some() {
                return Ok(());
            }
        }

        let schema_id = {
            let req = AllocSchemaIdRequest {
                name: name.to_string(),
            };
            let resp = self
                .meta_client
                .alloc_schema_id(req)
                .await
                .box_err()
                .with_context(|| CreateSchemaWithCause {
                    catalog: &self.name,
                    schema: name.to_string(),
                })?;
            resp.id
        };

        let mut schemas = self.schemas.write().unwrap();
        if schemas.get(name).is_some() {
            return Ok(());
        }

        let schema: SchemaRef = Arc::new(SchemaImpl::new(
            self.name.to_string(),
            name.to_string(),
            SchemaId::from_u32(schema_id),
            self.shard_set.clone(),
        ));

        schemas.insert(name.to_string(), schema);

        info!(
            "create schema success, catalog:{}, schema:{}",
            &self.name, name
        );
        Ok(())
    }

    fn all_schemas(&self) -> catalog::Result<Vec<SchemaRef>> {
        Ok(self
            .schemas
            .read()
            .unwrap()
            .iter()
            .map(|(_, v)| v.clone())
            .collect())
    }
}

/// A volatile implementation for [`Schema`].
///
/// The implementation is actually a delegation for [`cluster::TableManager`].
struct SchemaImpl {
    /// Catalog name
    catalog_name: String,
    /// Schema name
    schema_name: String,
    schema_id: SchemaId,
    shard_set: ShardSet,
    /// Tables of schema
    tables: RwLock<HashMap<String, TableRef>>,
    /// Guard for creating/dropping table
    create_table_mutex: Mutex<()>,
}

impl SchemaImpl {
    fn new(
        catalog_name: String,
        schema_name: String,
        schema_id: SchemaId,
        shard_set: ShardSet,
    ) -> Self {
        Self {
            catalog_name,
            schema_name,
            schema_id,
            shard_set,
            tables: Default::default(),
            create_table_mutex: Mutex::new(()),
        }
    }

    fn get_table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> schema::Result<Option<TableRef>> {
        ensure!(
            self.catalog_name == catalog_name,
            CatalogMismatch {
                expect: &self.catalog_name,
                given: catalog_name,
            }
        );

        ensure!(
            self.schema_name == schema_name,
            SchemaMismatch {
                expect: &self.schema_name,
                given: schema_name,
            }
        );

        let tables = self.tables.read().unwrap();
        debug!(
            "Volatile schema impl gets table, table_name:{:?}, all_tables:{:?}",
            table_name, self.tables
        );
        Ok(tables.get(table_name).cloned())
    }

    fn add_table(&self, table: TableRef) -> Option<TableRef> {
        let mut tables = self.tables.write().unwrap();
        tables.insert(table.name().to_string(), table)
    }

    fn add_new_table(&self, table: TableRef) {
        let old = self.add_table(table);

        assert!(old.is_none());
    }

    fn remove_table(&self, table_name: &str) -> Option<TableRef> {
        let mut tables = self.tables.write().unwrap();
        tables.remove(table_name)
    }
}

#[async_trait]
impl Schema for SchemaImpl {
    fn name(&self) -> NameRef {
        &self.schema_name
    }

    fn id(&self) -> SchemaId {
        self.schema_id
    }

    fn table_by_name(&self, name: NameRef) -> schema::Result<Option<TableRef>> {
        let table = self.tables.read().unwrap().get(name).cloned();
        Ok(table)
    }

    // In memory schema does not support persisting table info
    async fn create_table(
        &self,
        request: CreateTableRequest,
        opts: CreateOptions,
    ) -> schema::Result<TableRef> {
        // FIXME: Error should be returned if create_if_not_exist is false.
        if let Some(table) = self.get_table(
            &request.params.catalog_name,
            &request.params.schema_name,
            &request.params.table_name,
        )? {
            if let Some(given_table_id) = request.table_id {
                let expected_table_id = table.id();
                ensure!(
                    expected_table_id == given_table_id,
                    DifferentTableId {
                        table_name: table.name().to_string(),
                        expected_table_id,
                        given_table_id,
                    }
                );
            }
            return Ok(table);
        }

        // Prepare to create table.
        let _create_table_guard = self.create_table_mutex.lock().await;

        if let Some(table) = self.get_table(
            &request.params.catalog_name,
            &request.params.schema_name,
            &request.params.table_name,
        )? {
            return Ok(table);
        }

        // Do real create table.
        // Partition table is not stored in ShardTableManager.
        if request.params.partition_info.is_none() {
            let _ = self
                .shard_set
                .get(request.shard_id)
                .with_context(|| schema::CreateTable {
                    request: request.clone(),
                    msg: "shard not found".to_string(),
                })?;
        }
        let request = request.into_engine_create_request(None, self.schema_id);

        // Table engine is able to handle duplicate table creation.
        let table = opts
            .table_engine
            .create_table(request)
            .await
            .box_err()
            .context(CreateTableWithCause)?;

        self.add_new_table(table.clone());

        Ok(table)
    }

    async fn drop_table(
        &self,
        request: DropTableRequest,
        opts: DropOptions,
    ) -> schema::Result<bool> {
        if self
            .get_table(
                &request.catalog_name,
                &request.schema_name,
                &request.table_name,
            )?
            .is_none()
        {
            return Ok(false);
        };

        // Prepare to drop table
        let _drop_table_guard = self.create_table_mutex.lock().await;

        let table = match self.get_table(
            &request.catalog_name,
            &request.schema_name,
            &request.table_name,
        )? {
            Some(v) => v,
            None => return Ok(false),
        };

        // Drop the table in the engine first.
        let request = request.into_engine_drop_request(self.schema_id);
        let real_dropped = opts
            .table_engine
            .drop_table(request)
            .await
            .box_err()
            .context(DropTableWithCause)?;

        // Remove the table from the memory.
        self.remove_table(table.name());
        Ok(real_dropped)
    }

    fn all_tables(&self) -> schema::Result<Vec<TableRef>> {
        Ok(self
            .tables
            .read()
            .unwrap()
            .iter()
            .map(|(_, v)| v.clone())
            .collect())
    }

    fn register_table(&self, table: TableRef) {
        self.add_table(table);
    }

    fn unregister_table(&self, table_name: &str) {
        let _ = self.remove_table(table_name);
    }
}
