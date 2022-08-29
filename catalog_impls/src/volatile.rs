// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

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
        self, CatalogMismatch, CloseOptions, CloseTable, CloseTableRequest, CreateOptions,
        CreateTable, CreateTableRequest, DropOptions, DropTable, DropTableRequest, NameRef,
        OpenOptions, OpenTable, OpenTableRequest, Schema, SchemaMismatch, SchemaRef,
    },
    Catalog, CatalogRef,
};
use common_types::schema::SchemaName;
use log::{debug, info};
use snafu::{ensure, ResultExt};
use table_engine::table::{SchemaId, TableRef};
use tokio::sync::Mutex;

use crate::{SchemaIdAlloc, TableIdAlloc};

/// ManagerImpl manages multiple volatile catalogs.
pub struct ManagerImpl<S, T> {
    catalogs: HashMap<String, Arc<CatalogImpl<S, T>>>,
    schema_id_alloc: Arc<S>,
    table_id_alloc: Arc<T>,
}

impl<S, T> ManagerImpl<S, T>
where
    S: SchemaIdAlloc + 'static,
    T: TableIdAlloc + 'static,
{
    pub async fn new(schema_id_alloc: S, table_id_alloc: T) -> Self {
        let mut manager = ManagerImpl {
            catalogs: HashMap::new(),
            table_id_alloc: Arc::new(table_id_alloc),
            schema_id_alloc: Arc::new(schema_id_alloc),
        };

        manager.maybe_create_default_catalog().await;

        manager
    }
}

impl<S, T> Manager for ManagerImpl<S, T>
where
    S: SchemaIdAlloc + 'static,
    T: TableIdAlloc + 'static,
{
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
            .iter()
            .map(|(_, v)| v.clone() as CatalogRef)
            .collect())
    }
}

impl<S, T> ManagerImpl<S, T>
where
    S: SchemaIdAlloc,
    T: TableIdAlloc + 'static,
{
    async fn maybe_create_default_catalog(&mut self) {
        // Try to get default catalog, create it if not exists.
        if self.catalogs.get(consts::DEFAULT_CATALOG).is_none() {
            // Default catalog is not exists, create and store it.
            self.create_catalog(consts::DEFAULT_CATALOG.to_string())
                .await;
        };
    }

    async fn create_catalog(&mut self, catalog_name: String) -> Arc<CatalogImpl<S, T>> {
        let catalog = Arc::new(CatalogImpl {
            name: catalog_name.clone(),
            schemas: RwLock::new(HashMap::new()),
            schema_id_alloc: self.schema_id_alloc.clone(),
            table_id_alloc: self.table_id_alloc.clone(),
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
struct CatalogImpl<S, T> {
    /// Catalog name
    name: String,
    /// All the schemas belonging to the catalog.
    schemas: RwLock<HashMap<SchemaName, SchemaRef>>,
    schema_id_alloc: Arc<S>,
    table_id_alloc: Arc<T>,
}

#[async_trait]
impl<S, T> Catalog for CatalogImpl<S, T>
where
    S: SchemaIdAlloc,
    T: TableIdAlloc + 'static,
{
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

        let schema_id = self
            .schema_id_alloc
            .alloc_schema_id(name)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(catalog::CreateSchema {
                catalog: &self.name,
                schema: name,
            })?;

        let mut schemas = self.schemas.write().unwrap();
        if schemas.get(name).is_some() {
            return Ok(());
        }

        let schema: SchemaRef = Arc::new(SchemaImpl::new(
            self.name.to_string(),
            name.to_string(),
            schema_id,
            self.table_id_alloc.clone(),
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
/// The tables belonging to the schema won't be recovered during initialization
/// and will be opened afterwards.
struct SchemaImpl<T> {
    /// Catalog name
    catalog_name: String,
    /// Schema name
    schema_name: String,
    /// Tables of schema
    tables: RwLock<HashMap<String, TableRef>>,
    /// Guard for creating/dropping table
    create_table_mutex: Mutex<()>,
    schema_id: SchemaId,
    table_id_alloc: Arc<T>,
}

impl<T> SchemaImpl<T> {
    fn new(
        catalog_name: String,
        schema_name: String,
        schema_id: SchemaId,
        table_id_alloc: Arc<T>,
    ) -> Self {
        Self {
            catalog_name,
            schema_name,
            tables: RwLock::new(HashMap::new()),
            create_table_mutex: Mutex::new(()),
            schema_id,
            table_id_alloc,
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

        // Check table existence
        let tables = self.tables.read().unwrap();
        debug!(
            "Memory catalog impl, get table, table_name:{:?}, tables:{:?}",
            table_name, self.tables
        );
        Ok(tables.get(table_name).cloned())
    }

    fn add_table(&self, table: TableRef) {
        let mut tables = self.tables.write().unwrap();
        let old = tables.insert(table.name().to_string(), table);
        assert!(old.is_none());
    }

    fn remove_table(&self, table_name: &str) -> Option<TableRef> {
        let mut tables = self.tables.write().unwrap();
        tables.remove(table_name)
    }
}

#[async_trait]
impl<T: TableIdAlloc> Schema for SchemaImpl<T> {
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
            &request.catalog_name,
            &request.schema_name,
            &request.table_name,
        )? {
            return Ok(table);
        }

        // prepare to create table
        let _create_table_guard = self.create_table_mutex.lock().await;

        if let Some(table) = self.get_table(
            &request.catalog_name,
            &request.schema_name,
            &request.table_name,
        )? {
            return Ok(table);
        }

        let table_id = self
            .table_id_alloc
            .alloc_table_id(&request.schema_name, &request.table_name)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(schema::AllocateTableId {
                schema: &self.schema_name,
                table: &request.table_name,
            })?;

        let request = request.into_engine_create_request(table_id);

        // Table engine handles duplicate table creation
        let table = opts
            .table_engine
            .create_table(request)
            .await
            .context(CreateTable)?;

        self.add_table(table.clone());

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

        // prepare to drop table
        let _drop_table_guard = self.create_table_mutex.lock().await;

        let table = match self.get_table(
            &request.catalog_name,
            &request.schema_name,
            &request.table_name,
        )? {
            Some(v) => v,
            None => return Ok(false),
        };

        let schema_name = request.schema_name.clone();
        let table_name = request.table_name.clone();

        // drop the table in the engine first.
        let real_dropped = opts
            .table_engine
            .drop_table(request)
            .await
            .context(DropTable)?;

        // invalidate the table id after table is dropped in engine.
        self.table_id_alloc
            .invalidate_table_id(&schema_name, &table_name, table.id())
            .await
            .map_err(|e| Box::new(e) as _)
            .context(schema::InvalidateTableId {
                schema: &self.schema_name,
                table_name,
                table_id: table.id(),
            })?;

        // remove the table from the catalog memory.
        self.remove_table(table.name());
        Ok(real_dropped)
    }

    async fn open_table(
        &self,
        request: OpenTableRequest,
        opts: OpenOptions,
    ) -> schema::Result<Option<TableRef>> {
        let table = self.get_table(
            &request.catalog_name,
            &request.schema_name,
            &request.table_name,
        )?;
        if table.is_some() {
            return Ok(table);
        }

        // Table engine handles duplicate table creation
        let table_name = request.table_name.clone();
        let table = opts
            .table_engine
            .open_table(request)
            .await
            .context(OpenTable)?;

        if let Some(table) = &table {
            // Now the table engine have create the table, but we may not be the
            // creator thread
            let mut tables = self.tables.write().unwrap();
            tables.entry(table_name).or_insert_with(|| table.clone());
        }

        Ok(table)
    }

    async fn close_table(
        &self,
        request: CloseTableRequest,
        opts: CloseOptions,
    ) -> schema::Result<()> {
        if self
            .get_table(
                &request.catalog_name,
                &request.schema_name,
                &request.table_name,
            )?
            .is_none()
        {
            return Ok(());
        }

        let table_name = request.table_name.clone();
        opts.table_engine
            .close_table(request)
            .await
            .context(CloseTable)?;

        self.remove_table(&table_name);

        Ok(())
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
}
