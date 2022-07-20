// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! A memory catalog implementation
//!
//! Mainly for test

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
use log::info;
use snafu::{ensure, ResultExt};
use table_engine::table::{SchemaId, TableId, TableRef};
use tokio::sync::Mutex;

#[async_trait]
pub trait SchemaIdGenerator {
    type Error: std::error::Error + Send + Sync + 'static;
    async fn alloc_schema_id(
        &self,
        schema_name: NameRef,
    ) -> std::result::Result<SchemaId, Self::Error>;
}

#[async_trait]
pub trait TableIdGenerator {
    type Error: std::error::Error + Send + Sync + 'static;
    async fn alloc_table_id(
        &self,
        schema_id: SchemaId,
        table_name: NameRef,
    ) -> std::result::Result<TableId, Self::Error>;

    async fn invalidate_table_id(
        &self,
        schema_id: SchemaId,
        table_id: TableId,
    ) -> std::result::Result<(), Self::Error>;
}

struct ManagerImplInner {
    catalogs: HashMap<String, CatalogRef>,
}

/// In-memory catalog manager
#[derive(Clone)]
pub struct ManagerImpl {
    inner: Arc<ManagerImplInner>,
}

impl Manager for ManagerImpl {
    fn default_catalog_name(&self) -> NameRef {
        consts::DEFAULT_CATALOG
    }

    fn default_schema_name(&self) -> NameRef {
        consts::DEFAULT_SCHEMA
    }

    fn catalog_by_name(&self, name: NameRef) -> manager::Result<Option<CatalogRef>> {
        let catalog = self.inner.catalogs.get(name).cloned();
        Ok(catalog)
    }

    fn all_catalogs(&self) -> manager::Result<Vec<CatalogRef>> {
        Ok(self.inner.catalogs.iter().map(|(_, v)| v.clone()).collect())
    }
}

/// In-memory catalog
struct CatalogImpl<S, T> {
    /// Catalog name
    name: String,
    /// Schemas of catalog
    schemas: RwLock<HashMap<String, SchemaRef>>,
    /// Global schema id generator, Each schema has a unique schema id.
    schema_id_generator: Arc<S>,
    /// Global table id generator.
    table_id_generator: Arc<T>,
}

#[async_trait]
impl<S, T> Catalog for CatalogImpl<S, T>
where
    S: SchemaIdGenerator + Send + Sync,
    T: TableIdGenerator + Send + Sync + 'static,
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
            .schema_id_generator
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
            self.table_id_generator.clone(),
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

/// In-memory schema
struct SchemaImpl<T> {
    /// Catalog name
    catalog_name: String,
    /// Schema name
    schema_name: String,
    /// Tables of schema
    tables: RwLock<HashMap<String, TableRef>>,
    /// Guard for create/drop table
    create_table_mutex: Mutex<()>,
    schema_id: SchemaId,
    table_id_gen: Arc<T>,
}

impl<T> SchemaImpl<T> {
    fn new(
        catalog_name: String,
        schema_name: String,
        schema_id: SchemaId,
        table_id_gen: Arc<T>,
    ) -> Self {
        Self {
            catalog_name,
            schema_name,
            tables: RwLock::new(HashMap::new()),
            create_table_mutex: Mutex::new(()),
            schema_id,
            table_id_gen,
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
impl<T: TableIdGenerator + Send + Sync> Schema for SchemaImpl<T> {
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
            .table_id_gen
            .alloc_table_id(request.schema_id, &request.table_name)
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

        let schema_id = request.schema_id;
        let table_name = request.table_name.clone();

        // drop the table in the engine first.
        let real_dropped = opts
            .table_engine
            .drop_table(request)
            .await
            .context(DropTable)?;

        // invalidate the table id after table is dropped in engine.
        self.table_id_gen
            .invalidate_table_id(schema_id, table.id())
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
