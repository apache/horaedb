// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! A memory catalog implementation
//!
//! Mainly for test

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use catalog::{
    self, consts,
    manager::{self, Manager},
    schema::{
        self, CatalogMismatch, CreateOptions, CreateTable, DropOptions, NameRef, Schema,
        SchemaMismatch, SchemaRef, TooManyTable, UnSupported,
    },
    Catalog, CatalogRef,
};
use log::info;
use snafu::{ensure, OptionExt, ResultExt};
use table_engine::{
    engine::{CreateTableRequest, DropTableRequest},
    table::{SchemaId, SchemaIdGenerator, TableId, TableRef, TableSeqGenerator},
};

struct ManagerImplInner {
    catalogs: HashMap<String, CatalogRef>,
}

/// In-memory catalog manager
#[derive(Clone)]
pub struct ManagerImpl {
    inner: Arc<ManagerImplInner>,
}

impl Default for ManagerImpl {
    fn default() -> Self {
        let schema_id_generator = SchemaIdGenerator::default();
        let schema_id = schema_id_generator.alloc_schema_id().unwrap();

        // Register default schema
        let default_schema: SchemaRef = Arc::new(SchemaImpl::new(
            consts::DEFAULT_CATALOG.to_string(),
            consts::DEFAULT_SCHEMA.to_string(),
            schema_id,
        ));
        let mut schemas = HashMap::new();
        schemas.insert(consts::DEFAULT_SCHEMA.to_string(), default_schema);

        // Use above schemas to create a default catalog
        let default_catalog: CatalogRef = Arc::new(CatalogImpl {
            name: consts::DEFAULT_CATALOG.to_string(),
            schemas: RwLock::new(schemas),
            schema_id_generator: Arc::new(schema_id_generator),
        });
        // Register default catalog
        let mut catalogs = HashMap::new();
        catalogs.insert(consts::DEFAULT_CATALOG.to_string(), default_catalog);

        Self {
            inner: Arc::new(ManagerImplInner { catalogs }),
        }
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
        let catalog = self.inner.catalogs.get(name).cloned();
        Ok(catalog)
    }

    fn all_catalogs(&self) -> manager::Result<Vec<CatalogRef>> {
        Ok(self.inner.catalogs.iter().map(|(_, v)| v.clone()).collect())
    }
}

/// In-memory catalog
struct CatalogImpl {
    /// Catalog name
    name: String,
    /// Schemas of catalog
    schemas: RwLock<HashMap<String, SchemaRef>>,
    /// Global schema id generator, Each schema has a unique schema id.
    schema_id_generator: Arc<SchemaIdGenerator>,
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
        let mut schemas = self.schemas.write().unwrap();

        if schemas.get(name).is_some() {
            return Ok(());
        }

        let schema_id = self.schema_id_generator.alloc_schema_id().unwrap();

        let schema: SchemaRef = Arc::new(SchemaImpl::new(
            self.name.to_string(),
            name.to_string(),
            schema_id,
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
struct SchemaImpl {
    /// Catalog name
    catalog_name: String,
    /// Schema name
    schema_name: String,
    /// Tables of schema
    tables: RwLock<HashMap<String, TableRef>>,
    schema_id: SchemaId,
    table_seq_generator: TableSeqGenerator,
}

impl SchemaImpl {
    fn new(catalog_name: String, schema_name: String, schema_id: SchemaId) -> Self {
        Self {
            catalog_name,
            schema_name,
            tables: RwLock::new(HashMap::new()),
            schema_id,
            table_seq_generator: TableSeqGenerator::default(),
        }
    }
}

#[async_trait]
impl Schema for SchemaImpl {
    fn name(&self) -> NameRef {
        &self.schema_name
    }

    fn table_by_name(&self, name: NameRef) -> schema::Result<Option<TableRef>> {
        let table = self.tables.read().unwrap().get(name).cloned();
        Ok(table)
    }

    fn alloc_table_id(&self, name: NameRef) -> schema::Result<TableId> {
        let table_seq = self
            .table_seq_generator
            .alloc_table_seq()
            .context(TooManyTable {
                schema: &self.schema_name,
                table: name,
            })?;

        Ok(TableId::new(self.schema_id, table_seq))
    }

    // In memory schema does not support persisting table info
    async fn create_table(
        &self,
        request: CreateTableRequest,
        opts: CreateOptions,
    ) -> schema::Result<TableRef> {
        ensure!(
            self.catalog_name == request.catalog_name,
            CatalogMismatch {
                expect: &self.catalog_name,
                given: request.catalog_name,
            }
        );
        ensure!(
            self.schema_name == request.schema_name,
            SchemaMismatch {
                expect: &self.schema_name,
                given: request.schema_name,
            }
        );

        {
            // Check table existence
            let tables = self.tables.read().unwrap();
            if let Some(table) = tables.get(&request.table_name) {
                return Ok(table.clone());
            }
        }

        // Table engine handles duplicate table creation
        let table_name = request.table_name.clone();
        let table = opts
            .table_engine
            .create_table(request)
            .await
            .context(CreateTable)?;

        {
            // Now the table engine have create the table, but we may not be the
            // creator thread
            let mut tables = self.tables.write().unwrap();
            tables.entry(table_name).or_insert_with(|| table.clone());
        }

        Ok(table)
    }

    async fn drop_table(
        &self,
        request: DropTableRequest,
        _opts: DropOptions,
    ) -> schema::Result<bool> {
        UnSupported {
            msg: format!(
                "Dropping table is not supported by memory catalog, request:{:?}",
                request
            ),
        }
        .fail()
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
