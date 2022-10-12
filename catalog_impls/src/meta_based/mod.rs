// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

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
        self, CatalogMismatch, CloseOptions, CloseTableRequest, CreateOptions, CreateTableRequest,
        DropOptions, DropTableRequest, NameRef, OpenOptions, OpenTableRequest, Schema,
        SchemaMismatch, SchemaRef,
    },
    Catalog, CatalogRef, CreateSchemaWithCause,
};
use common_types::schema::SchemaName;
use log::info;
use meta_client::{types::AllocSchemaIdRequest, MetaClientRef};
use snafu::{ensure, ResultExt};
use table_engine::table::{SchemaId, TableRef};
use table_manager::TableManager;

pub mod table_manager;

pub struct ManagerImpl {
    catalogs: HashMap<String, Arc<CatalogImpl>>,
    table_manager: TableManager,
    meta_client: MetaClientRef,
}

impl ManagerImpl {
    pub async fn new(table_manager: TableManager, meta_client: MetaClientRef) -> Self {
        let mut manager = ManagerImpl {
            catalogs: HashMap::new(),
            table_manager,
            meta_client,
        };

        manager.maybe_create_default_catalog().await;

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
            .iter()
            .map(|(_, v)| v.clone() as CatalogRef)
            .collect())
    }
}

impl ManagerImpl {
    async fn maybe_create_default_catalog(&mut self) {
        // TODO: we should delegate this operation to the [TableManager].
        // Try to get default catalog, create it if not exists.
        if self.catalogs.get(consts::DEFAULT_CATALOG).is_none() {
            // Default catalog is not exists, create and store it.
            self.create_catalog(consts::DEFAULT_CATALOG.to_string())
                .await;
        };
    }

    async fn create_catalog(&mut self, catalog_name: String) -> Arc<CatalogImpl> {
        let catalog = Arc::new(CatalogImpl {
            name: catalog_name.clone(),
            schemas: RwLock::new(HashMap::new()),
            table_manager: self.table_manager.clone(),
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
    table_manager: TableManager,
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

        let schema_id = match self.table_manager.get_schema_id(&self.name, name) {
            Some(v) => v,
            None => {
                let req = AllocSchemaIdRequest {
                    name: name.to_string(),
                };

                let resp = self
                    .meta_client
                    .alloc_schema_id(req)
                    .await
                    .map_err(|e| Box::new(e) as _)
                    .with_context(|| CreateSchemaWithCause {
                        catalog: self.name.to_string(),
                        schema: name.to_string(),
                    })?;

                info!(
                    "Succeed to allocate schema id({}) for schema({})",
                    resp.id, name
                );

                resp.id
            }
        };

        // TODO: add the new schema to table_manager.

        let mut schemas = self.schemas.write().unwrap();
        if schemas.get(name).is_some() {
            return Ok(());
        }

        let schema: SchemaRef = Arc::new(SchemaImpl::new(
            self.name.to_string(),
            name.to_string(),
            SchemaId::from_u32(schema_id),
            self.table_manager.clone(),
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

/// An implementation based [`TableManager`] for [`Schema`].
struct SchemaImpl {
    /// Catalog name
    catalog_name: String,
    /// Schema name
    schema_name: String,
    schema_id: SchemaId,
    table_manager: TableManager,
}

impl SchemaImpl {
    fn new(
        catalog_name: String,
        schema_name: String,
        schema_id: SchemaId,
        table_manager: TableManager,
    ) -> Self {
        Self {
            catalog_name,
            schema_name,
            schema_id,
            table_manager,
        }
    }

    fn get_table_with_check(
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
        Ok(self
            .table_manager
            .table_by_name(catalog_name, schema_name, table_name))
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
        let table = self
            .table_manager
            .table_by_name(&self.catalog_name, &self.schema_name, name);
        Ok(table)
    }

    // In memory schema does not support persisting table info
    async fn create_table(
        &self,
        request: CreateTableRequest,
        _opts: CreateOptions,
    ) -> schema::Result<TableRef> {
        // FIXME: Error should be returned if create_if_not_exist is false.
        if let Some(table) = self.get_table_with_check(
            &request.catalog_name,
            &request.schema_name,
            &request.table_name,
        )? {
            return Ok(table);
        }

        todo!()
    }

    async fn drop_table(
        &self,
        request: DropTableRequest,
        _opts: DropOptions,
    ) -> schema::Result<bool> {
        let _table = self.get_table_with_check(
            &request.catalog_name,
            &request.schema_name,
            &request.table_name,
        )?;

        todo!();
    }

    async fn open_table(
        &self,
        request: OpenTableRequest,
        _opts: OpenOptions,
    ) -> schema::Result<Option<TableRef>> {
        let _table = self.get_table_with_check(
            &request.catalog_name,
            &request.schema_name,
            &request.table_name,
        )?;

        todo!()
    }

    async fn close_table(
        &self,
        request: CloseTableRequest,
        _opts: CloseOptions,
    ) -> schema::Result<()> {
        let _table = self.get_table_with_check(
            &request.catalog_name,
            &request.schema_name,
            &request.table_name,
        )?;

        todo!()
    }

    fn all_tables(&self) -> schema::Result<Vec<TableRef>> {
        Ok(self
            .table_manager
            .tables_by_schema(&self.catalog_name, &self.schema_name))
    }
}
