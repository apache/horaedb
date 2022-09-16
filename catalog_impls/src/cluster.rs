use std::sync::Arc;

use async_trait::async_trait;
use catalog::{
    consts,
    manager::Manager,
    schema::{
        self, CloseOptions, CloseTableRequest, CreateOptions, CreateTableRequest, DropOptions,
        DropTableRequest, NameRef, OpenOptions, OpenTableRequest, Schema, SchemaRef,
    },
    Catalog, CatalogRef,
};
use cluster::{cluster_impl::ClusterImpl, table_manager::TableManager};
use table_engine::table::{SchemaId, TableRef};

pub struct ManagerImpl(ClusterImpl);

impl Manager for ManagerImpl {
    fn default_catalog_name(&self) -> NameRef {
        consts::DEFAULT_CATALOG
    }

    fn default_schema_name(&self) -> NameRef {
        consts::DEFAULT_SCHEMA
    }

    fn catalog_by_name(&self, name: NameRef) -> catalog::manager::Result<Option<CatalogRef>> {
        let catalog = self.0.table_manager().get_catalog_name(name).map(|name| {
            Arc::new(CatalogImpl {
                name,
                table_manager: self.0.table_manager().clone(),
            }) as _
        });

        Ok(catalog)
    }

    fn all_catalogs(&self) -> catalog::manager::Result<Vec<CatalogRef>> {
        let catalogs = self
            .0
            .table_manager()
            .get_all_catalog_names()
            .into_iter()
            .map(|name| {
                Arc::new(CatalogImpl {
                    name,
                    table_manager: self.0.table_manager().clone(),
                }) as _
            })
            .collect();

        Ok(catalogs)
    }
}

pub struct CatalogImpl {
    /// Catalog name
    name: String,
    table_manager: TableManager,
}

#[async_trait]
impl Catalog for CatalogImpl {
    /// Get the catalog name
    fn name(&self) -> NameRef {
        &self.name
    }

    /// Find schema by name
    fn schema_by_name(&self, name: NameRef) -> catalog::Result<Option<SchemaRef>> {
        let schema = self
            .table_manager
            .get_schema_id(&self.name, name)
            .map(|id| {
                Arc::new(SchemaImpl {
                    catalog_name: self.name.clone(),
                    schema_name: name.to_string(),
                    id: id.into(),
                    table_manager: self.table_manager.clone(),
                }) as _
            });

        Ok(schema)
    }

    #[allow(unused_variables)]
    async fn create_schema<'a>(&'a self, name: NameRef<'a>) -> catalog::Result<()> {
        todo!()
    }

    /// All schemas
    fn all_schemas(&self) -> catalog::Result<Vec<SchemaRef>> {
        let schemas = self
            .table_manager
            .get_all_schema_infos(&self.name)
            .into_iter()
            .map(|info| {
                Arc::new(SchemaImpl {
                    catalog_name: self.name.clone(),
                    schema_name: info.name,
                    id: info.id.into(),
                    table_manager: self.table_manager.clone(),
                }) as _
            })
            .collect();

        Ok(schemas)
    }
}

pub struct SchemaImpl {
    /// Catalog name
    catalog_name: String,
    /// Schema name
    schema_name: String,
    /// Schema id
    id: SchemaId,
    table_manager: TableManager,
}

#[async_trait]
impl Schema for SchemaImpl {
    /// Get schema name.
    fn name(&self) -> NameRef {
        &self.schema_name
    }

    /// Get schema id
    fn id(&self) -> SchemaId {
        self.id
    }

    /// Find table by name.
    fn table_by_name(&self, name: NameRef) -> schema::Result<Option<TableRef>> {
        let table = self
            .table_manager
            .table_by_name(&self.catalog_name, &self.schema_name, name);

        Ok(table)
    }

    /// Create table according to `request`.
    #[allow(unused_variables)]
    async fn create_table(
        &self,
        request: CreateTableRequest,
        opts: CreateOptions,
    ) -> schema::Result<TableRef> {
        todo!()
    }

    /// Drop table according to `request`.
    ///
    /// Returns true if the table is really dropped.
    #[allow(unused_variables)]
    async fn drop_table(
        &self,
        request: DropTableRequest,
        opts: DropOptions,
    ) -> schema::Result<bool> {
        todo!()
    }

    /// Open the table according to `request`.
    ///
    /// Return None if table does not exist.
    #[allow(unused_variables)]
    async fn open_table(
        &self,
        request: OpenTableRequest,
        opts: OpenOptions,
    ) -> schema::Result<Option<TableRef>> {
        todo!()
    }

    /// Close the table according to `request`.
    ///
    /// Return false if table does not exist.
    #[allow(unused_variables)]
    async fn close_table(
        &self,
        request: CloseTableRequest,
        opts: CloseOptions,
    ) -> schema::Result<()> {
        todo!()
    }

    /// All tables
    fn all_tables(&self) -> schema::Result<Vec<TableRef>> {
        let tables = self
            .table_manager
            .get_all_table_ref(&self.catalog_name, &self.schema_name);

        Ok(tables)
    }
}
