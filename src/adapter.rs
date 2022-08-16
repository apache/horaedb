use async_trait::async_trait;
use catalog::{
    manager::ManagerRef as CatalogManagerRef,
    schema::{CloseOptions, CloseTableRequest, NameRef, OpenOptions, OpenTableRequest, SchemaRef},
};
use catalog_impls::{SchemaIdAlloc, TableIdAlloc};
use cluster::TableManipulator;
use log::debug;
use meta_client_v2::{types::DropTableRequest, MetaClientRef};
use table_engine::{
    engine::TableEngineRef,
    table::{SchemaId, TableId},
    ANALYTIC_ENGINE_TYPE,
};

pub struct SchemaIdAllocAdapter(pub MetaClientRef);

#[async_trait]
impl SchemaIdAlloc for SchemaIdAllocAdapter {
    type Error = meta_client_v2::Error;

    async fn alloc_schema_id<'a>(&self, schema_name: NameRef<'a>) -> Result<SchemaId, Self::Error> {
        self.0
            .alloc_schema_id(cluster::AllocSchemaIdRequest {
                name: schema_name.to_string(),
            })
            .await
            .map(|resp| SchemaId::from(resp.id))
    }
}
pub struct TableIdAllocAdapter(pub MetaClientRef);

#[async_trait]
impl TableIdAlloc for TableIdAllocAdapter {
    type Error = meta_client_v2::Error;

    async fn alloc_table_id<'a>(
        &self,
        schema_name: NameRef<'a>,
        table_name: NameRef<'a>,
    ) -> Result<TableId, Self::Error> {
        self.0
            .alloc_table_id(cluster::AllocTableIdRequest {
                schema_name: schema_name.to_string(),
                name: table_name.to_string(),
            })
            .await
            .map(|v| TableId::from(v.id))
    }

    async fn invalidate_table_id<'a>(
        &self,
        schema_name: NameRef<'a>,
        table_name: NameRef<'a>,
        table_id: TableId,
    ) -> Result<(), Self::Error> {
        self.0
            .drop_table(DropTableRequest {
                schema_name: schema_name.to_string(),
                name: table_name.to_string(),
                id: table_id.as_u64(),
            })
            .await
    }
}

pub struct TableManipulatorImpl {
    pub catalog_manager: CatalogManagerRef,
    pub table_engine: TableEngineRef,
}

impl TableManipulatorImpl {
    fn catalog_schema_by_name(
        &self,
        schema_name: &str,
    ) -> Result<(NameRef, Option<SchemaRef>), Box<dyn std::error::Error + Send + Sync>> {
        let default_catalog_name = self.catalog_manager.default_catalog_name();
        let default_catalog = self
            .catalog_manager
            .catalog_by_name(default_catalog_name)
            .map_err(Box::new)?
            .unwrap();
        let schema = default_catalog
            .schema_by_name(schema_name)
            .map_err(Box::new)?;
        Ok((default_catalog_name, schema))
    }
}
#[async_trait]
impl TableManipulator for TableManipulatorImpl {
    async fn open_table(
        &self,
        schema_name: &str,
        table_name: &str,
        table_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (default_catalog_name, schema) = self.catalog_schema_by_name(schema_name)?;
        let schema = schema.unwrap();
        let table_id = TableId::from(table_id);
        let req = OpenTableRequest {
            catalog_name: default_catalog_name.to_string(),
            schema_name: schema_name.to_string(),
            schema_id: schema.id(),
            table_name: table_name.to_string(),
            table_id,
            engine: ANALYTIC_ENGINE_TYPE.to_string(),
        };
        let opts = OpenOptions {
            table_engine: self.table_engine.clone(),
        };
        let table = schema.open_table(req, opts).await.map_err(Box::new)?;
        debug!(
            "Finish opening table:{}-{}, catalog:{}, schema:{}, really_opened:{}",
            table_name,
            table_id,
            default_catalog_name,
            schema_name,
            table.is_some()
        );
        Ok(())
    }

    async fn close_table(
        &self,
        schema_name: &str,
        table_name: &str,
        table_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (default_catalog_name, schema) = self.catalog_schema_by_name(schema_name)?;
        let schema = schema.unwrap();
        let table_id = TableId::from(table_id);
        let req = CloseTableRequest {
            catalog_name: default_catalog_name.to_string(),
            schema_name: schema_name.to_string(),
            schema_id: schema.id(),
            table_name: table_name.to_string(),
            table_id,
            engine: ANALYTIC_ENGINE_TYPE.to_string(),
        };
        let opts = CloseOptions {
            table_engine: self.table_engine.clone(),
        };
        schema.close_table(req, opts).await.map_err(Box::new)?;
        debug!(
            "Finish closing table:{}-{}, catalog:{}, schema:{}",
            table_name, table_id, default_catalog_name, schema_name
        );
        Ok(())
    }
}
