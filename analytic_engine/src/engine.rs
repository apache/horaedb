// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Implements the TableEngine trait

use std::sync::Arc;

use async_trait::async_trait;
use log::info;
use object_store::ObjectStore;
use snafu::ResultExt;
use table_engine::{
    engine::{Close, CreateTableRequest, DropTableRequest, OpenTableRequest, Result, TableEngine},
    table::TableRef,
    ANALYTIC_ENGINE_TYPE,
};
use wal::manager::WalManager;

use crate::{
    context::CommonContext, instance::InstanceRef, meta::Manifest, space::SpaceName,
    sst::factory::Factory, table::TableImpl,
};

/// TableEngine implementation
pub struct TableEngineImpl<Wal, Meta, Store, Fa> {
    /// Instance of the table engine
    instance: InstanceRef<Wal, Meta, Store, Fa>,
}

impl<Wal, Meta, Store, Fa> Clone for TableEngineImpl<Wal, Meta, Store, Fa> {
    fn clone(&self) -> Self {
        Self {
            instance: self.instance.clone(),
        }
    }
}

impl<
        Wal: WalManager + Send + Sync + 'static,
        Meta: Manifest + Send + Sync + 'static,
        Store: ObjectStore,
        Fa,
    > TableEngineImpl<Wal, Meta, Store, Fa>
{
    pub fn new(instance: InstanceRef<Wal, Meta, Store, Fa>) -> Self {
        Self { instance }
    }
}

impl<Wal, Meta, Store, Fa> TableEngineImpl<Wal, Meta, Store, Fa> {
    pub fn instance(&self) -> InstanceRef<Wal, Meta, Store, Fa> {
        self.instance.clone()
    }
}

impl<Wal, Meta, Store, Fa> Drop for TableEngineImpl<Wal, Meta, Store, Fa> {
    fn drop(&mut self) {
        info!("Table engine dropped");
    }
}

#[async_trait]
impl<
        Wal: WalManager + Send + Sync + 'static,
        Meta: Manifest + Send + Sync + 'static,
        Store: ObjectStore,
        Fa: Factory + Send + Sync + 'static,
    > TableEngine for TableEngineImpl<Wal, Meta, Store, Fa>
{
    fn engine_type(&self) -> &str {
        ANALYTIC_ENGINE_TYPE
    }

    async fn close(&self) -> Result<()> {
        info!("Try to close table engine");

        // Close the instance.
        self.instance
            .close()
            .await
            .map_err(|e| Box::new(e) as _)
            .context(Close)?;

        info!("Table engine closed");

        Ok(())
    }

    async fn create_table(&self, request: CreateTableRequest) -> Result<TableRef> {
        let space = build_space_name(&request.catalog_name, &request.schema_name);

        info!(
            "Table engine impl create table, space:{}, request:{:?}",
            space, request
        );

        let ctx = CommonContext {
            db_write_buffer_size: self.instance.db_write_buffer_size,
            space_write_buffer_size: self.instance.space_write_buffer_size,
        };
        let space_table = self.instance.create_table(&ctx, &space, request).await?;

        let table_impl = Arc::new(TableImpl::new(
            space_table,
            self.instance.clone(),
            ANALYTIC_ENGINE_TYPE.to_string(),
        ));

        Ok(table_impl)
    }

    async fn drop_table(&self, request: DropTableRequest) -> Result<bool> {
        let space = build_space_name(&request.catalog_name, &request.schema_name);

        info!(
            "Table engine impl drop table, space:{}, request:{:?}",
            space, request
        );

        let ctx = CommonContext {
            db_write_buffer_size: self.instance.db_write_buffer_size,
            space_write_buffer_size: self.instance.space_write_buffer_size,
        };
        let dropped = self.instance.drop_table(&ctx, &space, request).await?;
        Ok(dropped)
    }

    async fn open_table(&self, request: OpenTableRequest) -> Result<Option<TableRef>> {
        let space = build_space_name(&request.catalog_name, &request.schema_name);

        info!(
            "Table engine impl open table, space:{}, request:{:?}",
            space, request
        );
        let ctx = CommonContext {
            db_write_buffer_size: self.instance.db_write_buffer_size,
            space_write_buffer_size: self.instance.space_write_buffer_size,
        };
        let space_table = match self
            .instance
            .find_table(&ctx, &space, &request.table_name)?
        {
            Some(v) => v,
            None => return Ok(None),
        };

        let table_impl = Arc::new(TableImpl::new(
            space_table,
            self.instance.clone(),
            ANALYTIC_ENGINE_TYPE.to_string(),
        ));

        Ok(Some(table_impl))
    }
}

/// Build the space name from catalog and schema
// TODO(yingwen): Should we store the <catalog, schema> => space mapping in the
// system catalog, then put it in the CreateTableRequest, avoid generating space
// name here
fn build_space_name(catalog: &str, schema: &str) -> SpaceName {
    // FIXME(yingwen): Find out a better way to create space name
    format!("{}/{}", catalog, schema)
}
