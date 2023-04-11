// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Implements the TableEngine trait

use std::sync::Arc;

use async_trait::async_trait;
use common_util::error::BoxError;
use log::info;
use snafu::ResultExt;
use table_engine::{
    engine::{
        Close, CloseTableRequest, CreateTableRequest, DropTableRequest, OpenTableRequest, Result,
        TableEngine,
    },
    table::{SchemaId, TableRef},
    ANALYTIC_ENGINE_TYPE,
};

use crate::{instance::InstanceRef, space::SpaceId, table::TableImpl};

/// TableEngine implementation
pub struct TableEngineImpl {
    /// Instance of the table engine
    instance: InstanceRef,
}

impl Clone for TableEngineImpl {
    fn clone(&self) -> Self {
        Self {
            instance: self.instance.clone(),
        }
    }
}

impl TableEngineImpl {
    pub fn new(instance: InstanceRef) -> Self {
        Self { instance }
    }
}

impl Drop for TableEngineImpl {
    fn drop(&mut self) {
        info!("Table engine dropped");
    }
}

#[async_trait]
impl TableEngine for TableEngineImpl {
    fn engine_type(&self) -> &str {
        ANALYTIC_ENGINE_TYPE
    }

    async fn close(&self) -> Result<()> {
        info!("Try to close table engine");

        // Close the instance.
        self.instance.close().await.box_err().context(Close)?;

        info!("Table engine closed");

        Ok(())
    }

    async fn create_table(&self, request: CreateTableRequest) -> Result<TableRef> {
        let space_id = build_space_id(request.schema_id);

        info!(
            "Table engine impl create table, space_id:{}, request:{:?}",
            space_id, request
        );

        let space_table = self.instance.create_table(space_id, request).await?;

        let table_impl: TableRef = Arc::new(TableImpl::new(
            self.instance.clone(),
            ANALYTIC_ENGINE_TYPE.to_string(),
            space_id,
            space_table.table_data().id,
            space_table.table_data().clone(),
            space_table,
        ));

        Ok(table_impl)
    }

    async fn drop_table(&self, request: DropTableRequest) -> Result<bool> {
        let space_id = build_space_id(request.schema_id);

        info!(
            "Table engine impl drop table, space_id:{}, request:{:?}",
            space_id, request
        );

        let dropped = self.instance.drop_table(space_id, request).await?;
        Ok(dropped)
    }

    async fn open_table(&self, request: OpenTableRequest) -> Result<Option<TableRef>> {
        let space_id = build_space_id(request.schema_id);

        info!(
            "Table engine impl open table, space_id:{}, request:{:?}",
            space_id, request
        );
        let space_table = match self.instance.open_table(space_id, &request).await? {
            Some(v) => v,
            None => return Ok(None),
        };

        let table_impl = Arc::new(TableImpl::new(
            self.instance.clone(),
            ANALYTIC_ENGINE_TYPE.to_string(),
            space_id,
            space_table.table_data().id,
            space_table.table_data().clone(),
            space_table,
        ));

        Ok(Some(table_impl))
    }

    async fn close_table(&self, request: CloseTableRequest) -> Result<()> {
        let space_id = build_space_id(request.schema_id);

        info!(
            "Table engine impl close table, space_id:{}, request:{:?}",
            space_id, request,
        );

        self.instance.close_table(space_id, request).await?;

        Ok(())
    }
}

/// Generate the space id from the schema id with assumption schema id is unique
/// globally.
#[inline]
pub fn build_space_id(schema_id: SchemaId) -> SpaceId {
    schema_id.as_u32()
}
