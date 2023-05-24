// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Implements the TableEngine trait

use std::sync::Arc;

use async_trait::async_trait;
use common_util::error::BoxError;
use log::{error, info, warn};
use snafu::ResultExt;
use table_engine::{
    engine::{
        Close, CloseShardRequest, CloseTableRequest, CreateTableRequest, DropTableRequest,
        OpenShardRequest, OpenTableRequest, Result, TableEngine,
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

    async fn open_tables_of_shard(
        &self,
        open_requests: Vec<table_engine::engine::OpenTableRequest>,
    ) -> Vec<table_engine::engine::Result<Option<TableRef>>> {
        if open_requests.is_empty() {
            return Vec::new();
        }

        let mut open_results = Vec::with_capacity(open_requests.len());
        for request in open_requests {
            let result = self
                .open_table(request.clone())
                .await
                .map_err(|e| {
                    error!("Failed to open table, open_request:{request:?}, err:{e}");
                    e
                })
                .map(|table_opt| {
                    if table_opt.is_none() {
                        error!(
                            "Table engine returns none when opening table, open_request:{request:?}"
                        );
                    }
                    table_opt
                });

            if let Ok(None) = result {
                warn!("Try to open a missing table, open request:{request:?}");
            }

            open_results.push(result);
        }

        open_results
    }

    async fn close_tables_of_shard(
        &self,
        close_requests: Vec<table_engine::engine::CloseTableRequest>,
    ) -> Vec<table_engine::engine::Result<String>> {
        if close_requests.is_empty() {
            return Vec::new();
        }

        let mut close_results = Vec::with_capacity(close_requests.len());
        for request in close_requests {
            let result = self
                .close_table(request.clone())
                .await
                .map_err(|e| {
                    error!("Failed to close table, close_request:{request:?}, err:{e}");
                    e
                })
                .map(|_| request.table_name);

            close_results.push(result);
        }

        close_results
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

    async fn open_shard(&self, request: OpenShardRequest) -> Vec<Result<Option<TableRef>>> {
        let table_defs = request.table_defs;
        let open_requests = table_defs
            .into_iter()
            .map(|def| OpenTableRequest {
                catalog_name: def.catalog_name,
                schema_name: def.schema_name,
                schema_id: def.schema_id,
                table_name: def.name,
                table_id: def.id,
                engine: request.engine.clone(),
                shard_id: request.shard_id,
            })
            .collect();

        self.open_tables_of_shard(open_requests).await
    }

    async fn close_shard(
        &self,
        request: CloseShardRequest,
    ) -> Vec<table_engine::engine::Result<String>> {
        let table_defs = request.table_defs;
        let close_requests = table_defs
            .into_iter()
            .map(|def| CloseTableRequest {
                catalog_name: def.catalog_name,
                schema_name: def.schema_name,
                schema_id: def.schema_id,
                table_name: def.name,
                table_id: def.id,
                engine: request.engine.clone(),
            })
            .collect();

        self.close_tables_of_shard(close_requests).await
    }
}

/// Generate the space id from the schema id with assumption schema id is unique
/// globally.
#[inline]
pub fn build_space_id(schema_id: SchemaId) -> SpaceId {
    schema_id.as_u32()
}
