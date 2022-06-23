// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Table engine implementation

use std::sync::Arc;

use async_trait::async_trait;
use table_engine::{
    engine::{
        CloseTableRequest, CreateTableRequest, DropTableRequest, OpenTableRequest, Result,
        TableEngine, TableEngineRef, UnknownEngineType,
    },
    memory::MemoryTable,
    table::TableRef,
    ANALYTIC_ENGINE_TYPE, MEMORY_ENGINE_TYPE,
};

/// Memory table engine implementation
// Mainly for test purpose now
pub struct MemoryTableEngine;

#[async_trait]
impl TableEngine for MemoryTableEngine {
    fn engine_type(&self) -> &str {
        MEMORY_ENGINE_TYPE
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }

    async fn create_table(&self, request: CreateTableRequest) -> Result<TableRef> {
        Ok(Arc::new(MemoryTable::new(
            request.table_name,
            request.table_id,
            request.table_schema,
            MEMORY_ENGINE_TYPE.to_string(),
        )))
    }

    async fn drop_table(&self, _request: DropTableRequest) -> Result<bool> {
        Ok(true)
    }

    async fn open_table(&self, _request: OpenTableRequest) -> Result<Option<TableRef>> {
        Ok(None)
    }

    async fn close_table(&self, _request: CloseTableRequest) -> Result<()> {
        Ok(())
    }
}

/// Route [CreateTableRequest] to the correct engine by its engine type
pub struct TableEngineProxy<T> {
    /// Memory table engine
    pub memory: MemoryTableEngine,
    /// Analytic table engine
    pub analytic: T,
}

#[async_trait]
impl<T: TableEngine + Send + Sync + 'static> TableEngine for TableEngineProxy {
    fn engine_type(&self) -> &str {
        "TableEngineProxy"
    }

    async fn close(&self) -> Result<()> {
        self.memory.close().await?;
        self.analytic.close().await?;

        Ok(())
    }

    async fn create_table(&self, request: CreateTableRequest) -> Result<TableRef> {
        // TODO(yingwen): Use a map
        match request.engine.as_str() {
            MEMORY_ENGINE_TYPE => self.memory.create_table(request).await,
            ANALYTIC_ENGINE_TYPE => self.analytic.create_table(request).await,
            engine_type => UnknownEngineType { engine_type }.fail(),
        }
    }

    async fn drop_table(&self, request: DropTableRequest) -> Result<bool> {
        match request.engine.as_str() {
            MEMORY_ENGINE_TYPE => self.memory.drop_table(request).await,
            ANALYTIC_ENGINE_TYPE => self.analytic.drop_table(request).await,
            engine_type => UnknownEngineType { engine_type }.fail(),
        }
    }

    /// Open table, return error if table not exists
    async fn open_table(&self, request: OpenTableRequest) -> Result<Option<TableRef>> {
        match request.engine.as_str() {
            MEMORY_ENGINE_TYPE => self.memory.open_table(request).await,
            ANALYTIC_ENGINE_TYPE => self.analytic.open_table(request).await,
            engine_type => UnknownEngineType { engine_type }.fail(),
        }
    }

    /// Close table, it is ok to close a closed table.
    async fn close_table(&self, request: OpenTableRequest) -> Result<()> {
        match request.engine.as_str() {
            MEMORY_ENGINE_TYPE => self.memory.close_table(request).await,
            ANALYTIC_ENGINE_TYPE => self.analytic.close_table(request).await,
            engine_type => UnknownEngineType { engine_type }.fail(),
        }
    }
}
