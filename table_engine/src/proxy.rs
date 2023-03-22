// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Table engine proxy

use async_trait::async_trait;

use crate::{
    engine::{
        CloseTableRequest, CreateTableRequest, DropTableRequest, OpenTableRequest, TableEngine,
        TableEngineRef, UnknownEngineType,
    },
    memory::MemoryTableEngine,
    table::TableRef,
    ANALYTIC_ENGINE_TYPE, MEMORY_ENGINE_TYPE,
};

/// Route [CreateTableRequest] to the correct engine by its engine type
pub struct TableEngineProxy {
    /// Memory table engine
    pub memory: MemoryTableEngine,
    /// Analytic table engine
    pub analytic: TableEngineRef,
}

#[async_trait]
impl TableEngine for TableEngineProxy {
    fn engine_type(&self) -> &str {
        "TableEngineProxy"
    }

    async fn close(&self) -> crate::engine::Result<()> {
        self.memory.close().await?;
        self.analytic.close().await?;

        Ok(())
    }

    async fn create_table(&self, request: CreateTableRequest) -> crate::engine::Result<TableRef> {
        // TODO(yingwen): Use a map
        match request.engine.as_str() {
            MEMORY_ENGINE_TYPE => self.memory.create_table(request).await,
            ANALYTIC_ENGINE_TYPE => self.analytic.create_table(request).await,
            engine_type => UnknownEngineType { engine_type }.fail(),
        }
    }

    async fn drop_table(&self, request: DropTableRequest) -> crate::engine::Result<bool> {
        match request.engine.as_str() {
            MEMORY_ENGINE_TYPE => self.memory.drop_table(request).await,
            ANALYTIC_ENGINE_TYPE => self.analytic.drop_table(request).await,
            engine_type => UnknownEngineType { engine_type }.fail(),
        }
    }

    /// Open table, return error if table not exists
    async fn open_table(
        &self,
        request: OpenTableRequest,
    ) -> crate::engine::Result<Option<TableRef>> {
        match request.engine.as_str() {
            MEMORY_ENGINE_TYPE => self.memory.open_table(request).await,
            ANALYTIC_ENGINE_TYPE => self.analytic.open_table(request).await,
            engine_type => UnknownEngineType { engine_type }.fail(),
        }
    }

    /// Close table, it is ok to close a closed table.
    async fn close_table(&self, request: CloseTableRequest) -> crate::engine::Result<()> {
        match request.engine.as_str() {
            MEMORY_ENGINE_TYPE => self.memory.close_table(request).await,
            ANALYTIC_ENGINE_TYPE => self.analytic.close_table(request).await,
            engine_type => UnknownEngineType { engine_type }.fail(),
        }
    }
}
