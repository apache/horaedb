// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Table engine proxy

use async_trait::async_trait;

use crate::{
    engine::{
        CloseShardRequest, CloseTableRequest, CreateTableRequest, DropTableRequest,
        OpenShardRequest, OpenShardResult, OpenTableRequest, TableEngine, TableEngineRef,
        UnknownEngineType,
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

    async fn validate_create_table(
        &self,
        request: &CreateTableRequest,
    ) -> crate::engine::Result<()> {
        match request.engine.as_str() {
            MEMORY_ENGINE_TYPE => self.memory.validate_create_table(request).await,
            ANALYTIC_ENGINE_TYPE => self.analytic.validate_create_table(request).await,
            engine_type => UnknownEngineType { engine_type }.fail(),
        }
    }

    async fn create_table(&self, request: CreateTableRequest) -> crate::engine::Result<TableRef> {
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

    async fn open_shard(
        &self,
        request: OpenShardRequest,
    ) -> crate::engine::Result<OpenShardResult> {
        match request.engine.as_str() {
            MEMORY_ENGINE_TYPE => self.memory.open_shard(request).await,
            ANALYTIC_ENGINE_TYPE => self.analytic.open_shard(request).await,
            engine_type => UnknownEngineType { engine_type }.fail(),
        }
    }

    /// Close tables on same shard.
    async fn close_shard(&self, request: CloseShardRequest) -> Vec<crate::engine::Result<String>> {
        match request.engine.as_str() {
            MEMORY_ENGINE_TYPE => self.memory.close_shard(request).await,
            ANALYTIC_ENGINE_TYPE => self.analytic.close_shard(request).await,
            engine_type => vec![UnknownEngineType { engine_type }.fail()],
        }
    }
}
