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

//! Remote table engine

pub mod model;

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use generic_error::GenericError;
use macros::define_result;
use model::{ReadRequest, WriteRequest};
use snafu::Snafu;
use trace_metric::MetricsCollector;

use crate::{
    remote::model::{
        AlterTableOptionsRequest, AlterTableSchemaRequest, ExecutePlanRequest, GetTableInfoRequest,
        TableInfo, WriteBatchResult,
    },
    stream::SendableRecordBatchStream,
};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to read from remote, err:{}", source))]
    Read { source: GenericError },

    #[snafu(display("Failed to write to remote, err:{}", source))]
    Write { source: GenericError },

    #[snafu(display("Failed to alter schema, err:{}", source))]
    AlterSchema { source: GenericError },

    #[snafu(display("Failed to alter options, err:{}", source))]
    AlterOptions { source: GenericError },

    #[snafu(display("Failed to get table info from remote, err:{}", source))]
    GetTableInfo { source: GenericError },

    #[snafu(display("Failed to execute physical plan from remote, err:{}", source))]
    ExecutePhysicalPlan { source: GenericError },
}

define_result!(Error);

/// Remote table engine interface
#[async_trait]
pub trait RemoteEngine: fmt::Debug + Send + Sync {
    /// Read from the remote engine.
    async fn read(&self, request: ReadRequest) -> Result<SendableRecordBatchStream>;

    /// Write to the remote engine.
    async fn write(&self, request: WriteRequest) -> Result<usize>;

    async fn write_batch(&self, requests: Vec<WriteRequest>) -> Result<Vec<WriteBatchResult>>;

    async fn alter_table_schema(&self, request: AlterTableSchemaRequest) -> Result<()>;

    async fn alter_table_options(&self, request: AlterTableOptionsRequest) -> Result<()>;

    async fn get_table_info(&self, request: GetTableInfoRequest) -> Result<TableInfo>;

    async fn execute_physical_plan(
        &self,
        request: ExecutePlanRequest,
        metrics_collector: MetricsCollector,
    ) -> Result<SendableRecordBatchStream>;
}

/// Remote engine reference
pub type RemoteEngineRef = Arc<dyn RemoteEngine>;
