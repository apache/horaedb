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

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use common_types::projected_schema::ProjectedSchema;
use datafusion::{
    error::{DataFusionError, Result as DfResult},
    execution::TaskContext,
    physical_plan::{ExecutionPlan, SendableRecordBatchStream},
};
use futures::future::BoxFuture;
use generic_error::BoxError;
use table_engine::{predicate::PredicateRef, remote::model::TableIdentifier, table::TableRef};
use trace_metric::collector::RemoteMetricsCollector;

pub mod codec;
pub mod physical_plan;
pub mod resolver;
#[cfg(test)]
pub mod test_util;

/// Remote datafusion physical plan executor
pub trait RemotePhysicalPlanExecutor: fmt::Debug + Send + Sync + 'static {
    fn execute(
        &self,
        table: TableIdentifier,
        task_context: &TaskContext,
        plan: Arc<dyn ExecutionPlan>,
        remote_metrics_collector: RemoteMetricsCollector,
    ) -> DfResult<BoxFuture<'static, DfResult<SendableRecordBatchStream>>>;
}

pub type RemotePhysicalPlanExecutorRef = Arc<dyn RemotePhysicalPlanExecutor>;

/// Executable scan's builder
///
/// It is not suitable to restrict the detailed implementation of executable
/// scan, so we define a builder here which return the general `ExecutionPlan`.
#[async_trait]
pub trait ExecutableScanBuilder: fmt::Debug + Send + Sync + 'static {
    async fn build(
        &self,
        table: TableRef,
        ctx: TableScanContext,
    ) -> DfResult<Arc<dyn ExecutionPlan>>;
}

type ExecutableScanBuilderRef = Box<dyn ExecutableScanBuilder>;

#[derive(Clone)]
pub struct TableScanContext {
    pub batch_size: usize,

    /// Suggested read parallelism, the actual returned stream should equal to
    /// `read_parallelism`.
    pub read_parallelism: usize,

    /// The schema and projection for read, the output data should match this
    /// schema.
    pub projected_schema: ProjectedSchema,

    /// Predicate of the query.
    pub predicate: PredicateRef,
}

impl TableScanContext {
    pub fn new(
        batch_size: usize,
        read_parallelism: usize,
        projected_schema: ProjectedSchema,
        predicate: PredicateRef,
    ) -> Self {
        Self {
            batch_size,
            read_parallelism,
            projected_schema,
            predicate,
        }
    }
}

impl TryFrom<TableScanContext> for ceresdbproto::remote_engine::TableScanContext {
    type Error = datafusion::error::DataFusionError;

    fn try_from(value: TableScanContext) -> DfResult<Self> {
        let pb_projected_schema = value
            .projected_schema
            .try_into()
            .box_err()
            .map_err(DataFusionError::External)?;

        let pb_predicate = value
            .predicate
            .as_ref()
            .try_into()
            .box_err()
            .map_err(DataFusionError::External)?;

        Ok(Self {
            batch_size: value.batch_size as u64,
            read_parallelism: value.read_parallelism as u64,
            projected_schema: Some(pb_projected_schema),
            predicate: Some(pb_predicate),
        })
    }
}

impl TryFrom<ceresdbproto::remote_engine::TableScanContext> for TableScanContext {
    type Error = datafusion::error::DataFusionError;

    fn try_from(value: ceresdbproto::remote_engine::TableScanContext) -> DfResult<Self> {
        let projected_schema = value
            .projected_schema
            .ok_or(DataFusionError::Internal(
                "projected schema not found".to_string(),
            ))?
            .try_into()
            .map_err(|e| {
                DataFusionError::Internal(format!("failed to decode projected schema, err:{}", e))
            })?;

        let predicate = value
            .predicate
            .ok_or(DataFusionError::Internal("predicate not found".to_string()))?
            .try_into()
            .map_err(|e| {
                DataFusionError::Internal(format!("failed to decode predicate, err:{}", e))
            })?;

        Ok(Self {
            batch_size: value.batch_size as usize,
            read_parallelism: value.read_parallelism as usize,
            projected_schema,
            predicate: Arc::new(predicate),
        })
    }
}

impl fmt::Debug for TableScanContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let projection = self.projected_schema.projection();

        f.debug_struct("TableScanContext")
            .field("read_parallelism", &self.read_parallelism)
            .field("batch_size", &self.batch_size)
            .field("projection", &projection)
            .field("predicate", &self.predicate)
            .finish()
    }
}
