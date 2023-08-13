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

// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{any::Any, fmt, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    error::{DataFusionError, Result as DfResult},
    execution::TaskContext,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
        SendableRecordBatchStream as DfSendableRecordBatchStream, Statistics,
    },
};
use table_engine::{
    remote::{model::TableIdentifier, RemoteEngineRef},
    table::ReadRequest,
};

#[derive(Debug)]
pub struct UnresolvedPartitionedScan {
    pub sub_tables: Vec<TableIdentifier>,
    pub read_request: ReadRequest,
}

impl ExecutionPlan for UnresolvedPartitionedScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.read_request
            .projected_schema
            .to_projected_arrow_schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.sub_tables.len())
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(
            "UnresolvedPartitionedScan should not have children".to_string(),
        ))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<DfSendableRecordBatchStream> {
        Err(DataFusionError::Internal(
            "UnresolvedPartitionedScan can not be executed".to_string(),
        ))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for UnresolvedPartitionedScan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "UnresolvedPartitionedScan: sub_tables={:?}, read_request:{:?}, partition_count={}",
            self.sub_tables,
            self.read_request,
            self.output_partitioning().partition_count(),
        )
    }
}

/// Executable partitioned table scan
///
/// It will send the `remote_exec_plan`s to corresponding nodes to execute.
#[derive(Debug)]
pub struct ResolvedPartitionedScan {
    pub remote_engine: RemoteEngineRef,
    pub remote_exec_plans: Vec<(TableIdentifier, Arc<dyn ExecutionPlan>)>,
}

impl ExecutionPlan for ResolvedPartitionedScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    // TODO: check if it is right.
    fn schema(&self) -> SchemaRef {
        self.remote_exec_plans
            .first()
            .expect("remote_exec_plans should not be empty")
            .1
            .schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.remote_exec_plans.len())
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(
            "UnresolvedPartitionedScan should not have children".to_string(),
        ))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<DfSendableRecordBatchStream> {
        todo!()
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

// TODO: make display for the plan more pretty.
impl DisplayAs for ResolvedPartitionedScan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ResolvedPartitionedScan: remote_exec_plans:{:?}, partition_count={}",
            self.remote_exec_plans,
            self.output_partitioning().partition_count(),
        )
    }
}
