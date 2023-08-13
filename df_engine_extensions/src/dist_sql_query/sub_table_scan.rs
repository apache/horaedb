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
use table_engine::{provider::ScanTable, remote::model::TableIdentifier, table::ReadRequest};

/// Unresolved sub table scan which can't be executed before rewriting
#[derive(Debug)]
pub struct UnresolvedSubTableScan {
    pub table: TableIdentifier,
    pub read_request: ReadRequest,
}

impl ExecutionPlan for UnresolvedSubTableScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.read_request
            .projected_schema
            .to_projected_arrow_schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.read_request.opts.read_parallelism)
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
            "UnresolvedSubTableScan should not have children".to_string(),
        ))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<DfSendableRecordBatchStream> {
        Err(DataFusionError::Internal(
            "UnresolvedSubTableScan can not be executed".to_string(),
        ))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for UnresolvedSubTableScan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "UnresolvedSubTableScan: table={:?}, read_request:{:?}, partition_count={}",
            self.table,
            self.read_request,
            self.output_partitioning().partition_count(),
        )
    }
}

/// `ResolvedSubTableScan` is `ScanTable` actually.
pub type ResolvedSubTableScan = ScanTable;
