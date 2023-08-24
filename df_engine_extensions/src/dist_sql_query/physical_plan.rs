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

//! dist sql query physical plans

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
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use table_engine::{remote::model::TableIdentifier, table::ReadRequest};

use crate::dist_sql_query::RemotePhysicalPlanExecutor;

/// Placeholder of partitioned table's scan plan
/// It is inexecutable actually and just for carrying the necessary information
/// of building remote execution plans for sub tables.
// TODO: can we skip this and generate `ResolvedPartitionedScan` directly?
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

/// The executable scan plan of the partitioned table
/// It includes remote execution plans of sub tables, and will send them to
/// related nodes to execute.
#[derive(Debug)]
pub struct ResolvedPartitionedScan {
    pub remote_executor: Arc<dyn RemotePhysicalPlanExecutor>,
    pub remote_exec_plans: Vec<(TableIdentifier, Arc<dyn ExecutionPlan>)>,
    pub extension_codec: Arc<dyn PhysicalExtensionCodec>,
}

impl ResolvedPartitionedScan {
    pub fn extend_remote_exec_plans(
        &self,
        extended_node: Arc<dyn ExecutionPlan>,
    ) -> DfResult<Arc<ResolvedPartitionedScan>> {
        let origin_plans = self.remote_exec_plans.clone();
        let new_plans = origin_plans
            .into_iter()
            .map(|(table, plan)| {
                extended_node
                    .clone()
                    .with_new_children(vec![plan])
                    .map(|extended_plan| (table, extended_plan))
            })
            .collect::<DfResult<Vec<_>>>()?;

        let plan = ResolvedPartitionedScan {
            remote_executor: self.remote_executor.clone(),
            remote_exec_plans: new_plans,
            extension_codec: self.extension_codec.clone(),
        };

        Ok(Arc::new(plan))
    }
}

impl ExecutionPlan for ResolvedPartitionedScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

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

/// Placeholder of sub table's scan plan
/// It is inexecutable actually and just for carrying the necessary information
/// of building the executable scan plan.
#[derive(Debug, Clone)]
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

impl TryFrom<ceresdbproto::remote_engine::UnreolvedSubScan> for UnresolvedSubTableScan {
    type Error = DataFusionError;

    fn try_from(value: ceresdbproto::remote_engine::UnreolvedSubScan) -> Result<Self, Self::Error> {
        let table_ident: TableIdentifier = value
            .table
            .ok_or(DataFusionError::Internal(
                "table ident not found".to_string(),
            ))?
            .into();
        let read_request: ReadRequest = value
            .read_request
            .ok_or(DataFusionError::Internal(
                "read request not found".to_string(),
            ))?
            .try_into()
            .map_err(|e| {
                DataFusionError::Internal(format!("failed to decode read request, err:{e}"))
            })?;

        Ok(Self {
            table: table_ident,
            read_request,
        })
    }
}

impl TryFrom<UnresolvedSubTableScan> for ceresdbproto::remote_engine::UnreolvedSubScan {
    type Error = DataFusionError;

    fn try_from(value: UnresolvedSubTableScan) -> Result<Self, Self::Error> {
        let table_ident: ceresdbproto::remote_engine::TableIdentifier = value.table.into();
        let read_request: ceresdbproto::remote_engine::TableReadRequest =
            value.read_request.try_into().map_err(|e| {
                DataFusionError::Internal(format!("failed to encode read request, err:{e}"))
            })?;

        Ok(Self {
            table: Some(table_ident),
            read_request: Some(read_request),
        })
    }
}
