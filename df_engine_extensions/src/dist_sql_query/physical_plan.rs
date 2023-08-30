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

use std::{
    any::Any,
    fmt,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{datatypes::SchemaRef as ArrowSchemaRef, record_batch::RecordBatch};
use common_types::schema::RecordSchema;
use datafusion::{
    error::{DataFusionError, Result as DfResult},
    execution::TaskContext,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
        SendableRecordBatchStream as DfSendableRecordBatchStream, Statistics,
    },
};
use datafusion_proto::{
    bytes::physical_plan_to_bytes_with_extension_codec, physical_plan::PhysicalExtensionCodec,
};
use futures::{future::BoxFuture, FutureExt, Stream, StreamExt};
use table_engine::{remote::model::TableIdentifier, table::ReadRequest};

use crate::dist_sql_query::{EncodedPlan, RemotePhysicalPlanExecutor};

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

    fn schema(&self) -> ArrowSchemaRef {
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
        let new_plans = self
            .remote_exec_plans
            .iter()
            .map(|(table, plan)| {
                extended_node
                    .clone()
                    .with_new_children(vec![plan.clone()])
                    .map(|extended_plan| (table.clone(), extended_plan))
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

    fn schema(&self) -> ArrowSchemaRef {
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
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<DfSendableRecordBatchStream> {
        let (sub_table, plan) = &self.remote_exec_plans[partition];

        // Encode to build `EncodedPlan`.
        let plan_bytes = physical_plan_to_bytes_with_extension_codec(
            plan.clone(),
            self.extension_codec.as_ref(),
        )?;
        let record_schema = RecordSchema::try_from(plan.schema()).map_err(|e| {
            DataFusionError::Internal(format!(
                "failed to convert arrow_schema to record_schema, arrow_schema:{}, err:{e}",
                plan.schema()
            ))
        })?;
        let encoded_plan = EncodedPlan {
            plan: plan_bytes,
            schema: record_schema,
        };

        // Send plan for remote execution.
        let stream_future =
            self.remote_executor
                .execute(sub_table.clone(), &context, encoded_plan)?;
        let record_stream = PartitionedScanStream::new(stream_future, plan.schema());

        Ok(Box::pin(record_stream))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

/// Partitioned scan stream
struct PartitionedScanStream {
    /// Future to init the stream
    stream_future: BoxFuture<'static, DfResult<DfSendableRecordBatchStream>>,

    /// Inited stream to poll the record
    stream_state: StreamState,

    /// Record schema
    arrow_record_schema: ArrowSchemaRef,
}

impl PartitionedScanStream {
    /// Create an empty RecordBatchStream
    pub fn new(
        stream_future: BoxFuture<'static, DfResult<DfSendableRecordBatchStream>>,
        arrow_record_schema: ArrowSchemaRef,
    ) -> Self {
        Self {
            stream_future,
            stream_state: StreamState::Initializing,
            arrow_record_schema,
        }
    }
}

impl RecordBatchStream for PartitionedScanStream {
    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_record_schema.clone()
    }
}

impl Stream for PartitionedScanStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            let stream_state = &mut this.stream_state;
            match stream_state {
                StreamState::Initializing => {
                    let poll_res = this.stream_future.poll_unpin(cx);
                    match poll_res {
                        Poll::Ready(Ok(stream)) => {
                            *stream_state = StreamState::Polling(stream);
                        }
                        Poll::Ready(Err(e)) => {
                            *stream_state = StreamState::Failed(e.to_string());
                            return Poll::Ready(Some(Err(e)));
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }

                StreamState::Polling(stream) => {
                    let poll_res = stream.poll_next_unpin(cx);
                    if let Poll::Ready(Some(Err(e))) = &poll_res {
                        *stream_state = StreamState::Failed(e.to_string());
                    }

                    return poll_res;
                }

                StreamState::Failed(err_msg) => {
                    return Poll::Ready(Some(Err(DataFusionError::Internal(format!(
                        "failed to poll record stream, err:{err_msg}"
                    )))));
                }
            }
        }
    }
}

pub enum StreamState {
    Initializing,
    Polling(DfSendableRecordBatchStream),
    Failed(String),
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

    fn schema(&self) -> ArrowSchemaRef {
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

impl TryFrom<ceresdbproto::remote_engine::UnresolvedSubScan> for UnresolvedSubTableScan {
    type Error = DataFusionError;

    fn try_from(
        value: ceresdbproto::remote_engine::UnresolvedSubScan,
    ) -> Result<Self, Self::Error> {
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

impl TryFrom<UnresolvedSubTableScan> for ceresdbproto::remote_engine::UnresolvedSubScan {
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
