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
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use arrow::{datatypes::SchemaRef as ArrowSchemaRef, record_batch::RecordBatch};
use datafusion::{
    error::{DataFusionError, Result as DfResult},
    execution::TaskContext,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        aggregates::{AggregateExec, AggregateMode},
        coalesce_batches::CoalesceBatchesExec,
        coalesce_partitions::CoalescePartitionsExec,
        displayable,
        filter::FilterExec,
        metrics::{Count, MetricValue, MetricsSet},
        projection::ProjectionExec,
        repartition::RepartitionExec,
        DisplayAs, DisplayFormatType, ExecutionPlan, Metric, Partitioning, RecordBatchStream,
        SendableRecordBatchStream as DfSendableRecordBatchStream, Statistics,
    },
};
use futures::{future::BoxFuture, FutureExt, Stream, StreamExt};
use table_engine::{remote::model::TableIdentifier, table::ReadRequest};
use trace_metric::{collector::FormatCollectorVisitor, MetricsCollector, TraceMetricWhenDrop};

use crate::dist_sql_query::{RemotePhysicalPlanExecutor, RemoteTaskContext, TableScanContext};

/// Placeholder of partitioned table's scan plan
/// It is inexecutable actually and just for carrying the necessary information
/// of building remote execution plans for sub tables.
// TODO: can we skip this and generate `ResolvedPartitionedScan` directly?
#[derive(Debug)]
pub struct UnresolvedPartitionedScan {
    pub sub_tables: Vec<TableIdentifier>,
    pub table_scan_ctx: TableScanContext,
    pub metrics_collector: MetricsCollector,
}

impl UnresolvedPartitionedScan {
    pub fn new(
        table_name: &str,
        sub_tables: Vec<TableIdentifier>,
        read_request: ReadRequest,
    ) -> Self {
        let metrics_collector = MetricsCollector::new(table_name.to_string());
        let table_scan_ctx = TableScanContext {
            batch_size: read_request.opts.batch_size,
            read_parallelism: read_request.opts.read_parallelism,
            projected_schema: read_request.projected_schema,
            predicate: read_request.predicate,
        };

        Self {
            sub_tables,
            table_scan_ctx,
            metrics_collector,
        }
    }
}

impl ExecutionPlan for UnresolvedPartitionedScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.table_scan_ctx
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
            "UnresolvedPartitionedScan: sub_tables={:?}, table_scan_ctx:{:?}, partition_count={}",
            self.sub_tables,
            self.table_scan_ctx,
            self.output_partitioning().partition_count(),
        )
    }
}

/// The executable scan plan of the partitioned table
/// It includes remote execution plans of sub tables, and will send them to
/// related nodes to execute.
#[derive(Debug)]
pub(crate) struct ResolvedPartitionedScan {
    pub remote_exec_ctx: Arc<RemoteExecContext>,
    pub pushdown_continue: bool,
    pub metrics_collector: MetricsCollector,
    pub is_analyze: bool,
}

impl ResolvedPartitionedScan {
    pub fn new(
        remote_executor: Arc<dyn RemotePhysicalPlanExecutor>,
        sut_table_plan_ctxs: Vec<SubTablePlanContext>,
        metrics_collector: MetricsCollector,
        is_analyze: bool,
    ) -> Self {
        let remote_exec_ctx = Arc::new(RemoteExecContext {
            executor: remote_executor,
            plan_ctxs: sut_table_plan_ctxs,
        });

        Self::new_with_details(remote_exec_ctx, true, metrics_collector, is_analyze)
    }

    pub fn new_with_details(
        remote_exec_ctx: Arc<RemoteExecContext>,
        pushdown_continue: bool,
        metrics_collector: MetricsCollector,
        is_analyze: bool,
    ) -> Self {
        Self {
            remote_exec_ctx,
            pushdown_continue,
            metrics_collector,
            is_analyze,
        }
    }

    pub fn pushdown_finished(&self) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self {
            remote_exec_ctx: self.remote_exec_ctx.clone(),
            pushdown_continue: false,
            metrics_collector: self.metrics_collector.clone(),
            is_analyze: self.is_analyze,
        })
    }

    pub fn try_to_push_down_more(
        &self,
        cur_node: Arc<dyn ExecutionPlan>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Can not push more...
        if !self.pushdown_continue {
            return cur_node.with_new_children(vec![self.pushdown_finished()]);
        }

        // Push down more, and when occur the terminated push down able node, we need to
        // set `can_push_down_more` false.
        let pushdown_status = Self::maybe_a_pushdown_node(cur_node.clone());
        let (node, can_push_down_more) = match pushdown_status {
            PushDownEvent::Continue(node) => (node, true),
            PushDownEvent::Terminated(node) => (node, false),
            PushDownEvent::Unable => {
                let partitioned_scan = self.pushdown_finished();
                return cur_node.with_new_children(vec![partitioned_scan]);
            }
        };

        let new_plan_ctxs = self
            .remote_exec_ctx
            .plan_ctxs
            .iter()
            .map(|plan_ctx| {
                node.clone()
                    .with_new_children(vec![plan_ctx.plan.clone()])
                    .map(|extended_plan| SubTablePlanContext {
                        table: plan_ctx.table.clone(),
                        plan: extended_plan,
                        metrics_collector: plan_ctx.metrics_collector.clone(),
                        remote_metrics: plan_ctx.remote_metrics.clone(),
                    })
            })
            .collect::<DfResult<Vec<_>>>()?;

        let remote_exec_ctx = Arc::new(RemoteExecContext {
            executor: self.remote_exec_ctx.executor.clone(),
            plan_ctxs: new_plan_ctxs,
        });
        let plan = ResolvedPartitionedScan::new_with_details(
            remote_exec_ctx,
            can_push_down_more,
            self.metrics_collector.clone(),
            self.is_analyze,
        );

        Ok(Arc::new(plan))
    }

    #[inline]
    pub fn maybe_a_pushdown_node(plan: Arc<dyn ExecutionPlan>) -> PushDownEvent {
        PushDownEvent::new(plan)
    }

    /// `ResolvedPartitionedScan` can be executable after satisfying followings:
    ///    + The pushdown searching process is finished.
    #[inline]
    fn is_executable(&self) -> bool {
        !self.pushdown_continue
    }
}

#[derive(Debug)]
pub struct RemoteExecContext {
    executor: Arc<dyn RemotePhysicalPlanExecutor>,
    plan_ctxs: Vec<SubTablePlanContext>,
}

#[derive(Debug)]
pub(crate) struct SubTablePlanContext {
    table: TableIdentifier,
    plan: Arc<dyn ExecutionPlan>,
    metrics_collector: MetricsCollector,
    remote_metrics: Arc<Mutex<String>>,
}

impl SubTablePlanContext {
    pub fn new(
        table: TableIdentifier,
        plan: Arc<dyn ExecutionPlan>,
        metrics_collector: MetricsCollector,
        remote_metrics: Arc<Mutex<String>>,
    ) -> Self {
        Self {
            table,
            plan,
            metrics_collector,
            remote_metrics,
        }
    }
}

impl ExecutionPlan for ResolvedPartitionedScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.remote_exec_ctx
            .plan_ctxs
            .first()
            .expect("remote_exec_plans should not be empty")
            .plan
            .schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.remote_exec_ctx.plan_ctxs.len())
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // If this is a analyze plan, we should not collect metrics of children
        // which have been send to remote, So we just return empty children.
        if self.is_analyze {
            return vec![];
        }

        self.remote_exec_ctx
            .plan_ctxs
            .iter()
            .map(|plan_ctx| plan_ctx.plan.clone())
            .collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(
            "UnresolvedPartitionedScan can't be built directly from new children".to_string(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<DfSendableRecordBatchStream> {
        if !self.is_executable() {
            return Err(DataFusionError::Internal(format!(
                "partitioned scan is still inexecutable, plan:{}",
                displayable(self).indent(true)
            )));
        }

        let SubTablePlanContext {
            table: sub_table,
            plan,
            metrics_collector,
            remote_metrics,
        } = &self.remote_exec_ctx.plan_ctxs[partition];

        let remote_task_ctx =
            RemoteTaskContext::new(context, sub_table.clone(), remote_metrics.clone());

        // Send plan for remote execution.
        let stream_future = self
            .remote_exec_ctx
            .executor
            .execute(remote_task_ctx, plan.clone())?;
        let record_stream =
            PartitionedScanStream::new(stream_future, plan.schema(), metrics_collector.clone());

        Ok(Box::pin(record_stream))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        let mut metric_set = MetricsSet::new();

        for sub_table_ctx in &self.remote_exec_ctx.plan_ctxs {
            let metrics_desc = format!(
                "{}:\n{}",
                sub_table_ctx.table.table,
                sub_table_ctx.remote_metrics.lock().unwrap()
            );
            metric_set.push(Arc::new(Metric::new(
                MetricValue::Count {
                    name: format!("\n{metrics_desc}").into(),
                    count: Count::new(),
                },
                None,
            )));
        }

        let mut format_visitor = FormatCollectorVisitor::default();
        self.metrics_collector.visit(&mut format_visitor);
        let metrics_desc = format_visitor.into_string();
        metric_set.push(Arc::new(Metric::new(
            MetricValue::Count {
                name: format!("\n{metrics_desc}").into(),
                count: Count::new(),
            },
            None,
        )));

        Some(metric_set)
    }
}

/// Partitioned scan stream
pub(crate) struct PartitionedScanStream {
    /// Future to init the stream
    stream_future: BoxFuture<'static, DfResult<DfSendableRecordBatchStream>>,

    /// Stream to poll the records
    stream_state: StreamState,

    /// Record schema
    arrow_record_schema: ArrowSchemaRef,

    /// Last time left due to `Pending`
    last_time_left: Option<Instant>,

    /// Metrics collected for analyze
    metrics: Metrics,
}

impl PartitionedScanStream {
    /// Create an empty RecordBatchStream
    pub fn new(
        stream_future: BoxFuture<'static, DfResult<DfSendableRecordBatchStream>>,
        arrow_record_schema: ArrowSchemaRef,
        metrics_collector: MetricsCollector,
    ) -> Self {
        let metrics = Metrics {
            metrics_collector,
            ..Default::default()
        };
        Self {
            stream_future,
            stream_state: StreamState::Initializing,
            arrow_record_schema,
            last_time_left: None,
            metrics,
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
        let this_time_polled = Instant::now();
        let wait_cost = match this.last_time_left {
            Some(last_left) => this_time_polled.saturating_duration_since(last_left),
            None => Duration::default(),
        };
        this.metrics.wait_duration += wait_cost;
        this.metrics.total_duration += wait_cost;

        let poll_result = loop {
            let stream_state = &mut this.stream_state;
            match stream_state {
                StreamState::Initializing => {
                    let poll_res = this.stream_future.poll_unpin(cx);
                    match poll_res {
                        Poll::Ready(Ok(stream)) => {
                            *stream_state = StreamState::Polling(stream);
                        }
                        Poll::Ready(Err(e)) => {
                            *stream_state = StreamState::InitializeFailed;
                            break Poll::Ready(Some(Err(e)));
                        }
                        Poll::Pending => break Poll::Pending,
                    }
                }
                StreamState::InitializeFailed => return Poll::Ready(None),
                StreamState::Polling(stream) => break stream.poll_next_unpin(cx),
            }
        };

        let this_time_left = Instant::now();
        let poll_cost = this_time_left.saturating_duration_since(this_time_polled);
        this.metrics.poll_duration += poll_cost;
        this.metrics.total_duration += poll_cost;
        this.last_time_left = Some(this_time_left);

        poll_result
    }
}

/// Stream state
/// Before polling record batch from it, we must initializing the record batch
/// stream first. The process of state changing is like:
///
/// ```plaintext
///     ┌────────────┐                                        
///     │Initializing│                                        
///     └──────┬─────┘                                        
///   _________▽_________     ┌──────────────────────────────┐
///  ╱                   ╲    │Polling(we just return the    │
/// ╱ Success to init the ╲___│inner stream's polling result)│
/// ╲ record batch stream ╱yes└──────────────────────────────┘
///  ╲___________________╱                                    
///           │no                                            
///  ┌────────▽───────┐                                      
///  │InitializeFailed│                                      
///  └────────────────┘                                      
/// ```
pub(crate) enum StreamState {
    Initializing,
    InitializeFailed,
    Polling(DfSendableRecordBatchStream),
}

impl DisplayAs for ResolvedPartitionedScan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "ResolvedPartitionedScan: pushdown_continue:{}, partition_count:{}",
                    self.pushdown_continue,
                    self.remote_exec_ctx.plan_ctxs.len()
                )
            }
        }
    }
}

/// Placeholder of sub table's scan plan
/// It is inexecutable actually and just for carrying the necessary information
/// of building the executable scan plan.
#[derive(Debug, Clone)]
pub struct UnresolvedSubTableScan {
    pub table: TableIdentifier,
    pub table_scan_ctx: TableScanContext,
}

impl ExecutionPlan for UnresolvedSubTableScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.table_scan_ctx
            .projected_schema
            .to_projected_arrow_schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.table_scan_ctx.read_parallelism)
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
            "UnresolvedSubTableScan: table:{:?}, table_scan_ctx:{:?}, partition_count:{}",
            self.table,
            self.table_scan_ctx,
            self.output_partitioning().partition_count(),
        )
    }
}

impl TryFrom<ceresdbproto::remote_engine::UnresolvedSubScan> for UnresolvedSubTableScan {
    type Error = DataFusionError;

    fn try_from(
        value: ceresdbproto::remote_engine::UnresolvedSubScan,
    ) -> Result<Self, Self::Error> {
        let table = value
            .table
            .ok_or(DataFusionError::Internal(
                "table ident not found".to_string(),
            ))?
            .into();
        let table_scan_ctx = value
            .table_scan_ctx
            .ok_or(DataFusionError::Internal(
                "table scan context not found".to_string(),
            ))?
            .try_into()
            .map_err(|e| {
                DataFusionError::Internal(format!("failed to decode table scan context, err:{e}"))
            })?;

        Ok(Self {
            table,
            table_scan_ctx,
        })
    }
}

impl TryFrom<UnresolvedSubTableScan> for ceresdbproto::remote_engine::UnresolvedSubScan {
    type Error = DataFusionError;

    fn try_from(value: UnresolvedSubTableScan) -> Result<Self, Self::Error> {
        let table = value.table.into();
        let table_scan_ctx = value.table_scan_ctx.try_into().map_err(|e| {
            DataFusionError::Internal(format!("failed to encode read request, err:{e}"))
        })?;

        Ok(Self {
            table: Some(table),
            table_scan_ctx: Some(table_scan_ctx),
        })
    }
}

/// Pushdown status, including:
///   + Unable, plan node which can't be pushed down to
///     `ResolvedPartitionedScan` node.
///   + Continue, node able to be pushed down to `ResolvedPartitionedScan`, and
///     the newly generated `ResolvedPartitionedScan` can continue to accept
///     more pushdown nodes after.
///   + Terminated, node able to be pushed down to `ResolvedPartitionedScan`,
///     but the newly generated `ResolvedPartitionedScan` can't accept more
///     pushdown nodes after.
pub enum PushDownEvent {
    Unable,
    Continue(Arc<dyn ExecutionPlan>),
    Terminated(Arc<dyn ExecutionPlan>),
}

impl PushDownEvent {
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        if let Some(aggr) = plan.as_any().downcast_ref::<AggregateExec>() {
            if *aggr.mode() == AggregateMode::Partial {
                Self::Terminated(plan)
            } else {
                Self::Unable
            }
        } else if plan.as_any().downcast_ref::<FilterExec>().is_some()
            || plan.as_any().downcast_ref::<ProjectionExec>().is_some()
            || plan.as_any().downcast_ref::<RepartitionExec>().is_some()
            || plan
                .as_any()
                .downcast_ref::<CoalescePartitionsExec>()
                .is_some()
            || plan
                .as_any()
                .downcast_ref::<CoalesceBatchesExec>()
                .is_some()
        {
            Self::Continue(plan)
        } else {
            Self::Unable
        }
    }
}
/// Metrics for [ChainIterator].
#[derive(TraceMetricWhenDrop, Default)]
struct Metrics {
    #[metric(duration)]
    wait_duration: Duration,
    #[metric(duration)]
    poll_duration: Duration,
    #[metric(duration)]
    total_duration: Duration,
    #[metric(collector)]
    metrics_collector: MetricsCollector,
}

#[cfg(test)]
mod test {
    use datafusion::error::DataFusionError;
    use futures::StreamExt;

    use crate::dist_sql_query::{
        physical_plan::PartitionedScanStream,
        test_util::{MockPartitionedScanStreamBuilder, PartitionedScanStreamCase},
    };

    #[tokio::test]
    async fn test_stream_poll_success() {
        let builder = MockPartitionedScanStreamBuilder::new(PartitionedScanStreamCase::Success);
        let mut stream = builder.build();
        let result_opt = stream.next().await;
        assert!(result_opt.is_none());
    }

    #[tokio::test]
    async fn test_stream_init_failed() {
        let builder =
            MockPartitionedScanStreamBuilder::new(PartitionedScanStreamCase::InitializeFailed);
        let stream = builder.build();
        test_stream_failed_state(stream, "failed to init").await
    }

    #[tokio::test]
    async fn test_stream_poll_failed() {
        let builder = MockPartitionedScanStreamBuilder::new(PartitionedScanStreamCase::PollFailed);
        let stream = builder.build();
        test_stream_failed_state(stream, "failed to poll").await
    }

    async fn test_stream_failed_state(mut stream: PartitionedScanStream, failed_msg: &str) {
        // Error happened, check error message.
        let result_opt = stream.next().await;
        assert!(result_opt.is_some());
        let result = result_opt.unwrap();
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            DataFusionError::Internal(msg) => {
                assert!(msg.contains(failed_msg))
            }
            other => panic!("unexpected error:{other}"),
        }

        // Should return `None` in next poll.
        let result_opt = stream.next().await;
        assert!(result_opt.is_none());
    }
}
