// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Datafusion `TableProvider` adapter

use std::{
    any::Any,
    collections::HashSet,
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use common_types::{projected_schema::ProjectedSchema, request_id::RequestId, schema::Schema};
use datafusion::{
    config::{ConfigEntry, ConfigExtension, ExtensionOptions},
    datasource::TableProvider,
    error::{DataFusionError, Result},
    execution::context::{SessionState, TaskContext},
    logical_expr::{Expr, TableProviderFilterPushDown, TableSource, TableType},
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        expressions,
        metrics::{Count, MetricValue, MetricsSet},
        projection::ProjectionExec,
        DisplayAs, DisplayFormatType, ExecutionPlan, Metric, Partitioning, PhysicalExpr,
        SendableRecordBatchStream as DfSendableRecordBatchStream, Statistics,
    },
};
use df_operator::visitor;
use logger::debug;
use runtime::Priority;
use trace_metric::{collector::FormatCollectorVisitor, MetricsCollector};

use crate::{
    predicate::{PredicateBuilder, PredicateRef},
    stream::{ScanStreamState, ToDfStream},
    table::{ReadOptions, ReadRequest, TableRef},
};

pub const SCAN_TABLE_METRICS_COLLECTOR_NAME: &str = "scan_table";

#[derive(Clone, Debug)]
pub struct HoraeDBOptions {
    pub request_id: String,
    pub request_timeout: Option<u64>,
    pub default_schema: String,
    pub default_catalog: String,
    pub priority: Priority,
}

impl ConfigExtension for HoraeDBOptions {
    const PREFIX: &'static str = "horaedb";
}

impl HoraeDBOptions {
    const REQUEST_ID_KEY: &'static str = "request_id";
    const REQUEST_PRIORITY_KEY: &'static str = "request_priority";
    const REQUEST_TIMEOUT_KEY: &'static str = "request_timeout";
}

impl ExtensionOptions for HoraeDBOptions {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        match key {
            Self::REQUEST_ID_KEY => self.request_id = value.to_string(),
            Self::REQUEST_TIMEOUT_KEY => {
                self.request_timeout = Some(value.parse::<u64>().map_err(|e| {
                    DataFusionError::External(
                        format!("could not parse request_timeout, input:{value}, err:{e:?}").into(),
                    )
                })?)
            }
            Self::REQUEST_PRIORITY_KEY => {
                self.priority = value
                    .parse::<u8>()
                    .map_err(|e| {
                        DataFusionError::External(
                            format!("request_priority should be u8, input:{value}, err:{e:?}")
                                .into(),
                        )
                    })
                    .and_then(|value| {
                        Priority::try_from(value).map_err(|e| {
                            DataFusionError::External(
                                format!("parse request_priority failed, input:{value}, err:{e:?}")
                                    .into(),
                            )
                        })
                    })?
            }
            _ => Err(DataFusionError::External(
                format!("could not find key, key:{key}").into(),
            ))?,
        }

        Ok(())
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        vec![
            ConfigEntry {
                key: Self::REQUEST_ID_KEY.to_string(),
                value: Some(self.request_id.to_string()),
                description: "",
            },
            ConfigEntry {
                key: Self::REQUEST_TIMEOUT_KEY.to_string(),
                value: self.request_timeout.map(|v| v.to_string()),
                description: "",
            },
            ConfigEntry {
                key: Self::REQUEST_PRIORITY_KEY.to_string(),
                value: Some(self.priority.as_u8().to_string()),
                description: "",
            },
        ]
    }
}

/// Builder for table scan which is for supporting different scan impls
#[async_trait]
pub trait TableScanBuilder: fmt::Debug + Send + Sync + 'static {
    async fn build(&self, request: ReadRequest) -> Result<Arc<dyn ExecutionPlan>>;
}

#[derive(Debug)]
pub struct NormalTableScanBuilder {
    table: TableRef,
}

impl NormalTableScanBuilder {
    pub fn new(table: TableRef) -> Self {
        Self { table }
    }
}

#[async_trait]
impl TableScanBuilder for NormalTableScanBuilder {
    async fn build(&self, request: ReadRequest) -> Result<Arc<dyn ExecutionPlan>> {
        let mut scan_table = ScanTable::new(self.table.clone(), request);
        scan_table.maybe_init_stream().await?;

        Ok(Arc::new(scan_table))
    }
}

/// An adapter to [TableProvider] with schema snapshot.
///
/// This adapter holds a schema snapshot of the table and always returns that
/// schema to caller.
#[derive(Debug)]
pub struct TableProviderAdapter<B> {
    table: TableRef,

    /// The schema of the table when this adapter is created, used as schema
    /// snapshot for read to avoid the reader sees different schema during
    /// query
    current_table_schema: Schema,

    /// Table scan builder
    builder: B,
}

impl<B: TableScanBuilder> TableProviderAdapter<B> {
    pub fn new(table: TableRef, builder: B) -> Self {
        // Take a snapshot of the schema
        let current_table_schema = table.schema();

        Self {
            table,
            current_table_schema,
            builder,
        }
    }

    pub fn as_table_ref(&self) -> &TableRef {
        &self.table
    }

    pub async fn scan_table(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let options = state.config_options().extensions.get::<HoraeDBOptions>();
        assert!(options.is_some());
        let options = options.unwrap();
        let request_id = RequestId::from(options.request_id.clone());
        let deadline = options
            .request_timeout
            .map(|n| Instant::now() + Duration::from_millis(n));
        let read_parallelism = state.config().target_partitions();
        let priority = options.priority;
        debug!(
            "TableProvider scan table, table:{}, request_id:{}, projection:{:?}, filters:{:?}, limit:{:?}, deadline:{:?}, parallelism:{}, priority:{:?}",
            self.table.name(),
            request_id,
            projection,
            filters,
            limit,
            deadline,
            read_parallelism,
            priority,
        );

        let mut need_reprojection = false;
        let all_projections = if let Some(proj) = projection {
            let mut original_projections = proj.clone();
            let projections_from_filter =
                collect_projection_from_expr(filters, &self.current_table_schema);
            for proj in projections_from_filter {
                if !original_projections.contains(&proj) {
                    original_projections.push(proj);
                    // If the projection from filters have columns not in the original projection,
                    // we need to add it to projection, and add a ProjectionExec plan to project the
                    // orignal columns. Eg:
                    // ```text
                    // select a from table where b > 1
                    // ```
                    // The original projection only contains a, but the filter has column b, so we
                    // need to query both a and b column from table but only
                    // output a column. More details can be found in:
                    // https://github.com/apache/arrow-datafusion/pull/9131#pullrequestreview-1865020767
                    need_reprojection = true;
                }
            }
            Some(original_projections)
        } else {
            None
        };
        let predicate = self.check_and_build_predicate_from_filters(filters);
        let projected_schema =
            ProjectedSchema::new(self.current_table_schema.clone(), all_projections).map_err(
                |e| {
                    DataFusionError::Internal(format!(
                        "Invalid projection, plan:{self:?}, projection:{projection:?}, err:{e:?}"
                    ))
                },
            )?;

        let projection_exprs = if need_reprojection {
            let original_projection = projection.unwrap();
            let exprs = (0..original_projection.len())
                .map(|i| {
                    let column = projected_schema.target_column_schema(i);
                    (
                        Arc::new(expressions::Column::new(&column.name, i))
                            as Arc<dyn PhysicalExpr>,
                        column.name.clone(),
                    )
                })
                .collect::<Vec<_>>();
            Some(exprs)
        } else {
            None
        };
        let opts = ReadOptions {
            deadline,
            read_parallelism,
            batch_size: state.config_options().execution.batch_size,
        };

        // TODO: metrics collector name should relate to detail scan impl?
        let request = ReadRequest {
            request_id,
            opts,
            projected_schema,
            predicate,
            metrics_collector: MetricsCollector::new(SCAN_TABLE_METRICS_COLLECTOR_NAME.to_string()),
            priority,
        };

        let scan = self.builder.build(request).await?;
        if let Some(expr) = projection_exprs {
            let plan = ProjectionExec::try_new(expr, scan)?;
            Ok(Arc::new(plan))
        } else {
            Ok(scan)
        }
    }

    fn check_and_build_predicate_from_filters(&self, filters: &[Expr]) -> PredicateRef {
        let pushdown_filters = filters
            .iter()
            .filter_map(|filter| {
                let filter_cols = visitor::find_columns_by_expr(filter);

                let support_pushdown = self
                    .table
                    .support_pushdown(&self.current_table_schema, &filter_cols);
                if support_pushdown {
                    Some(filter.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        PredicateBuilder::default()
            .add_pushdown_exprs(&pushdown_filters)
            .extract_time_range(&self.current_table_schema, filters)
            .build()
    }

    fn pushdown_inner(&self, filters: &[&Expr]) -> Vec<TableProviderFilterPushDown> {
        filters
            .iter()
            .map(|filter| {
                let filter_cols = visitor::find_columns_by_expr(filter);

                let support_pushdown = self
                    .table
                    .support_pushdown(&self.current_table_schema, &filter_cols);
                if support_pushdown {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect()
    }
}

#[async_trait]
impl<B: TableScanBuilder> TableProvider for TableProviderAdapter<B> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        // We use the `read_schema` as the schema of this `TableProvider`
        self.current_table_schema.clone().into_arrow_schema_ref()
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.scan_table(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(self.pushdown_inner(filters))
    }

    /// Get the type of this table for metadata/catalog purposes.
    fn table_type(&self) -> TableType {
        TableType::Base
    }
}

impl<B: TableScanBuilder> TableSource for TableProviderAdapter<B> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get a reference to the schema for this table
    fn schema(&self) -> SchemaRef {
        self.current_table_schema.clone().into_arrow_schema_ref()
    }

    /// Get the type of this table for metadata/catalog purposes.
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Tests whether the table provider can make use of a filter expression
    /// to optimize data retrieval.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(self.pushdown_inner(filters))
    }
}

/// Physical plan of scanning table.
pub struct ScanTable {
    table: TableRef,
    request: ReadRequest,
    stream_state: Mutex<ScanStreamState>,

    // FIXME: in origin partitioned table scan need to modify the parallelism when initializing
    // stream...
    parallelism: usize,
}

impl ScanTable {
    pub fn new(table: TableRef, request: ReadRequest) -> Self {
        let parallelism = request.opts.read_parallelism;
        Self {
            table,
            request,
            stream_state: Mutex::new(ScanStreamState::default()),
            parallelism,
        }
    }

    pub async fn maybe_init_stream(&mut self) -> Result<()> {
        let read_res = self.table.partitioned_read(self.request.clone()).await;

        let mut stream_state = self.stream_state.lock().unwrap();
        if stream_state.is_inited() {
            return Ok(());
        }
        stream_state.init(read_res);
        self.parallelism = stream_state.streams.len();

        Ok(())
    }
}

impl ExecutionPlan for ScanTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.request.projected_schema.to_projected_arrow_schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        // It represents how current node map the input streams to output ones.
        // However, we have no inputs here, so `UnknownPartitioning` is suitable.
        // In datafusion, always set it to `UnknownPartitioning` in the scan plan, for
        // example:  https://github.com/apache/arrow-datafusion/blob/cf152af6515f0808d840e1fe9c63b02802595826/datafusion/core/src/datasource/physical_plan/csv.rs#L175
        Partitioning::UnknownPartitioning(self.parallelism.max(1))
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(format!(
            "Children cannot be replaced in {self:?}"
        )))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<DfSendableRecordBatchStream> {
        let mut stream_state = self.stream_state.lock().unwrap();

        if !stream_state.is_inited() {
            return Err(DataFusionError::Internal(
                "Scan stream can't be executed before inited".to_string(),
            ));
        }

        let stream = stream_state.take_stream(partition)?;

        Ok(Box::pin(ToDfStream(stream)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        let mut metric_set = MetricsSet::new();

        let mut format_visitor = FormatCollectorVisitor::default();
        self.request.metrics_collector.visit(&mut format_visitor);
        let metrics_desc = format_visitor.into_string();
        let pushdown_filters = &self.request.predicate;
        metric_set.push(Arc::new(Metric::new(
            MetricValue::Count {
                name: format!("\n{pushdown_filters:?}\n{metrics_desc}").into(),
                count: Count::new(),
            },
            None,
        )));

        Some(metric_set)
    }

    fn statistics(
        &self,
    ) -> std::result::Result<datafusion::common::Statistics, datafusion::error::DataFusionError>
    {
        // TODO(yingwen): Implement this
        Ok(Statistics::new_unknown(&self.schema()))
    }
}

impl DisplayAs for ScanTable {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ScanTable: table={}, parallelism={}, priority={:?}, partition_count={:?}",
            self.table.name(),
            self.request.opts.read_parallelism,
            self.request.priority,
            self.output_partitioning()
        )
    }
}

impl fmt::Debug for ScanTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScanTable")
            .field("projected_schema", &self.request.projected_schema)
            .field("table", &self.table.name())
            .field("read_parallelism", &self.request.opts.read_parallelism)
            .field("predicate", &self.request.predicate)
            .finish()
    }
}

fn collect_projection_from_expr(exprs: &[Expr], schema: &Schema) -> HashSet<usize> {
    let mut projections = HashSet::new();
    exprs.iter().for_each(|expr| {
        for col_name in visitor::find_columns_by_expr(expr) {
            if let Some(idx) = schema.index_of(&col_name) {
                projections.insert(idx);
            }
        }
    });

    projections
}
