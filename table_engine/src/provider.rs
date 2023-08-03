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

//! Datafusion `TableProvider` adapter

use std::{
    any::Any,
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
        metrics::{Count, MetricValue, MetricsSet},
        DisplayAs, DisplayFormatType, ExecutionPlan, Metric, Partitioning,
        SendableRecordBatchStream as DfSendableRecordBatchStream, Statistics,
    },
};
use df_operator::visitor;
use log::debug;
use trace_metric::{collector::FormatCollectorVisitor, MetricsCollector};

use crate::{
    predicate::{PredicateBuilder, PredicateRef},
    stream::{ScanStreamState, ToDfStream},
    table::{ReadOptions, ReadRequest, TableRef},
};

const SCAN_TABLE_METRICS_COLLECTOR_NAME: &str = "scan_table";

#[derive(Clone, Debug)]
pub struct CeresdbOptions {
    pub request_id: u64,
    pub request_timeout: Option<u64>,
}

impl ConfigExtension for CeresdbOptions {
    const PREFIX: &'static str = "ceresdb";
}

impl ExtensionOptions for CeresdbOptions {
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
            "request_id" => {
                self.request_id = value.parse::<u64>().map_err(|e| {
                    DataFusionError::External(
                        format!("could not parse request_id, input:{value}, err:{e:?}").into(),
                    )
                })?
            }
            "request_timeout" => {
                self.request_timeout = Some(value.parse::<u64>().map_err(|e| {
                    DataFusionError::External(
                        format!("could not parse request_timeout, input:{value}, err:{e:?}").into(),
                    )
                })?)
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
                key: "request_id".to_string(),
                value: Some(self.request_id.to_string()),
                description: "",
            },
            ConfigEntry {
                key: "request_timeout".to_string(),
                value: self.request_timeout.map(|v| v.to_string()),
                description: "",
            },
        ]
    }
}

/// An adapter to [TableProvider] with schema snapshot.
///
/// This adapter holds a schema snapshot of the table and always returns that
/// schema to caller.
#[derive(Debug)]
pub struct TableProviderAdapter {
    table: TableRef,
    /// The schema of the table when this adapter is created, used as schema
    /// snapshot for read to avoid the reader sees different schema during
    /// query
    read_schema: Schema,
}

impl TableProviderAdapter {
    pub fn new(table: TableRef) -> Self {
        // Take a snapshot of the schema
        let read_schema = table.schema();

        Self { table, read_schema }
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
        let ceresdb_options = state.config_options().extensions.get::<CeresdbOptions>();
        assert!(ceresdb_options.is_some());
        let ceresdb_options = ceresdb_options.unwrap();
        let request_id = RequestId::from(ceresdb_options.request_id);
        let deadline = ceresdb_options
            .request_timeout
            .map(|n| Instant::now() + Duration::from_millis(n));
        let read_parallelism = state.config().target_partitions();
        debug!(
            "scan table, table:{}, request_id:{}, projection:{:?}, filters:{:?}, limit:{:?}, deadline:{:?}, parallelism:{}",
            self.table.name(),
            request_id,
            projection,
            filters,
            limit,
            deadline,
            read_parallelism,
        );

        let predicate = self.check_and_build_predicate_from_filters(filters);
        let mut scan_table = ScanTable {
            projected_schema: ProjectedSchema::new(self.read_schema.clone(), projection.cloned())
                .map_err(|e| {
                DataFusionError::Internal(format!(
                    "Invalid projection, plan:{self:?}, projection:{projection:?}, err:{e:?}"
                ))
            })?,
            table: self.table.clone(),
            request_id,
            read_parallelism,
            predicate,
            deadline,
            stream_state: Mutex::new(ScanStreamState::default()),
            metrics_collector: MetricsCollector::new(SCAN_TABLE_METRICS_COLLECTOR_NAME.to_string()),
        };
        scan_table.maybe_init_stream(state).await?;

        Ok(Arc::new(scan_table))
    }

    fn check_and_build_predicate_from_filters(&self, filters: &[Expr]) -> PredicateRef {
        let unique_keys = self.read_schema.unique_keys();

        let push_down_filters = filters
            .iter()
            .filter_map(|filter| {
                if Self::only_filter_unique_key_columns(filter, &unique_keys) {
                    Some(filter.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        PredicateBuilder::default()
            .add_pushdown_exprs(&push_down_filters)
            .extract_time_range(&self.read_schema, &push_down_filters)
            .build()
    }

    fn only_filter_unique_key_columns(filter: &Expr, unique_keys: &[&str]) -> bool {
        let filter_cols = visitor::find_columns_by_expr(filter);
        for filter_col in filter_cols {
            // If a column which is not part of the unique key occurred in `filter`, the
            // `filter` shouldn't be pushed down.
            if !unique_keys.contains(&filter_col.as_str()) {
                return false;
            }
        }
        true
    }
}

#[async_trait]
impl TableProvider for TableProviderAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        // We use the `read_schema` as the schema of this `TableProvider`
        self.read_schema.clone().into_arrow_schema_ref()
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

    fn supports_filter_pushdown(&self, _filter: &Expr) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Exact)
    }

    /// Get the type of this table for metadata/catalog purposes.
    fn table_type(&self) -> TableType {
        TableType::Base
    }
}

impl TableSource for TableProviderAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get a reference to the schema for this table
    fn schema(&self) -> SchemaRef {
        self.read_schema.clone().into_arrow_schema_ref()
    }

    /// Get the type of this table for metadata/catalog purposes.
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Tests whether the table provider can make use of a filter expression
    /// to optimize data retrieval.
    fn supports_filter_pushdown(&self, _filter: &Expr) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Exact)
    }
}

/// Physical plan of scanning table.
struct ScanTable {
    projected_schema: ProjectedSchema,
    table: TableRef,
    request_id: RequestId,
    read_parallelism: usize,
    predicate: PredicateRef,
    deadline: Option<Instant>,
    metrics_collector: MetricsCollector,

    stream_state: Mutex<ScanStreamState>,
}

impl ScanTable {
    async fn maybe_init_stream(&mut self, state: &SessionState) -> Result<()> {
        let req = ReadRequest {
            request_id: self.request_id,
            opts: ReadOptions {
                batch_size: state.config_options().execution.batch_size,
                read_parallelism: self.read_parallelism,
                deadline: self.deadline,
            },
            projected_schema: self.projected_schema.clone(),
            predicate: self.predicate.clone(),
            metrics_collector: self.metrics_collector.clone(),
        };

        let read_res = self.table.partitioned_read(req).await;

        let mut stream_state = self.stream_state.lock().unwrap();

        if stream_state.is_inited() {
            return Ok(());
        }

        // FIXME: in new version distributed query, we should remove this.
        if let Ok(partitioned_streams) = &read_res {
            self.read_parallelism = partitioned_streams.streams.len();
        }

        stream_state.init(read_res);

        Ok(())
    }
}

impl ExecutionPlan for ScanTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.to_projected_arrow_schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::RoundRobinBatch(self.read_parallelism)
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
        let mut format_visitor = FormatCollectorVisitor::default();
        self.metrics_collector.visit(&mut format_visitor);
        let metrics_desc = format_visitor.into_string();

        let metric_value = MetricValue::Count {
            name: format!("\n{metrics_desc}").into(),
            count: Count::new(),
        };
        let metric = Metric::new(metric_value, None);
        let mut metric_set = MetricsSet::new();
        metric_set.push(Arc::new(metric));

        Some(metric_set)
    }

    fn statistics(&self) -> Statistics {
        // TODO(yingwen): Implement this
        Statistics::default()
    }
}

impl DisplayAs for ScanTable {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ScanTable: table={}, parallelism={}",
            self.table.name(),
            self.read_parallelism,
        )
    }
}

impl fmt::Debug for ScanTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScanTable")
            .field("projected_schema", &self.projected_schema)
            .field("table", &self.table.name())
            .field("read_parallelism", &self.read_parallelism)
            .field("predicate", &self.predicate)
            .finish()
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use common_types::{
        column_schema,
        datum::DatumKind,
        schema::{Builder, Schema},
    };
    use datafusion::{
        logical_expr::{col, Expr},
        scalar::ScalarValue,
    };

    use super::*;
    use crate::{memory::MemoryTable, table::TableId};

    fn build_user_defined_primary_key_schema() -> Schema {
        Builder::new()
            .auto_increment_column_id(true)
            .add_key_column(
                column_schema::Builder::new("user_define1".to_string(), DatumKind::String)
                    .build()
                    .expect("should succeed build column schema"),
            )
            .unwrap()
            .add_key_column(
                column_schema::Builder::new("timestamp".to_string(), DatumKind::Timestamp)
                    .build()
                    .expect("should succeed build column schema"),
            )
            .unwrap()
            .add_normal_column(
                column_schema::Builder::new("field1".to_string(), DatumKind::String)
                    .is_tag(true)
                    .build()
                    .expect("should succeed build column schema"),
            )
            .unwrap()
            .add_normal_column(
                column_schema::Builder::new("field2".to_string(), DatumKind::Double)
                    .build()
                    .expect("should succeed build column schema"),
            )
            .unwrap()
            .build()
            .unwrap()
    }

    fn build_default_primary_key_schema() -> Schema {
        Builder::new()
            .auto_increment_column_id(true)
            .add_key_column(
                column_schema::Builder::new("tsid".to_string(), DatumKind::UInt64)
                    .build()
                    .expect("should succeed build column schema"),
            )
            .unwrap()
            .add_key_column(
                column_schema::Builder::new("timestamp".to_string(), DatumKind::Timestamp)
                    .build()
                    .expect("should succeed build column schema"),
            )
            .unwrap()
            .add_normal_column(
                column_schema::Builder::new("user_define1".to_string(), DatumKind::String)
                    .build()
                    .expect("should succeed build column schema"),
            )
            .unwrap()
            .add_normal_column(
                column_schema::Builder::new("field1".to_string(), DatumKind::String)
                    .is_tag(true)
                    .build()
                    .expect("should succeed build column schema"),
            )
            .unwrap()
            .add_normal_column(
                column_schema::Builder::new("field2".to_string(), DatumKind::Double)
                    .build()
                    .expect("should succeed build column schema"),
            )
            .unwrap()
            .build()
            .unwrap()
    }

    fn build_filters() -> Vec<Expr> {
        let filter1 = col("timestamp").lt(Expr::Literal(ScalarValue::UInt64(Some(10086))));
        let filter2 =
            col("user_define1").eq(Expr::Literal(ScalarValue::Utf8(Some("10086".to_string()))));
        let filter3 = col("field1").eq(Expr::Literal(ScalarValue::Utf8(Some("10087".to_string()))));
        let filter4 = col("field2").eq(Expr::Literal(ScalarValue::Float64(Some(10088.0))));

        vec![filter1, filter2, filter3, filter4]
    }

    #[test]
    pub fn test_push_down_in_user_defined_primary_key_case() {
        let test_filters = build_filters();
        let user_defined_pk_schema = build_user_defined_primary_key_schema();

        let table = MemoryTable::new(
            "test_table".to_string(),
            TableId::new(0),
            user_defined_pk_schema,
            "memory".to_string(),
        );
        let provider = TableProviderAdapter::new(Arc::new(table));
        let predicate = provider.check_and_build_predicate_from_filters(&test_filters);

        let expected_filters = vec![test_filters[0].clone(), test_filters[1].clone()];
        assert_eq!(predicate.exprs(), &expected_filters);
    }

    #[test]
    pub fn test_push_down_in_default_primary_key_case() {
        let test_filters = build_filters();
        let default_pk_schema = build_default_primary_key_schema();

        let table = MemoryTable::new(
            "test_table".to_string(),
            TableId::new(0),
            default_pk_schema,
            "memory".to_string(),
        );
        let provider = TableProviderAdapter::new(Arc::new(table));
        let predicate = provider.check_and_build_predicate_from_filters(&test_filters);

        let expected_filters = vec![test_filters[0].clone(), test_filters[2].clone()];
        assert_eq!(predicate.exprs(), &expected_filters);
    }
}
