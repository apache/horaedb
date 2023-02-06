// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

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
    config::OPT_BATCH_SIZE,
    datasource::datasource::{TableProvider, TableProviderFilterPushDown},
    error::{DataFusionError, Result},
    execution::context::{SessionState, TaskContext},
    logical_plan::Expr,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        DisplayFormatType, ExecutionPlan, Partitioning,
        SendableRecordBatchStream as DfSendableRecordBatchStream, Statistics,
    },
    scalar::ScalarValue,
};
use datafusion_expr::{TableSource, TableType};
use df_operator::visitor;
use log::debug;

use crate::{
    predicate::{PredicateBuilder, PredicateRef},
    stream::{SendableRecordBatchStream, ToDfStream},
    table::{self, ReadOptions, ReadOrder, ReadRequest, TableRef},
};

// Config keys set in Datafusion's SessionConfig
pub const CERESDB_REQUEST_TIMEOUT: &str = "ceresdb_request_timeout";
pub const CERESDB_REQUEST_ID: &str = "ceresdb_request_id";

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
    read_parallelism: usize,
}

impl TableProviderAdapter {
    pub fn new(table: TableRef, read_parallelism: usize) -> Self {
        // Take a snapshot of the schema
        let read_schema = table.schema();

        Self {
            table,
            read_schema,
            read_parallelism,
        }
    }

    pub fn as_table_ref(&self) -> &TableRef {
        &self.table
    }

    pub async fn scan_table(
        &self,
        state: &SessionState,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        read_order: ReadOrder,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let request_id = RequestId::from(state.config.config_options().get_u64(CERESDB_REQUEST_ID));
        let deadline = match state.config.config_options().get(CERESDB_REQUEST_TIMEOUT) {
            Some(ScalarValue::UInt64(Some(n))) => Some(Instant::now() + Duration::from_millis(n)),
            _ => None,
        };
        debug!(
            "scan table, table:{}, request_id:{}, projection:{:?}, filters:{:?}, limit:{:?}, read_order:{:?}, deadline:{:?}",
            self.table.name(),
            request_id,
            projection,
            filters,
            limit,
            read_order,
            deadline,
        );

        // Forbid the parallel reading if the data order is required.
        let read_parallelism = if read_order.is_in_order() && self.table.partition_info().is_none()
        {
            1
        } else {
            self.read_parallelism
        };

        let predicate = self.check_and_build_predicate_from_filters(filters);
        let mut scan_table = ScanTable {
            projected_schema: ProjectedSchema::new(self.read_schema.clone(), projection.clone())
                .map_err(|e| {
                    DataFusionError::Internal(format!(
                        "Invalid projection, plan:{:?}, projection:{:?}, err:{:?}",
                        self, projection, e
                    ))
                })?,
            table: self.table.clone(),
            request_id,
            read_order,
            read_parallelism,
            predicate,
            deadline,
            stream_state: Mutex::new(ScanStreamState::default()),
        };
        scan_table.maybe_init_stream(state).await?;

        Ok(Arc::new(scan_table))
    }

    fn check_and_build_predicate_from_filters(&self, filters: &[Expr]) -> PredicateRef {
        // 1.Only filters of primary key or timestamp key can be pushed down.
        // Build key set.
        let mut key_set = HashSet::new();
        // Timestamp key should be always picked.
        key_set.insert(self.read_schema.timestamp_name());
        if self.read_schema.tsid_column().is_some() {
            // When tsid exists, that means default primary key (tsid, timestamp) is used.
            // So, all filters of tag columns(tsid is the hash result of all tags) can be pushed down.
            for column in self.read_schema.columns() {
                if column.is_tag {
                    key_set.insert(&column.name);
                }
            }
        } else {
            // When tsid does not exist, that means user defined primary key is used.
            // So, only filters of primary key can be pushed down.
            for primary_idx in self.read_schema.primary_key_indexes() {
                let primary_column = self.read_schema.column(*primary_idx);
                key_set.insert(&primary_column.name);
            }
        }

        let push_down_filters = filters
            .iter()
            .filter_map(|filter| {
                if self.should_push_down(filter, &key_set) {
                    Some(filter.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        // 2.Build predicates from push down filters.
        PredicateBuilder::default()
            .add_pushdown_exprs(&push_down_filters)
            .extract_time_range(&self.read_schema, &push_down_filters)
            .build()
    }

    fn should_push_down(&self, filter: &Expr, key_set: &HashSet<&str>) -> bool {
        let columns = visitor::find_columns_by_expr(filter);
        let mut should_push_down = true;
        for column in columns {
            // Once found a column not primary key or timestamp key in `filter`,
            // the `filter` will be marked as unable to push down.
            if !key_set.contains(column.as_str()) {
                should_push_down = false;
                break;
            }
        }

        should_push_down
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
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.scan_table(state, projection, filters, limit, ReadOrder::None)
            .await
    }

    fn supports_filter_pushdown(&self, _filter: &Expr) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
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
        Ok(TableProviderFilterPushDown::Inexact)
    }
}

#[derive(Default)]
struct ScanStreamState {
    inited: bool,
    err: Option<table::Error>,
    streams: Vec<Option<SendableRecordBatchStream>>,
}

impl ScanStreamState {
    fn take_stream(&mut self, index: usize) -> Result<SendableRecordBatchStream> {
        if let Some(e) = &self.err {
            return Err(DataFusionError::Execution(format!(
                "Failed to read table, partition:{}, err:{}",
                index, e
            )));
        }

        // TODO(yingwen): Return an empty stream if index is out of bound.
        self.streams[index].take().ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Read partition multiple times is not supported, partition:{}",
                index
            ))
        })
    }
}

/// Physical plan of scanning table.
struct ScanTable {
    projected_schema: ProjectedSchema,
    table: TableRef,
    request_id: RequestId,
    read_order: ReadOrder,
    read_parallelism: usize,
    predicate: PredicateRef,
    deadline: Option<Instant>,

    stream_state: Mutex<ScanStreamState>,
}

impl ScanTable {
    async fn maybe_init_stream(&mut self, state: &SessionState) -> Result<()> {
        let req = ReadRequest {
            request_id: self.request_id,
            opts: ReadOptions {
                batch_size: state.config.config_options.get_u64(OPT_BATCH_SIZE) as usize,
                read_parallelism: self.read_parallelism,
                deadline: self.deadline,
            },
            projected_schema: self.projected_schema.clone(),
            predicate: self.predicate.clone(),
            order: self.read_order,
        };

        let read_res = self.table.partitioned_read(req).await;

        let mut stream_state = self.stream_state.lock().unwrap();
        if stream_state.inited {
            return Ok(());
        }

        match read_res {
            Ok(partitioned_streams) => {
                self.read_parallelism = partitioned_streams.streams.len();
                stream_state.streams = partitioned_streams.streams.into_iter().map(Some).collect();
            }
            Err(e) => {
                stream_state.err = Some(e);
            }
        }
        stream_state.inited = true;

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
            "Children cannot be replaced in {:?}",
            self
        )))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<DfSendableRecordBatchStream> {
        let mut stream_state = self.stream_state.lock().unwrap();
        let stream = stream_state.take_stream(partition)?;

        Ok(Box::pin(ToDfStream(stream)))
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ScanTable: table={}, parallelism={}, order={:?}, ",
            self.table.name(),
            self.read_parallelism,
            self.read_order,
        )
    }

    fn statistics(&self) -> Statistics {
        // TODO(yingwen): Implement this
        Statistics::default()
    }
}

impl fmt::Debug for ScanTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScanTable")
            .field("projected_schema", &self.projected_schema)
            .field("table", &self.table.name())
            .field("read_order", &self.read_order)
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
    use datafusion::scalar::ScalarValue;
    use datafusion_expr::{col, Expr};

    use super::TableProviderAdapter;
    use crate::{memory::MemoryTable, table::TableId};

    fn build_user_defined_primary_key_schema() -> Schema {
        Builder::new()
            .auto_increment_column_id(true)
            .add_key_column(
                column_schema::Builder::new("key1".to_string(), DatumKind::String)
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
                column_schema::Builder::new("timestamp".to_string(), DatumKind::Timestamp)
                    .build()
                    .expect("should succeed build column schema"),
            )
            .unwrap()
            .add_key_column(
                column_schema::Builder::new("tsid".to_string(), DatumKind::UInt64)
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
        let filter2 = col("key1").eq(Expr::Literal(ScalarValue::Utf8(Some("10086".to_string()))));
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
        let provider = TableProviderAdapter::new(Arc::new(table), 1);
        let predicate = provider.check_and_build_predicate_from_filters(&test_filters);

        let expected_filters = vec![test_filters[0].clone(), test_filters[1].clone()];
        assert_eq!(predicate.exprs(), &expected_filters);
    }

    #[test]
    pub fn test_push_down_in_default_primary_key_case() {
        let test_filters = build_filters();
        let test_filters = vec![
            test_filters[0].clone(),
            test_filters[2].clone(),
            test_filters[3].clone(),
        ];
        let default_pk_schema = build_default_primary_key_schema();

        let table = MemoryTable::new(
            "test_table".to_string(),
            TableId::new(0),
            default_pk_schema,
            "memory".to_string(),
        );
        let provider = TableProviderAdapter::new(Arc::new(table), 1);
        let predicate = provider.check_and_build_predicate_from_filters(&test_filters);

        let expected_filters = vec![test_filters[0].clone(), test_filters[1].clone()];
        assert_eq!(predicate.exprs(), &expected_filters);
    }
}
