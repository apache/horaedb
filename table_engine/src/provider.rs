// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

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
    datasource::datasource::{TableProvider, TableProviderFilterPushDown},
    error::{DataFusionError, Result},
    execution::context::{SessionState, TaskContext},
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        DisplayFormatType, ExecutionPlan, Partitioning,
        SendableRecordBatchStream as DfSendableRecordBatchStream, Statistics,
    },
};
use datafusion_expr::{Expr, TableSource, TableType};
use log::debug;

use crate::{
    predicate::{PredicateBuilder, PredicateRef},
    stream::{SendableRecordBatchStream, ToDfStream},
    table::{self, ReadOptions, ReadOrder, ReadRequest, TableRef},
};

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
                        format!("could not parse request_id, input:{}, err:{:?}", value, e).into(),
                    )
                })?
            }
            "request_timeout" => {
                self.request_timeout = Some(value.parse::<u64>().map_err(|e| {
                    DataFusionError::External(
                        format!(
                            "could not parse request_timeout, input:{}, err:{:?}",
                            value, e
                        )
                        .into(),
                    )
                })?)
            }
            _ => Err(DataFusionError::External(
                format!("could not find key, key:{}", key).into(),
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
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        read_order: ReadOrder,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let ceresdb_options = state.config_options().extensions.get::<CeresdbOptions>();
        let (request_id, deadline) = if let Some(options) = ceresdb_options {
            let request_id = RequestId::from(options.request_id);
            let deadline = options
                .request_timeout
                .map(|n| Instant::now() + Duration::from_millis(n));
            (request_id, deadline)
        } else {
            (RequestId::from(0), None)
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

        let predicate = self.predicate_from_filters(filters);
        let mut scan_table = ScanTable {
            projected_schema: ProjectedSchema::new(self.read_schema.clone(), projection.cloned())
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

    fn predicate_from_filters(&self, filters: &[Expr]) -> PredicateRef {
        PredicateBuilder::default()
            .add_pushdown_exprs(filters)
            .extract_time_range(&self.read_schema, filters)
            .build()
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
                batch_size: state.config_options().execution.batch_size,
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
