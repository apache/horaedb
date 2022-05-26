// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Datafusion `TableProvider` adapter

use std::{any::Any, fmt, sync::Arc};

use arrow_deps::{
    arrow::datatypes::SchemaRef,
    datafusion::{
        datasource::datasource::{TableProvider, TableProviderFilterPushDown},
        error::{DataFusionError, Result},
        execution::runtime_env::RuntimeEnv,
        logical_plan::Expr,
        physical_plan::{
            DisplayFormatType, ExecutionPlan, Partitioning,
            SendableRecordBatchStream as DfSendableRecordBatchStream, Statistics,
        },
    },
};
use async_trait::async_trait;
use common_types::{projected_schema::ProjectedSchema, request_id::RequestId, schema::Schema};
use log::debug;
use tokio::sync::Mutex;

use crate::{
    predicate::{PredicateBuilder, PredicateRef},
    stream::{SendableRecordBatchStream, ToDfStream},
    table::{self, ReadOptions, ReadOrder, ReadRequest, TableRef},
};

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
    request_id: RequestId,
    read_parallelism: usize,
}

impl TableProviderAdapter {
    pub fn new(table: TableRef, request_id: RequestId, read_parallelism: usize) -> Self {
        // Take a snapshot of the schema
        let read_schema = table.schema();

        Self {
            table,
            read_schema,
            request_id,
            read_parallelism,
        }
    }

    pub fn as_table_ref(&self) -> &TableRef {
        &self.table
    }

    pub fn scan_table(
        &self,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        read_order: ReadOrder,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        debug!(
            "scan table, table:{}, request_id:{}, projection:{:?}, filters:{:?}, limit:{:?}, read_order:{:?}",
            self.table.name(),
            self.request_id,
            projection,
            filters,
            limit,
            read_order,
        );

        // Forbid the parallel reading if the data order is required.
        let read_parallelism = if read_order.is_in_order() {
            1
        } else {
            self.read_parallelism
        };

        let predicate = self.predicate_from_filters(filters);
        Ok(Arc::new(ScanTable {
            projected_schema: ProjectedSchema::new(self.read_schema.clone(), projection.clone())
                .map_err(|e| {
                    DataFusionError::Internal(format!(
                        "Invalid projection, plan:{:?}, projection:{:?}, err:{:?}",
                        self, projection, e
                    ))
                })?,
            table: self.table.clone(),
            request_id: self.request_id,
            read_order,
            read_parallelism,
            predicate,
            stream_state: Mutex::new(ScanStreamState::default()),
        }))
    }

    fn predicate_from_filters(&self, filters: &[Expr]) -> PredicateRef {
        PredicateBuilder::default()
            .add_pushdown_exprs(filters)
            .set_time_range(&self.read_schema, filters)
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
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.scan_table(projection, filters, limit, ReadOrder::None)
    }

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

    stream_state: Mutex<ScanStreamState>,
}

impl ScanTable {
    async fn maybe_init_stream(&self, runtime: Arc<RuntimeEnv>) -> Result<()> {
        let mut stream_state = self.stream_state.lock().await;
        if stream_state.inited {
            return Ok(());
        }

        let req = ReadRequest {
            request_id: self.request_id,
            opts: ReadOptions {
                batch_size: runtime.batch_size(),
                read_parallelism: self.read_parallelism,
            },
            projected_schema: self.projected_schema.clone(),
            predicate: self.predicate.clone(),
            order: self.read_order,
        };

        let read_res = self.table.partitioned_read(req).await;
        match read_res {
            Ok(partitioned_streams) => {
                assert_eq!(self.read_parallelism, partitioned_streams.streams.len());
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

#[async_trait]
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

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    fn with_new_children(&self, _: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(format!(
            "Children cannot be replaced in {:?}",
            self
        )))
    }

    async fn execute(
        &self,
        partition: usize,
        runtime: Arc<RuntimeEnv>,
    ) -> Result<DfSendableRecordBatchStream> {
        self.maybe_init_stream(runtime).await?;

        let mut stream_state = self.stream_state.lock().await;
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
