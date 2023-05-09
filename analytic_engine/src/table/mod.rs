// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Table implementation

use std::{collections::HashMap, fmt, sync::Mutex};

use async_trait::async_trait;
use common_types::{
    row::{Row, RowGroupBuilder},
    schema::Schema,
    time::TimeRange,
};
use common_util::error::BoxError;
use datafusion::{common::Column, logical_expr::Expr};
use futures::TryStreamExt;
use log::warn;
use snafu::{ensure, OptionExt, ResultExt};
use table_engine::{
    partition::PartitionInfo,
    predicate::PredicateBuilder,
    stream::{PartitionedStreams, SendableRecordBatchStream},
    table::{
        AlterOptions, AlterSchema, AlterSchemaRequest, Compact, Flush, FlushRequest, Get,
        GetInvalidPrimaryKey, GetNullPrimaryKey, GetRequest, MergeWrite, ReadOptions, ReadOrder,
        ReadRequest, Result, Scan, Table, TableId, TableStats, WaitForPendingWrites, Write,
        WriteRequest,
    },
};
use tokio::sync::oneshot::{self, Receiver, Sender};
use trace_metric::MetricsCollector;

use self::data::TableDataRef;
use crate::{
    instance::{alter::Alterer, write::Writer, InstanceRef},
    space::{SpaceAndTable, SpaceId},
};

pub mod data;
pub mod metrics;
pub mod sst_util;
pub mod version;
pub mod version_edit;

const GET_METRICS_COLLECTOR_NAME: &str = "get";
const DEFAULT_PENDING_WRITE_REQUESTS: usize = 32;
const DEFAULT_MAX_PENDING_ROWS: usize = 10000;

/// Table trait implementation
pub struct TableImpl {
    space_table: SpaceAndTable,
    /// Instance
    instance: InstanceRef,
    /// Engine type
    engine_type: String,

    space_id: SpaceId,
    table_id: TableId,

    /// Holds a strong reference to prevent the underlying table from being
    /// dropped when this handle exist.
    table_data: TableDataRef,

    /// Buffer for written rows.
    pending_writes: Mutex<PendingWriteQueue>,
}

impl TableImpl {
    pub fn new(
        instance: InstanceRef,
        engine_type: String,
        space_id: SpaceId,
        table_id: TableId,
        table_data: TableDataRef,
        space_table: SpaceAndTable,
    ) -> Self {
        Self {
            space_table,
            instance,
            engine_type,
            space_id,
            table_id,
            table_data,
            pending_writes: Mutex::new(PendingWriteQueue::new(DEFAULT_MAX_PENDING_ROWS)),
        }
    }
}

impl fmt::Debug for TableImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableImpl")
            .field("space_id", &self.space_id)
            .field("table_id", &self.table_id)
            .finish()
    }
}

struct PendingWriteQueue {
    max_rows: usize,
    pending_writes: PendingWrites,
}

#[derive(Default)]
struct PendingWrites {
    writes: Vec<WriteRequest>,
    notifiers: Vec<Sender<Result<()>>>,
    num_rows: usize,
}

impl PendingWrites {
    /// Try to push the request into the pending queue.
    ///
    /// Original request will be returned if the schema is different.
    fn try_push(&mut self, request: WriteRequest) -> QueueResult {
        if !self.is_same_schema(request.row_group.schema()) {
            return QueueResult::Reject(request);
        }

        // For the first pending writes, don't provide the receiver because it should do
        // the write for the pending writes and no need to wait for the notification.
        let res = if self.is_empty() {
            self.writes.reserve(DEFAULT_PENDING_WRITE_REQUESTS);
            self.notifiers.reserve(DEFAULT_PENDING_WRITE_REQUESTS);
            QueueResult::First
        } else {
            let (tx, rx) = oneshot::channel();
            self.notifiers.push(tx);
            QueueResult::Waiter(rx)
        };

        self.num_rows += request.row_group.num_rows();
        self.writes.push(request);

        res
    }

    /// Check if the schema of the request is the same as the schema of the
    /// pending write requests.
    ///
    /// Return true if the pending write requests is empty.
    fn is_same_schema(&self, schema: &Schema) -> bool {
        if self.is_empty() {
            return true;
        }

        let request = &self.writes[0];
        schema.version() == request.row_group.schema().version()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.writes.is_empty()
    }
}

enum QueueResult {
    Reject(WriteRequest),
    First,
    Waiter(Receiver<Result<()>>),
}

impl PendingWriteQueue {
    fn new(max_rows: usize) -> Self {
        Self {
            max_rows,
            pending_writes: PendingWrites::default(),
        }
    }

    /// Try to push the request into the queue.
    ///
    /// If the queue is full or the schema is different, return the request
    /// back. Otherwise, return a receiver to let the caller wait for the write
    /// result.
    fn try_push(&mut self, request: WriteRequest) -> QueueResult {
        if self.is_full() {
            return QueueResult::Reject(request);
        }

        self.pending_writes.try_push(request)
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.pending_writes.num_rows >= self.max_rows
    }

    /// Clear the pending writes and reset the number of rows.
    fn reset(&mut self) -> PendingWrites {
        std::mem::take(&mut self.pending_writes)
    }
}

/// Merge the pending write requests into a same one.
///
/// The schema of all the pending write requests should be the same.
/// REQUIRES: the `pending_writes` is required non-empty.
fn merge_pending_write_requests(
    mut pending_writes: Vec<WriteRequest>,
    num_pending_rows: usize,
) -> WriteRequest {
    assert!(!pending_writes.is_empty());

    let mut last_req = pending_writes.pop().unwrap();
    let last_rows = last_req.row_group.take_rows();
    let schema = last_req.row_group.into_schema();
    let mut row_group_builder = RowGroupBuilder::with_capacity(schema, num_pending_rows);

    for mut pending_req in pending_writes {
        let rows = pending_req.row_group.take_rows();
        for row in rows {
            row_group_builder.push_checked_row(row)
        }
    }
    for row in last_rows {
        row_group_builder.push_checked_row(row);
    }
    let row_group = row_group_builder.build();
    WriteRequest { row_group }
}

impl TableImpl {
    /// Perform table writes.
    ///
    /// If the write is blocked, try to push the request into the pending queue,
    /// and merge and write them later.
    async fn write_with_pending_queue(&self, request: WriteRequest) -> Result<usize> {
        let num_rows = request.row_group.num_rows();

        let lock_res = self.table_data.serial_exec.try_lock();
        let (request, mut serial_exec, notifiers) = if let Ok(serial_exec) = lock_res {
            (request, serial_exec, vec![])
        } else {
            // Failed to acquire the serial_exec, put the request into the
            // pending queue.
            let queue_res = {
                let mut pending_queue = self.pending_writes.lock().unwrap();
                pending_queue.try_push(request)
            };
            match queue_res {
                QueueResult::First => {
                    // This is the first request in the queue, and we should
                    // take responsibilities for merging and writing the
                    // requests in the queue.
                    let serial_exec = self.table_data.serial_exec.lock().await;
                    // The `serial_exec` is acquired, let's merge the pending requests and write
                    // them all.
                    let pending_writes = {
                        let mut pending_queue = self.pending_writes.lock().unwrap();
                        pending_queue.reset()
                    };
                    assert!(
                        !pending_writes.is_empty(),
                        "The pending writes should contain at least the one just pushed."
                    );
                    let merged_write_request = merge_pending_write_requests(
                        pending_writes.writes,
                        pending_writes.num_rows,
                    );
                    (merged_write_request, serial_exec, pending_writes.notifiers)
                }
                QueueResult::Waiter(rx) => {
                    // The request is successfully pushed into the queue, and just wait for the
                    // write result.
                    match rx.await {
                        Ok(res) => {
                            res.box_err().context(Write { table: self.name() })?;
                            return Ok(num_rows);
                        }
                        Err(_) => return WaitForPendingWrites { table: self.name() }.fail(),
                    }
                }
                QueueResult::Reject(request) => {
                    // The queue is full, return error.
                    warn!("Pending_writes queue is full, table:{}", self.name());
                    let serial_exec = self.table_data.serial_exec.lock().await;
                    (request, serial_exec, vec![])
                }
            }
        };

        let mut writer = Writer::new(
            self.instance.clone(),
            self.space_table.clone(),
            &mut serial_exec,
        );
        let write_res = writer
            .write(request)
            .await
            .box_err()
            .context(Write { table: self.name() });

        // There is no waiter for pending writes, return the write result.
        if notifiers.is_empty() {
            return write_res;
        }

        // Notify the waiters for the pending writes.
        match write_res {
            Ok(_) => {
                for notifier in notifiers {
                    if notifier.send(Ok(())).is_err() {
                        warn!(
                            "Failed to notify the ok result of pending writes, table:{}",
                            self.name()
                        );
                    }
                }
                Ok(num_rows)
            }
            Err(e) => {
                let err_msg = format!("Failed to do merge write, err:{e}");
                for notifier in notifiers {
                    let err = MergeWrite { msg: &err_msg }.fail();
                    if notifier.send(err).is_err() {
                        warn!(
                            "Failed to notify the error result of pending writes, table:{}",
                            self.name()
                        );
                    }
                }
                Err(e)
            }
        }
    }
}

#[async_trait]
impl Table for TableImpl {
    fn name(&self) -> &str {
        &self.table_data.name
    }

    fn id(&self) -> TableId {
        self.table_data.id
    }

    fn schema(&self) -> Schema {
        self.table_data.schema()
    }

    fn options(&self) -> HashMap<String, String> {
        self.table_data.table_options().to_raw_map()
    }

    fn partition_info(&self) -> Option<PartitionInfo> {
        None
    }

    fn engine_type(&self) -> &str {
        &self.engine_type
    }

    fn stats(&self) -> TableStats {
        self.table_data.metrics.table_stats()
    }

    async fn write(&self, request: WriteRequest) -> Result<usize> {
        if self.instance.max_rows_in_write_queue > 0 {
            return self.write_with_pending_queue(request).await;
        }

        let mut serial_exec = self.table_data.serial_exec.lock().await;
        let mut writer = Writer::new(
            self.instance.clone(),
            self.space_table.clone(),
            &mut serial_exec,
        );
        writer
            .write(request)
            .await
            .box_err()
            .context(Write { table: self.name() })
    }

    async fn read(&self, mut request: ReadRequest) -> Result<SendableRecordBatchStream> {
        request.opts.read_parallelism = 1;
        let mut streams = self
            .instance
            .partitioned_read_from_table(&self.space_table, request)
            .await
            .box_err()
            .context(Scan { table: self.name() })?;

        assert_eq!(streams.streams.len(), 1);
        let stream = streams.streams.pop().unwrap();

        Ok(stream)
    }

    async fn get(&self, request: GetRequest) -> Result<Option<Row>> {
        let schema = request.projected_schema.to_record_schema_with_key();
        let primary_key_columns = &schema.key_columns()[..];
        ensure!(
            primary_key_columns.len() == request.primary_key.len(),
            GetInvalidPrimaryKey {
                schema: schema.clone(),
                primary_key_columns,
            }
        );

        let mut primary_key_exprs: Vec<Expr> = Vec::with_capacity(request.primary_key.len());
        for (primary_key_value, column_schema) in
            request.primary_key.iter().zip(primary_key_columns.iter())
        {
            let v = primary_key_value
                .as_scalar_value()
                .with_context(|| GetNullPrimaryKey {
                    schema: schema.clone(),
                    primary_key_columns,
                })?;
            primary_key_exprs.push(
                Expr::Column(Column::from_qualified_name(&column_schema.name)).eq(Expr::Literal(v)),
            );
        }

        let predicate = PredicateBuilder::default()
            .set_time_range(TimeRange::min_to_max())
            .add_pushdown_exprs(&primary_key_exprs)
            .build();

        let read_request = ReadRequest {
            request_id: request.request_id,
            opts: ReadOptions::default(),
            projected_schema: request.projected_schema,
            predicate,
            order: ReadOrder::None,
            metrics_collector: MetricsCollector::new(GET_METRICS_COLLECTOR_NAME.to_string()),
        };
        let mut batch_stream = self
            .read(read_request)
            .await
            .box_err()
            .context(Scan { table: self.name() })?;

        let mut result_columns = Vec::with_capacity(schema.num_columns());

        while let Some(batch) = batch_stream
            .try_next()
            .await
            .box_err()
            .context(Get { table: self.name() })?
        {
            let row_num = batch.num_rows();
            if row_num == 0 {
                return Ok(None);
            }
            for row_idx in 0..row_num {
                for col_idx in 0..batch.num_columns() {
                    let col = batch.column(col_idx);
                    result_columns.push(col.datum(row_idx));
                }

                let mut result_columns_k = vec![];
                for col_idx in schema.primary_key_idx() {
                    result_columns_k.push(result_columns[*col_idx].clone());
                }
                if request.primary_key == result_columns_k {
                    return Ok(Some(Row::from_datums(result_columns)));
                }
                result_columns.clear();
            }
        }

        Ok(None)
    }

    async fn partitioned_read(&self, request: ReadRequest) -> Result<PartitionedStreams> {
        let streams = self
            .instance
            .partitioned_read_from_table(&self.space_table, request)
            .await
            .box_err()
            .context(Scan { table: self.name() })?;

        Ok(streams)
    }

    async fn alter_schema(&self, request: AlterSchemaRequest) -> Result<usize> {
        let mut serial_exec = self.table_data.serial_exec.lock().await;
        let mut alterer = Alterer::new(
            self.table_data.clone(),
            &mut serial_exec,
            self.instance.clone(),
        )
        .await;

        alterer
            .alter_schema_of_table(request)
            .await
            .box_err()
            .context(AlterSchema { table: self.name() })?;
        Ok(0)
    }

    async fn alter_options(&self, options: HashMap<String, String>) -> Result<usize> {
        let mut serial_exec = self.table_data.serial_exec.lock().await;
        let alterer = Alterer::new(
            self.table_data.clone(),
            &mut serial_exec,
            self.instance.clone(),
        )
        .await;

        alterer
            .alter_options_of_table(options)
            .await
            .box_err()
            .context(AlterOptions { table: self.name() })?;
        Ok(0)
    }

    async fn flush(&self, request: FlushRequest) -> Result<()> {
        self.instance
            .manual_flush_table(&self.table_data, request)
            .await
            .box_err()
            .context(Flush { table: self.name() })
    }

    async fn compact(&self) -> Result<()> {
        self.instance
            .manual_compact_table(&self.table_data)
            .await
            .box_err()
            .context(Compact { table: self.name() })?;
        Ok(())
    }
}
