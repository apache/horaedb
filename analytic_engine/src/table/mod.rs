// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Table implementation

use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use common_types::{
    row::{Row, RowGroupBuilder},
    schema::Schema,
    time::TimeRange,
};
use common_util::{error::BoxError, future_cancel::CancellationSafeFuture};
use datafusion::{common::Column, logical_expr::Expr};
use futures::TryStreamExt;
use log::{error, warn};
use snafu::{ensure, OptionExt, ResultExt};
use table_engine::{
    partition::PartitionInfo,
    predicate::PredicateBuilder,
    stream::{PartitionedStreams, SendableRecordBatchStream},
    table::{
        AlterOptions, AlterSchema, AlterSchemaRequest, Compact, Flush, FlushRequest, Get,
        GetInvalidPrimaryKey, GetNullPrimaryKey, GetRequest, MergeWrite, ReadOptions, ReadOrder,
        ReadRequest, Result, Scan, Table, TableId, TableStats, TooManyPendingWrites,
        WaitForPendingWrites, Write, WriteRequest,
    },
    ANALYTIC_ENGINE_TYPE,
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
// Additional 1/10 of the pending writes capacity is reserved for new pending
// writes.
const ADDITIONAL_PENDING_WRITE_CAP_RATIO: usize = 10;

struct WriteRequests {
    pub space_table: SpaceAndTable,
    pub instance: InstanceRef,
    pub table_data: TableDataRef,
    pub pending_writes: Arc<Mutex<PendingWriteQueue>>,
}

impl WriteRequests {
    pub fn new(
        space_table: SpaceAndTable,
        instance: InstanceRef,
        table_data: TableDataRef,
        pending_writes: Arc<Mutex<PendingWriteQueue>>,
    ) -> Self {
        Self {
            space_table,
            instance,
            table_data,
            pending_writes,
        }
    }
}

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
    pending_writes: Arc<Mutex<PendingWriteQueue>>,
}

impl TableImpl {
    pub fn new(instance: InstanceRef, space_table: SpaceAndTable) -> Self {
        let pending_writes = Mutex::new(PendingWriteQueue::new(instance.max_rows_in_write_queue));
        let table_data = space_table.table_data().clone();
        let space_id = space_table.space().space_id();
        Self {
            space_table,
            instance,
            engine_type: ANALYTIC_ENGINE_TYPE.to_string(),
            space_id,
            table_id: table_data.id,
            table_data,
            pending_writes: Arc::new(pending_writes),
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

/// The queue for buffering pending write requests.
struct PendingWriteQueue {
    max_rows: usize,
    pending_writes: PendingWrites,
}

/// The underlying queue for buffering pending write requests.
#[derive(Default)]
struct PendingWrites {
    writes: Vec<WriteRequest>,
    notifiers: Vec<Sender<Result<()>>>,
    num_rows: usize,
}

impl PendingWrites {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            writes: Vec::with_capacity(cap),
            notifiers: Vec::with_capacity(cap),
            num_rows: 0,
        }
    }

    /// Try to push the request into the pending queue.
    ///
    /// This push will be rejected if the schema is different.
    fn try_push(&mut self, request: WriteRequest) -> QueueResult {
        if !self.is_same_schema(request.row_group.schema()) {
            return QueueResult::Reject(request);
        }

        // For the first pending writes, don't provide the receiver because it should do
        // the write for the pending writes and no need to wait for the notification.
        let res = if self.is_empty() {
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

/// The result when trying to push write request to the queue.
enum QueueResult {
    /// This request is rejected because the queue is full or the schema is
    /// different.
    Reject(WriteRequest),
    /// This request is the first one in the queue.
    First,
    /// This request is pushed into the queue and the caller should wait for the
    /// finish notification.
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
    fn take_pending_writes(&mut self) -> PendingWrites {
        let curr_num_reqs = self.pending_writes.writes.len();
        let new_cap = curr_num_reqs / ADDITIONAL_PENDING_WRITE_CAP_RATIO + curr_num_reqs;
        let new_pending_writes = PendingWrites::with_capacity(new_cap);
        std::mem::replace(&mut self.pending_writes, new_pending_writes)
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
    /// Perform table write with pending queue.
    ///
    /// The writes will be put into the pending queue first. And the writer who
    /// submits the first request to the queue is responsible for merging and
    /// writing all the writes in the queue.
    ///
    /// NOTE: The write request will be rejected if the queue is full.
    async fn write_with_pending_queue(&self, request: WriteRequest) -> Result<usize> {
        let num_rows = request.row_group.num_rows();

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
                let write_requests = WriteRequests::new(
                    self.space_table.clone(),
                    self.instance.clone(),
                    self.table_data.clone(),
                    self.pending_writes.clone(),
                );

                match CancellationSafeFuture::new(
                    Self::write_requests(write_requests),
                    self.instance.write_runtime().clone(),
                )
                .await
                {
                    Ok(_) => Ok(num_rows),
                    Err(e) => Err(e),
                }
            }
            QueueResult::Waiter(rx) => {
                // The request is successfully pushed into the queue, and just wait for the
                // write result.
                match rx.await {
                    Ok(res) => {
                        res.box_err().context(Write { table: self.name() })?;
                        Ok(num_rows)
                    }
                    Err(_) => WaitForPendingWrites { table: self.name() }.fail(),
                }
            }
            QueueResult::Reject(_) => {
                // The queue is full, return error.
                error!(
                    "Pending_writes queue is full, max_rows_in_queue:{}, table:{}",
                    self.instance.max_rows_in_write_queue,
                    self.name(),
                );
                TooManyPendingWrites { table: self.name() }.fail()
            }
        }
    }

    async fn write_requests(write_requests: WriteRequests) -> Result<()> {
        let mut serial_exec = write_requests.table_data.serial_exec.lock().await;
        // The `serial_exec` is acquired, let's merge the pending requests and write
        // them all.
        let pending_writes = {
            let mut pending_queue = write_requests.pending_writes.lock().unwrap();
            pending_queue.take_pending_writes()
        };
        assert!(
            !pending_writes.is_empty(),
            "The pending writes should contain at least the one just pushed."
        );
        let merged_write_request =
            merge_pending_write_requests(pending_writes.writes, pending_writes.num_rows);

        let mut writer = Writer::new(
            write_requests.instance,
            write_requests.space_table,
            &mut serial_exec,
        );
        let write_res = writer
            .write(merged_write_request)
            .await
            .box_err()
            .context(Write {
                table: write_requests.table_data.name.clone(),
            });

        // There is no waiter for pending writes, return the write result.
        let notifiers = pending_writes.notifiers;
        if notifiers.is_empty() {
            return Ok(());
        }

        // Notify the waiters for the pending writes.
        match write_res {
            Ok(_) => {
                for notifier in notifiers {
                    if notifier.send(Ok(())).is_err() {
                        warn!(
                            "Failed to notify the ok result of pending writes, table:{}",
                            write_requests.table_data.name
                        );
                    }
                }
                Ok(())
            }
            Err(e) => {
                let err_msg = format!("Failed to do merge write, err:{e}");
                for notifier in notifiers {
                    let err = MergeWrite { msg: &err_msg }.fail();
                    if notifier.send(err).is_err() {
                        warn!(
                            "Failed to notify the error result of pending writes, table:{}",
                            write_requests.table_data.name
                        );
                    }
                }
                Err(e)
            }
        }
    }

    #[inline]
    fn should_queue_write_request(&self, request: &WriteRequest) -> bool {
        request.row_group.num_rows() < self.instance.max_rows_in_write_queue
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
        let _timer = self
            .space_table
            .table_data()
            .metrics
            .start_table_total_timer();

        if self.should_queue_write_request(&request) {
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

#[cfg(test)]
mod tests {
    use common_types::{schema::Version, time::Timestamp};

    use super::*;
    use crate::tests::{row_util, table::FixedSchemaTable};

    fn build_test_write_request(
        seed: i64,
        num_rows: usize,
        schema_version: Version,
    ) -> WriteRequest {
        let schema = FixedSchemaTable::default_schema_builder()
            .version(schema_version)
            .build()
            .unwrap();
        let mut schema_rows = Vec::with_capacity(num_rows);
        for i in 0..num_rows {
            let row = (
                "key1",
                Timestamp::new(seed + i as i64 * 7),
                "tag1-1",
                11.0,
                110.0,
                "tag2-1",
            );
            schema_rows.push(row);
        }
        let rows = row_util::new_rows_6(&schema_rows);
        let row_group = RowGroupBuilder::with_rows(schema, rows).unwrap().build();
        WriteRequest { row_group }
    }

    #[test]
    fn test_queue_write_requests() {
        let mut queue = PendingWriteQueue::new(100);
        let req0 = build_test_write_request(0, 99, 0);
        let res0 = queue.try_push(req0);
        assert!(!queue.is_full());
        assert!(matches!(res0, QueueResult::First));

        let req1 = build_test_write_request(10, 10, 0);
        let res1 = queue.try_push(req1);
        assert!(queue.is_full());
        assert!(matches!(res1, QueueResult::Waiter(_)));

        let req2 = build_test_write_request(20, 17, 0);
        let res2 = queue.try_push(req2);
        assert!(queue.is_full());
        assert!(matches!(res2, QueueResult::Reject(_)));
        if let QueueResult::Reject(req) = res2 {
            assert_eq!(req.row_group.num_rows(), 17);
        }

        // Reset the queue, and check the result.
        let pending_writes = queue.take_pending_writes();
        assert_eq!(pending_writes.num_rows, 99 + 10);
        assert_eq!(pending_writes.writes.len(), 2);
        // Only one waiter.
        assert_eq!(pending_writes.notifiers.len(), 1);

        assert!(queue.pending_writes.is_empty());
    }

    #[test]
    fn test_queue_write_requests_with_different_schema() {
        let mut queue = PendingWriteQueue::new(100);
        let req0 = build_test_write_request(0, 10, 0);
        let res0 = queue.try_push(req0);
        assert!(matches!(res0, QueueResult::First));

        let req1 = build_test_write_request(1, 10, 1);
        let res1 = queue.try_push(req1);
        assert!(matches!(res1, QueueResult::Reject(_)));
    }

    #[test]
    fn test_merge_pending_write_requests() {
        let mut queue = PendingWriteQueue::new(100);
        let mut total_requests = Vec::with_capacity(3);
        let req0 = build_test_write_request(0, 40, 0);
        total_requests.push(req0.clone());
        queue.try_push(req0);

        let req1 = build_test_write_request(10, 40, 0);
        total_requests.push(req1.clone());
        queue.try_push(req1);

        let req2 = build_test_write_request(10, 40, 0);
        total_requests.push(req2.clone());
        queue.try_push(req2);

        let pending_writes = queue.take_pending_writes();
        let mut merged_request =
            merge_pending_write_requests(pending_writes.writes, pending_writes.num_rows);

        let merged_rows = merged_request.row_group.take_rows();
        let original_rows = total_requests
            .iter_mut()
            .flat_map(|req| req.row_group.take_rows())
            .collect::<Vec<_>>();

        assert_eq!(merged_rows, original_rows);
    }
}
