// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Write logic of instance

use std::sync::Arc;

use common_types::{
    bytes::ByteVec,
    row::RowGroup,
    schema::{IndexInWriterSchema, Schema},
};
use common_util::{codec::row, define_result};
use log::{debug, error, info, trace, warn};
use object_store::ObjectStore;
use proto::table_requests;
use smallvec::SmallVec;
use snafu::{ensure, Backtrace, ResultExt, Snafu};
use table_engine::table::WriteRequest;
use tokio::sync::oneshot;
use wal::{
    log_batch::{LogWriteBatch, LogWriteEntry},
    manager::{SequenceNumber, WalManager, WriteContext},
};

use crate::{
    instance::{
        flush_compaction::TableFlushOptions,
        write_worker,
        write_worker::{BackgroundStatus, WorkerLocal, WriteTableCommand},
        Instance,
    },
    memtable::{key::KeySequence, PutContext},
    meta::Manifest,
    payload::WritePayload,
    space::SpaceAndTable,
    sst::factory::Factory,
    table::{
        data::{TableData, TableDataRef},
        version::MemTableForWrite,
    },
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to write to wal, table:{}, err:{}", table, source))]
    WriteLogBatch {
        table: String,
        source: wal::manager::Error,
    },

    #[snafu(display("Failed to write to memtable, table:{}, err:{}", table, source))]
    WriteMemTable {
        table: String,
        source: crate::table::version::Error,
    },

    #[snafu(display("Try to write to a dropped table, table:{}", table))]
    WriteDroppedTable { table: String },

    #[snafu(display(
        "Too many rows to write (more than {}), table:{}, rows:{}.\nBacktrace:\n{}",
        MAX_ROWS_TO_WRITE,
        table,
        rows,
        backtrace,
    ))]
    TooManyRows {
        table: String,
        rows: usize,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to find mutable memtable, table:{}, err:{}", table, source))]
    FindMutableMemTable {
        table: String,
        source: crate::table::data::Error,
    },
    #[snafu(display("Failed to write table, source:{}", source,))]
    Write { source: write_worker::Error },

    #[snafu(display("Failed to flush table, table:{}, err:{}", table, source))]
    FlushTable {
        table: String,
        source: crate::instance::flush_compaction::Error,
    },

    #[snafu(display(
        "Background flush failed, cannot write more data, err:{}.\nBacktrace:\n{}",
        msg,
        backtrace
    ))]
    BackgroundFlushFailed { msg: String, backtrace: Backtrace },

    #[snafu(display("Schema of request is incompatible with table, err:{}", source))]
    IncompatSchema {
        source: common_types::schema::CompatError,
    },

    #[snafu(display("Failed to encode row group, err:{}", source))]
    EncodeRowGroup {
        source: common_util::codec::row::Error,
    },

    #[snafu(display("Failed to update sequence of memtable, err:{}", source))]
    UpdateMemTableSequence { source: crate::memtable::Error },
}

define_result!(Error);

/// Max rows in a write request, must less than [u32::MAX]
const MAX_ROWS_TO_WRITE: usize = 10_000_000;

pub struct EncodeContext {
    row_group: RowGroup,
    index_in_writer: IndexInWriterSchema,
    encoded_rows: Vec<ByteVec>,
}

impl EncodeContext {
    fn new(row_group: RowGroup) -> Self {
        Self {
            row_group,
            index_in_writer: IndexInWriterSchema::default(),
            encoded_rows: Vec::new(),
        }
    }

    fn encode_rows(&mut self, table_schema: &Schema) -> Result<()> {
        // Encode the row group into the buffer, which can be reused to write to
        // memtable
        row::encode_row_group_for_wal(
            &self.row_group,
            table_schema,
            &self.index_in_writer,
            &mut self.encoded_rows,
        )
        .context(EncodeRowGroup)?;

        assert_eq!(self.row_group.num_rows(), self.encoded_rows.len());

        Ok(())
    }
}

impl<
        Wal: WalManager + Send + Sync + 'static,
        Meta: Manifest + Send + Sync + 'static,
        Store: ObjectStore,
        Fa: Factory + Send + Sync + 'static,
    > Instance<Wal, Meta, Store, Fa>
{
    /// Write data to the table under give space.
    pub async fn write_to_table(
        &self,
        space_table: &SpaceAndTable,
        request: WriteRequest,
    ) -> Result<usize> {
        // Collect metrics.
        space_table.table_data().metrics.on_write_request_begin();

        self.validate_before_write(space_table, &request)?;

        // Create a oneshot channel to send/receive write result.
        let (tx, rx) = oneshot::channel();
        let cmd = WriteTableCommand {
            space_table: space_table.clone(),
            request,
            tx,
        };

        // Send write request to write worker, actual works done in
        // Self::process_write_table_command().
        write_worker::process_command_in_write_worker(
            cmd.into_command(),
            space_table.table_data(),
            rx,
        )
        .await
        .context(Write)
    }

    /// Do the actual write, must called by write worker in write thread
    /// sequentially.
    pub(crate) async fn process_write_table_command(
        self: &Arc<Self>,
        worker_local: &mut WorkerLocal,
        space_table: &SpaceAndTable,
        request: WriteRequest,
    ) -> Result<usize> {
        let mut encode_ctx = EncodeContext::new(request.row_group);

        self.preprocess_write(worker_local, space_table, &mut encode_ctx)
            .await?;

        let table_data = space_table.table_data();
        let schema = table_data.schema();
        encode_ctx.encode_rows(&schema)?;

        let EncodeContext {
            row_group,
            index_in_writer,
            encoded_rows,
        } = encode_ctx;

        let sequence = self
            .write_to_wal(worker_local, &**table_data, encoded_rows)
            .await?;

        Self::write_to_memtable(
            worker_local,
            &**table_data,
            sequence,
            &row_group,
            index_in_writer,
        )
        .map_err(|e| {
            error!(
                "Failed to write to memtable, space_table:{:?}, err:{}",
                space_table, e
            );
            e
        })?;

        // Failure of writing memtable may cause inconsecutive sequence.
        if table_data.last_sequence() + 1 != sequence {
            warn!(
                "Sequence must be consecutive, space_table:{:?}, last_sequence:{}, wal_sequence:{}",
                space_table,
                table_data.last_sequence(),
                sequence
            );
        }

        debug!(
            "Instance write finished, update sequence, space_table:{:?}, last_sequence:{}",
            space_table, sequence
        );

        table_data.set_last_sequence(sequence);

        let num_rows = row_group.num_rows();
        // Collect metrics.
        table_data.metrics.on_write_request_done(num_rows);

        Ok(num_rows)
    }

    /// Return Ok if the request is valid, this is done before entering the
    /// write thread.
    fn validate_before_write(
        &self,
        space_table: &SpaceAndTable,
        request: &WriteRequest,
    ) -> Result<()> {
        ensure!(
            request.row_group.num_rows() < MAX_ROWS_TO_WRITE,
            TooManyRows {
                table: &space_table.table_data().name,
                rows: request.row_group.num_rows(),
            }
        );

        Ok(())
    }

    /// Preprocess before write, check:
    ///  - whether table is dropped
    ///  - memtable capacity and maybe trigger flush
    ///
    /// Fills [common_types::schema::IndexInWriterSchema] in [EncodeContext]
    async fn preprocess_write(
        self: &Arc<Self>,
        worker_local: &mut WorkerLocal,
        space_table: &SpaceAndTable,
        encode_ctx: &mut EncodeContext,
    ) -> Result<()> {
        let space = space_table.space();
        let table_data = space_table.table_data();

        ensure!(
            !table_data.is_dropped(),
            WriteDroppedTable {
                table: &table_data.name,
            }
        );

        // Checks schema compability.
        table_data
            .schema()
            .compatible_for_write(
                encode_ctx.row_group.schema(),
                &mut encode_ctx.index_in_writer,
            )
            .context(IncompatSchema)?;

        // TODO(yingwen): Allow write and retry flush.
        // Check background status, if background error occured, not allow to write
        // again.
        match &*worker_local.background_status() {
            // Compaction error is ignored now.
            BackgroundStatus::Ok | BackgroundStatus::CompactionFailed(_) => (),
            BackgroundStatus::FlushFailed(e) => {
                return BackgroundFlushFailed { msg: e.to_string() }.fail();
            }
        }

        if self.should_flush_instance() {
            if let Some(space) = self.space_store.find_maximum_memory_usage_space() {
                if let Some(table) = space.find_maximum_memory_usage_table() {
                    info!("Trying to flush table {} bytes {} in space {} because engine total memtable memory usage exceeds db_write_buffer_size {}.",
                          table.name,
                          table.memtable_memory_usage(),
                          space.name,
                          self.db_write_buffer_size,
                    );
                    self.handle_memtable_flush(worker_local, &table).await?;
                }
            }
        }

        if space.should_flush_space() {
            if let Some(table) = space.find_maximum_memory_usage_table() {
                info!("Trying to flush table {} bytes {} in space {} because space total memtable memory usage exceeds space_write_buffer_size {}.",
                      table.name,
                      table.memtable_memory_usage() ,
                      space.name,
                      space.write_buffer_size,
                );
                self.handle_memtable_flush(worker_local, &table).await?;
            }
        }

        if table_data.should_flush_table(worker_local) {
            self.handle_memtable_flush(worker_local, table_data).await?;
        }

        Ok(())
    }

    /// Write log_batch into wal, return the sequence number of log_batch.
    async fn write_to_wal(
        &self,
        _worker_local: &WorkerLocal,
        table_data: &TableData,
        encoded_rows: Vec<ByteVec>,
    ) -> Result<SequenceNumber> {
        // Convert into pb
        let mut write_req_pb = table_requests::WriteRequest::new();
        // Use the table schema instead of the schema in request to avoid schema
        // mismatch during replaying
        write_req_pb.set_schema(table_data.schema().into());
        write_req_pb.set_rows(encoded_rows.into());

        let mut log_batch = LogWriteBatch::new(table_data.wal_region_id());
        // Now we only have one request, so no need to use with_capacity
        log_batch.push(LogWriteEntry {
            payload: WritePayload::Write(&write_req_pb),
        });

        // Write to wal manager
        let write_ctx = WriteContext::default();
        let sequence = self
            .space_store
            .wal_manager
            .write(&write_ctx, &log_batch)
            .await
            .context(WriteLogBatch {
                table: &table_data.name,
            })?;

        Ok(sequence)
    }

    // TODO(yingwen): How to trigger flush if we found memtables are full during
    // inserting memtable? RocksDB checks memtable size in MemTableInserter
    /// Write data into memtable.
    ///
    /// The data in `encoded_rows` will be moved to memtable.
    ///
    /// The len of `row_group` and `encoded_rows` must be equal.
    pub(crate) fn write_to_memtable(
        worker_local: &WorkerLocal,
        table_data: &TableData,
        sequence: SequenceNumber,
        row_group: &RowGroup,
        index_in_writer: IndexInWriterSchema,
    ) -> Result<()> {
        if row_group.is_empty() {
            return Ok(());
        }

        let schema = row_group.schema();
        // Store all memtables we wrote and update their last sequence later.
        let mut wrote_memtables: SmallVec<[_; 4]> = SmallVec::new();
        let mut last_mutable_mem: Option<MemTableForWrite> = None;

        let mut ctx = PutContext::new(index_in_writer);
        for (row_idx, row) in row_group.iter().enumerate() {
            // TODO(yingwen): Add RowWithSchema and take RowWithSchema as input, then remove
            // this unwrap()
            let timestamp = row.timestamp(schema).unwrap();
            // skip expired row
            if table_data.is_expired(timestamp) {
                trace!("Skip expired row when write to memtable, row:{:?}", row);
                continue;
            }
            if last_mutable_mem.is_none()
                || !last_mutable_mem
                    .as_ref()
                    .unwrap()
                    .accept_timestamp(timestamp)
            {
                // The time range is not processed by current memtable, find next one.
                let mutable_mem = table_data
                    .find_or_create_mutable(worker_local, timestamp, schema)
                    .context(FindMutableMemTable {
                        table: &table_data.name,
                    })?;
                wrote_memtables.push(mutable_mem.clone());
                last_mutable_mem = Some(mutable_mem);
            }

            // We have check the row num is less than `MAX_ROWS_TO_WRITE`, it is safe to
            // cast it to u32 here
            let key_seq = KeySequence::new(sequence, row_idx as u32);
            // TODO(yingwen): Batch sample timestamp in sampling phase.
            last_mutable_mem
                .as_ref()
                .unwrap()
                .put(&mut ctx, key_seq, row, schema, timestamp)
                .context(WriteMemTable {
                    table: &table_data.name,
                })?;
        }

        // Update last sequence of memtable.
        for mem_wrote in wrote_memtables {
            mem_wrote
                .set_last_sequence(sequence)
                .context(UpdateMemTableSequence)?;
        }

        Ok(())
    }

    /// Flush memtables of table in background.
    ///
    /// Only flush mutable memtables, assuming all immutable memtables are
    /// flushing.
    async fn handle_memtable_flush(
        self: &Arc<Self>,
        worker_local: &mut WorkerLocal,
        table_data: &TableDataRef,
    ) -> Result<()> {
        let opts = TableFlushOptions::default();

        // Set `block_on_write_thread` to false and let flush do in background.
        self.flush_table_in_worker(worker_local, table_data, opts)
            .await
            .context(FlushTable {
                table: &table_data.name,
            })
    }
}
