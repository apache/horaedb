// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, VecDeque},
    fmt::Display,
    sync::Arc,
};

use async_trait::async_trait;
use common_types::{schema::IndexInWriterSchema, table::ShardId, SequenceNumber};
use common_util::error::{BoxError, GenericError, GenericResult};
use log::{debug, error, info, trace};
use snafu::ResultExt;
use table_engine::table::{TableId, TableRef};
use tokio::sync::{Mutex, MutexGuard};
use wal::{
    log_batch::LogEntry,
    manager::{
        ReadBoundary, ReadContext, ReadRequest, RegionId, ScanContext, ScanRequest, WalManager,
        WalManagerRef,
    },
};

use crate::{
    instance::{
        self,
        engine::{ReplayWalNoCause, ReplayWalWithCause, Result},
        flush_compaction::{Flusher, TableFlushOptions},
        serial_executor::TableOpSerialExecutor,
        write::MemTableWriter,
    },
    payload::{ReadPayload, WalDecoder},
    table::data::TableDataRef,
};

/// Wal replayer
pub struct WalReplayer {
    results: HashMap<TableId, Result<()>>,
    context: ReplayContext,
    mode: ReplayMode,
}

impl WalReplayer {
    fn build_core(mode: ReplayMode) -> Box<dyn ReplayCore> {
        info!("Replay wal in mode:{mode:?}");

        match mode {
            ReplayMode::RegionBased => Box::new(TableBasedCore),
            ReplayMode::TableBased => Box::new(RegionBasedCore),
        }
    }

    pub async fn replay(
        &mut self,
        table_datas: Vec<TableDataRef>,
    ) -> Result<HashMap<TableId, Result<()>>> {
        // Build core according to mode.
        let core = Self::build_core(self.mode);
        info!(
            "Replay wal logs begin, context:{}, tables:{:?}",
            self.context, table_datas
        );
        let result = core.replay(&self.context, &table_datas).await;
        info!(
            "Replay wal logs finish, context:{}, tables:{:?}",
            self.context, table_datas,
        );

        result
    }
}

struct ReplayContext {
    pub shard_id: ShardId,
    pub wal_manager: WalManagerRef,
    pub wal_replay_batch_size: usize,
    pub flusher: Flusher,
    pub max_retry_flush_limit: usize,
}

impl Display for ReplayContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplayContext")
            .field("shard_id", &self.shard_id)
            .field("replay_batch_size", &self.wal_replay_batch_size)
            .field("max_retry_flush_limit", &self.max_retry_flush_limit)
            .finish()
    }
}

#[derive(Debug, Clone, Copy)]
enum ReplayMode {
    RegionBased,
    TableBased,
}

/// Replay core, the abstract of different replay strategies
#[async_trait]
trait ReplayCore {
    async fn replay(
        &self,
        context: &ReplayContext,
        table_datas: &[TableDataRef],
    ) -> Result<HashMap<TableId, Result<()>>>;
}

/// Table based wal replay core
struct TableBasedCore;

#[async_trait]
impl ReplayCore for TableBasedCore {
    async fn replay(
        &self,
        context: &ReplayContext,
        table_datas: &[TableDataRef],
    ) -> Result<HashMap<TableId, Result<()>>> {
        debug!("Replay wal logs on table mode, context:{context}, tables:{table_datas:?}",);

        let mut results = HashMap::with_capacity(table_datas.len());
        let read_ctx = ReadContext {
            batch_size: context.wal_replay_batch_size,
            ..Default::default()
        };
        for table_data in table_datas {
            let table_id = table_data.id;
            let result = Self::recover_table_logs(
                &context.flusher,
                context.max_retry_flush_limit,
                context.wal_manager.as_ref(),
                table_data,
                context.wal_replay_batch_size,
                &read_ctx,
            )
            .await;

            results.insert(table_id, result);
        }

        Ok(results)
    }
}

impl TableBasedCore {
    /// Recover table data from wal.
    ///
    /// Called by write worker
    async fn recover_table_logs(
        flusher: &Flusher,
        max_retry_flush_limit: usize,
        wal_manager: &dyn WalManager,
        table_data: &TableDataRef,
        replay_batch_size: usize,
        read_ctx: &ReadContext,
    ) -> Result<()> {
        let table_location = table_data.table_location();
        let wal_location =
            instance::create_wal_location(table_location.id, table_location.shard_info);
        let read_req = ReadRequest {
            location: wal_location,
            start: ReadBoundary::Excluded(table_data.current_version().flushed_sequence()),
            end: ReadBoundary::Max,
        };

        // Read all wal of current table.
        let mut log_iter = wal_manager
            .read_batch(read_ctx, &read_req)
            .await
            .box_err()
            .context(ReplayWalWithCause { msg: None })?;

        let mut serial_exec = table_data.serial_exec.lock().await;
        let mut log_entry_buf = VecDeque::with_capacity(replay_batch_size);
        loop {
            // fetch entries to log_entry_buf
            let decoder = WalDecoder::default();
            log_entry_buf = log_iter
                .next_log_entries(decoder, log_entry_buf)
                .await
                .box_err()
                .context(ReplayWalWithCause { msg: None })?;

            if log_entry_buf.is_empty() {
                break;
            }

            // Replay all log entries of current table
            let last_sequence = log_entry_buf.back().unwrap().sequence;
            replay_table_log_entries(
                flusher,
                max_retry_flush_limit,
                &mut serial_exec,
                table_data,
                log_entry_buf.iter(),
                last_sequence,
            )
            .await?;
        }

        Ok(())
    }
}

/// Region based wal replay core
struct RegionBasedCore;

#[async_trait]
impl ReplayCore for RegionBasedCore {
    async fn replay(
        &self,
        context: &ReplayContext,
        table_datas: &[TableDataRef],
    ) -> Result<HashMap<TableId, Result<()>>> {
        debug!("Replay wal logs on region mode, context:{context}, states:{table_datas:?}",);

        let mut results = HashMap::with_capacity(table_datas.len());
        let scan_ctx = ScanContext {
            batch_size: context.wal_replay_batch_size,
            ..Default::default()
        };

        Self::replay_region_logs(
            &table_datas,
            context.shard_id,
            &context.flusher,
            context.max_retry_flush_limit,
            context.wal_manager.as_ref(),
            context.wal_replay_batch_size,
            &scan_ctx,
            &mut results,
        )
        .await?;

        Ok(results)
    }
}

impl RegionBasedCore {
    /// Replay logs in same region.
    ///
    /// Steps:
    /// + Scan all logs of region.
    /// + Split logs according to table ids.
    /// + Replay logs to recover data of tables.
    async fn replay_region_logs(
        table_datas: &[TableDataRef],
        shard_id: ShardId,
        flusher: &Flusher,
        max_retry_flush_limit: usize,
        wal_manager: &dyn WalManager,
        replay_batch_size: usize,
        scan_ctx: &ScanContext,
        table_results: &mut HashMap<TableId, Result<()>>,
    ) -> Result<()> {
        // Scan all wal logs of current shard.
        let scan_req = ScanRequest {
            region_id: shard_id as RegionId,
        };

        let mut log_iter = wal_manager
            .scan(scan_ctx, &scan_req)
            .await
            .box_err()
            .context(ReplayWalWithCause { msg: None })?;
        let mut log_entry_buf = VecDeque::with_capacity(replay_batch_size);

        // Lock all related tables.
        let mut serial_exec_ctxs = HashMap::with_capacity(table_datas.len());
        for table_data in table_datas {
            let serial_exec = table_data.serial_exec.lock().await;
            let serial_exec_ctx = SerialExecContext {
                table_data: table_data.clone(),
                serial_exec,
            };
            serial_exec_ctxs.insert(table_data.id, serial_exec_ctx);
        }

        // Split and replay logs.
        loop {
            let decoder = WalDecoder::default();
            log_entry_buf = log_iter
                .next_log_entries(decoder, log_entry_buf)
                .await
                .box_err()
                .context(ReplayWalWithCause { msg: None })?;

            if log_entry_buf.is_empty() {
                break;
            }

            Self::replay_single_batch(
                &log_entry_buf,
                flusher,
                max_retry_flush_limit,
                wal_manager,
                replay_batch_size,
                scan_ctx,
                &mut serial_exec_ctxs,
                table_results,
            )
            .await?;
        }

        Ok(())
    }

    async fn replay_single_batch(
        log_batch: &VecDeque<LogEntry<ReadPayload>>,
        flusher: &Flusher,
        max_retry_flush_limit: usize,
        wal_manager: &dyn WalManager,
        replay_batch_size: usize,
        read_ctx: &ReadContext,
        serial_exec_ctxs: &mut HashMap<TableId, SerialExecContext<'_>>,
        table_results: &mut HashMap<TableId, Result<()>>,
    ) -> Result<()> {
        let mut table_batches = Vec::with_capacity(serial_exec_ctxs.len());
        Self::split_log_batch_by_table(&log_batch, &mut table_batches);
        for table_batch in table_batches {
            let table_result = table_results.get(&table_batch.table_id);
            if let Some(Err(_)) = table_result {
                return Ok(());
            }

            // Replay all log entries of current table
            let serial_exec_ctx = serial_exec_ctxs.get_mut(&table_batch.table_id).unwrap();
            let last_sequence = table_batch.last_sequence;
            let result = replay_table_log_entries(
                flusher,
                max_retry_flush_limit,
                &mut serial_exec_ctx.serial_exec,
                &serial_exec_ctx.table_data,
                log_batch.range(table_batch.start_log_idx..table_batch.end_log_idx),
                last_sequence,
            )
            .await;

            table_results.insert(table_batch.table_id, result);
        }

        Ok(())
    }

    fn split_log_batch_by_table<P>(
        log_batch: &VecDeque<LogEntry<P>>,
        table_batches: &mut Vec<TableBatch>,
    ) {
        table_batches.clear();

        if log_batch.is_empty() {
            return;
        }

        // Split log batch by table id, for example:
        // input batch:
        //  |1|1|2|2|2|3|3|3|3|
        //
        // output batches:
        //  |1|1|, |2|2|2|, |3|3|3|3|
        let mut start_log_idx = 0usize;
        let mut curr_log_idx = 0usize;
        let mut start_table_id = log_batch.get(start_log_idx).unwrap().table_id;
        loop {
            let time_to_break = curr_log_idx == log_batch.len();
            let found_end_idx = if time_to_break {
                true
            } else {
                let current_table_id = log_batch.get(curr_log_idx).unwrap().table_id;
                if current_table_id != start_table_id {
                    true
                } else {
                    false
                }
            };

            if found_end_idx {
                let last_sequence = log_batch.get(curr_log_idx - 1).unwrap().sequence;
                table_batches.push(TableBatch {
                    table_id: TableId::new(start_table_id),
                    last_sequence,
                    start_log_idx,
                    end_log_idx: curr_log_idx,
                });

                // Step to next start idx.
                start_log_idx = curr_log_idx;
                start_table_id = if time_to_break {
                    u64::MAX
                } else {
                    log_batch.get(start_log_idx).unwrap().table_id
                };
            }

            if time_to_break {
                break;
            }
            curr_log_idx += 1;
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
struct TableBatch {
    table_id: TableId,
    last_sequence: SequenceNumber,
    start_log_idx: usize,
    end_log_idx: usize,
}

struct SerialExecContext<'a> {
    table_data: TableDataRef,
    serial_exec: MutexGuard<'a, TableOpSerialExecutor>,
}

/// Replay all log entries into memtable and flush if necessary.
async fn replay_table_log_entries(
    flusher: &Flusher,
    max_retry_flush_limit: usize,
    serial_exec: &mut TableOpSerialExecutor,
    table_data: &TableDataRef,
    log_entries: impl Iterator<Item = &LogEntry<ReadPayload>>,
    last_sequence: SequenceNumber,
) -> Result<()> {
    debug!(
        "Replay table log entries begin, table:{}, table_id:{:?}, sequence:{}",
        table_data.name, table_data.id, last_sequence
    );

    for log_entry in log_entries {
        let (sequence, payload) = (log_entry.sequence, &log_entry.payload);

        // Apply to memtable
        match payload {
            ReadPayload::Write { row_group } => {
                trace!(
                    "Instance replay row_group, table:{}, row_group:{:?}",
                    table_data.name,
                    row_group
                );

                let table_schema_version = table_data.schema_version();
                if table_schema_version != row_group.schema().version() {
                    // Data with old schema should already been flushed, but we avoid panic
                    // here.
                    error!(
                        "Ignore data with mismatch schema version during replaying, \
                        table:{}, \
                        table_id:{:?}, \
                        expect:{}, \
                        actual:{}, \
                        last_sequence:{}, \
                        sequence:{}",
                        table_data.name,
                        table_data.id,
                        table_schema_version,
                        row_group.schema().version(),
                        last_sequence,
                        sequence,
                    );

                    continue;
                }

                let index_in_writer =
                    IndexInWriterSchema::for_same_schema(row_group.schema().num_columns());
                let memtable_writer = MemTableWriter::new(table_data.clone(), serial_exec);
                memtable_writer
                    .write(sequence, &row_group.into(), index_in_writer)
                    .box_err()
                    .context(ReplayWalWithCause {
                        msg: Some(format!(
                            "table_id:{}, table_name:{}, space_id:{}",
                            table_data.space_id, table_data.name, table_data.id
                        )),
                    })?;

                // Flush the table if necessary.
                if table_data.should_flush_table(serial_exec) {
                    let opts = TableFlushOptions {
                        res_sender: None,
                        max_retry_flush_limit,
                    };
                    let flush_scheduler = serial_exec.flush_scheduler();
                    flusher
                        .schedule_flush(flush_scheduler, table_data, opts)
                        .await
                        .box_err()
                        .context(ReplayWalWithCause {
                            msg: Some(format!(
                                "table_id:{}, table_name:{}, space_id:{}",
                                table_data.space_id, table_data.name, table_data.id
                            )),
                        })?;
                }
            }
            ReadPayload::AlterSchema { .. } | ReadPayload::AlterOptions { .. } => {
                // Ignore records except Data.
                //
                // - DDL (AlterSchema and AlterOptions) should be recovered from
                //   Manifest on start.
            }
        }
    }

    debug!(
        "Replay table log entries finish, table:{}, table_id:{:?}, last_sequence:{}",
        table_data.name, table_data.id, last_sequence
    );

    table_data.set_last_sequence(last_sequence);

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use table_engine::table::TableId;
    use wal::log_batch::LogEntry;

    use crate::instance::wal_replayer::{RegionBasedCore, TableBatch};

    #[test]
    fn test_split_log_batch_by_table() {
        let test_set = test_set();
        for (test_batch, expected) in test_set {
            check_split_result(&test_batch, &expected);
        }
    }

    fn test_set() -> Vec<(VecDeque<LogEntry<u32>>, Vec<TableBatch>)> {
        let test_log_batch1: VecDeque<LogEntry<u32>> = VecDeque::from([
            LogEntry {
                table_id: 0,
                sequence: 1,
                payload: 0,
            },
            LogEntry {
                table_id: 0,
                sequence: 2,
                payload: 0,
            },
            LogEntry {
                table_id: 0,
                sequence: 3,
                payload: 0,
            },
            LogEntry {
                table_id: 1,
                sequence: 1,
                payload: 0,
            },
            LogEntry {
                table_id: 1,
                sequence: 2,
                payload: 0,
            },
            LogEntry {
                table_id: 2,
                sequence: 1,
                payload: 0,
            },
        ]);
        let expected1 = vec![
            TableBatch {
                table_id: TableId::new(0),
                last_sequence: 3,
                start_log_idx: 0,
                end_log_idx: 3,
            },
            TableBatch {
                table_id: TableId::new(1),
                last_sequence: 2,
                start_log_idx: 3,
                end_log_idx: 5,
            },
            TableBatch {
                table_id: TableId::new(2),
                last_sequence: 1,
                start_log_idx: 5,
                end_log_idx: 6,
            },
        ];

        let test_log_batch2: VecDeque<LogEntry<u32>> = VecDeque::from([LogEntry {
            table_id: 0,
            sequence: 1,
            payload: 0,
        }]);
        let expected2 = vec![TableBatch {
            table_id: TableId::new(0),
            last_sequence: 1,
            start_log_idx: 0,
            end_log_idx: 1,
        }];

        let test_log_batch3: VecDeque<LogEntry<u32>> = VecDeque::default();
        let expected3 = vec![];

        vec![
            (test_log_batch1, expected1),
            (test_log_batch2, expected2),
            (test_log_batch3, expected3),
        ]
    }

    fn check_split_result(batch: &VecDeque<LogEntry<u32>>, expected: &[TableBatch]) {
        let mut table_batches = Vec::new();
        RegionBasedCore::split_log_batch_by_table(batch, &mut table_batches);
        assert_eq!(&table_batches, expected);
    }
}
