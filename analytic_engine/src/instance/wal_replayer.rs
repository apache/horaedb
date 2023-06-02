// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{collections::{VecDeque, HashMap}};

use async_trait::async_trait;
use common_types::{table::ShardId, schema::IndexInWriterSchema, SequenceNumber};
use common_util::error::{GenericError, GenericResult, BoxError};
use log::{debug, info, trace, error};
use snafu::ResultExt;
use table_engine::table::TableId;
use wal::{manager::{WalManager, ReadContext, ReadRequest, WalManagerRef, ReadBoundary}, log_batch::LogEntry};
use crate::{instance::{engine::{Result, RecoverFromWalNoCause, RecoverFromWalWithCause}, flush_compaction::{Flusher, TableFlushOptions}, serial_executor::TableOpSerialExecutor, self, write::MemTableWriter}, table::data::TableDataRef, payload::{ReadPayload, WalDecoder}};

/// Wal replayer
pub struct WalReplayer {
    states: HashMap<TableId, ReplayState>,
    context: WalReplayContext,
}

// impl WalReplayer {
//     pub fn build_core<C: ReplayCore>(mode: ReplayMode) -> C {
//     }
// }
 
struct WalReplayContext {
    pub shard_id: ShardId,
    pub wal_manager: WalManagerRef,
    pub wal_replay_batch_size: usize,
    pub flusher: Flusher,
    pub max_retry_flush_limit: usize,
}

enum ReplayState {
    Recovering(TableDataRef),
    Failed(crate::instance::engine::Error),
    Success(()),
}

enum ReplayMode {
    RegionBased,
    TableBased,
}

#[async_trait]
trait ReplayCore {
    async fn replay(&self, context: &WalReplayContext, states: &mut HashMap<TableId, ReplayState>) -> Result<()>;
}

/// Table based wal replay core
struct TableBasedCore;

impl TableBasedCore {
    /// Recover table data from wal.
    ///
    /// Called by write worker
    async fn recover_single_table_data(
        flusher: &Flusher,
        max_retry_flush_limit: usize,
        wal_manager: &dyn WalManager,
        table_data: TableDataRef,
        replay_batch_size: usize,
        read_ctx: &ReadContext,
    ) -> Result<()> {
        debug!(
            "Instance recover table from wal, replay batch size:{}, table id:{}, shard info:{:?}",
            replay_batch_size, table_data.id, table_data.shard_info
        );

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
            .context(RecoverFromWalWithCause {
                msg: None,
            })?;

        let mut serial_exec = table_data.serial_exec.lock().await;
        let mut log_entry_buf = VecDeque::with_capacity(replay_batch_size);
        loop {
            // fetch entries to log_entry_buf
            let decoder = WalDecoder::default();
            log_entry_buf = log_iter
                .next_log_entries(decoder, log_entry_buf)
                .await
                .box_err()
                .context(RecoverFromWalWithCause {
                    msg: None,
                })?;

            if log_entry_buf.is_empty() {
                info!(
                    "Instance replay an empty table log entries, table:{}, table_id:{:?}",
                    table_data.name, table_data.id
                );
                break;
            }
    
            // Replay all log entries of current table
            let last_sequence = log_entry_buf.back().unwrap().sequence;
            replay_table_log_entries(
                flusher,
                max_retry_flush_limit,
                &mut serial_exec,
                &table_data,
                log_entry_buf.iter(),
                last_sequence
            )
            .await?;
        }

        Ok(())
    }
}

#[async_trait]
impl ReplayCore for TableBasedCore {
    async fn replay(&self, context: &WalReplayContext, states: &mut HashMap<TableId, ReplayState>) -> Result<()> {
        for (table_id, state) in states.iter_mut() {
            match state {
                ReplayState::Recovering(table_data) => {
                    let read_ctx = ReadContext {
                        batch_size: context.wal_replay_batch_size,
                        ..Default::default()
                    };
        
                    let result = Self::recover_single_table_data(
                        &context.flusher,
                        context.max_retry_flush_limit,
                        context.wal_manager.as_ref(),
                        table_data.clone(),
                        context.wal_replay_batch_size,
                        &read_ctx,
                    ).await;

                    match result {
                        Ok(()) => {
                            *state = ReplayState::Success(());
                        },
                        Err(e) => {
                            *state = ReplayState::Failed(e);
                        },
                    }
                },
                ReplayState::Failed(_) | ReplayState::Success(_) => {
                    return RecoverFromWalNoCause {
                        msg: Some(format!("table_id:{}, shard_id:{}", table_id, context.shard_id))
                    }.fail();
                }
            }
        }

        Ok(())
    }
}

/// Region based wal replay core
struct RegionBasedCore;

impl RegionBasedCore {
    /// Recover table data from wal.
    ///
    /// Called by write worker
    async fn replay_logs_in_region(
        shard_id: ShardId,
        flusher: &Flusher,
        max_retry_flush_limit: usize,
        wal_manager: &dyn WalManager,
        replay_batch_size: usize,
        read_ctx: &ReadContext,
        states: &mut HashMap<TableId, ReplayState>
    ) -> Result<()> {
        debug!(
            "Instance recover table from wal, replay batch size:{}, table id:{}, shard info:{:?}",
            replay_batch_size, table_data.id, table_data.shard_info
        );

        let table_location = table_data.table_location();
        let wal_location =
            instance::create_wal_location(table_location.id, table_location.shard_info);
        let read_req = ReadRequest {
            location: wal_location,
            start: ReadBoundary::Excluded(table_data.current_version().flushed_sequence()),
            end: ReadBoundary::Max,
        };

        // Read all wal of current shard.
        let mut log_iter = wal_manager
            .read_batch(read_ctx, &read_req)
            .await
            .box_err()
            .context(RecoverFromWalWithCause {
                msg: None,
            })?;
        let mut log_entry_buf = VecDeque::with_capacity(replay_batch_size);
            
        // Lock all related tables.
        let mut table_serial_execs = HashMap::with_capacity(states.len());
        for (table_id, state) in states {
            match state {
                ReplayState::Recovering(table_data) => {
                    let mut serial_exec = table_data.serial_exec.lock().await;
                    table_serial_execs.insert(*table_id, &mut *serial_exec);
                }
                ReplayState::Failed(_) | ReplayState::Success(_) => {
                    return RecoverFromWalNoCause {
                        msg: Some(format!("table_id:{table_id}, shard_id:{shard_id}"))
                    }.fail();
                } 
            }
        }

        loop {
            // fetch entries to log_entry_buf
            let decoder = WalDecoder::default();
            log_entry_buf = log_iter
                .next_log_entries(decoder, log_entry_buf)
                .await
                .box_err()
                .context(RecoverFromWalWithCause {
                    msg: None,
                })?;

            if log_entry_buf.is_empty() {
                info!(
                    "Instance replay an empty table log entries, table:{}, table_id:{:?}",
                    table_data.name, table_data.id
                );
                break;
            }

            Self::replay_single_batch(&log_entry_buf, flusher, max_retry_flush_limit, wal_manager, replay_batch_size, read_ctx, ).await;
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
            table_serial_execs: &HashMap<TableId, &mut TableOpSerialExecutor>,            
            states: &mut HashMap<TableId, ReplayState>
        ) -> Result<()>
    {
        struct TableRecoverContext {
            table_id: TableId,
            start_log_idx: usize,
            end_log_idx: usize,
        }

        let mut start_log_idx = 0usize;
        for log_idx in 0..log_batch.len() {
            let last_round = log_idx + 1 == log_batch.len();
            
            let start_log_entry = log_batch.get(start_log_idx).unwrap();
            let current_log_entry = log_batch.get(log_idx).unwrap();
            
            let start_table_id = start_log_entry.table_id;
            let current_table_id = current_log_entry.table_id;
            
            let table_ctx = if (current_table_id !=  start_table_id) || last_round {
                let end_log_idx = if last_round {
                    log_batch.len()
                } else {
                    start_log_idx = log_idx;
                    log_idx
                };
                
                TableRecoverContext {
                    table_id: TableId::new(start_table_id),
                    start_log_idx,
                    end_log_idx,
                }
            } else {
                continue;
            };

            // Replay all log entries of current table
            let last_sequence = current_log_entry.sequence;
            replay_table_log_entries(
                flusher,
                max_retry_flush_limit,
                &mut serial_exec,
                &table_data,
                log_batch.range(table_ctx.start_log_idx..table_ctx.end_log_idx),
                last_sequence
            )
            .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl ReplayCore for RegionBasedCore {
    async fn replay(&self, context: &WalReplayContext, states: &mut HashMap<TableId, ReplayState>) -> Result<()> {
        for (table_id, state) in states.iter_mut() {
            match state {
                ReplayState::Recovering(table_data) => {
                    let read_ctx = ReadContext {
                        batch_size: context.wal_replay_batch_size,
                        ..Default::default()
                    };
                    
                    let result = Self::replay_logs_in_region(
                        &context.flusher,
                        context.max_retry_flush_limit,
                        context.wal_manager.as_ref(),
                        table_data.clone(),
                        context.wal_replay_batch_size,
                        &read_ctx,
                    ).await;

                    match result {
                        Ok(()) => {
                            *state = ReplayState::Success(());
                        },
                        Err(e) => {
                            *state = ReplayState::Failed(e);
                        },
                    }
                },
                ReplayState::Failed(_) | ReplayState::Success(_) => {
                    return RecoverFromWalNoCause {
                        msg: Some(format!("table_id:{}, shard_id:{}", table_id, context.shard_id))
                    }.fail();
                }
            }
        }
        Ok(())
    }
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
        "Instance replay table log entries begin, table:{}, table_id:{:?}, sequence:{}",
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
                    .context(RecoverFromWalWithCause {
                        msg: Some(format!("table_id:{}, table_name:{}, space_id:{}", table_data.space_id, table_data.name, table_data.id)),
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
                        .context(RecoverFromWalWithCause {
                            msg: Some(format!("table_id:{}, table_name:{}, space_id:{}", table_data.space_id, table_data.name, table_data.id)),
                        })?;
                }
            }
            ReadPayload::AlterSchema { .. } | ReadPayload::AlterOptions { .. } => {
                // Ignore records except Data.
                //
                // - DDL (AlterSchema and AlterOptions) should be recovered
                //   from Manifest on start.
            }
        }
    }

    debug!(
        "Instance replay table log entries end, table:{}, table_id:{:?}, last_sequence:{}",
        table_data.name, table_data.id, last_sequence
    );

    table_data.set_last_sequence(last_sequence);

    Ok(())
}
