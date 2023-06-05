// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{collections::{VecDeque, HashMap}, fmt::Display};

use async_trait::async_trait;
use common_types::{table::ShardId, schema::IndexInWriterSchema, SequenceNumber};
use common_util::error::{GenericError, GenericResult, BoxError};
use log::{debug, info, trace, error};
use snafu::ResultExt;
use table_engine::table::TableId;
use wal::{manager::{WalManager, ReadContext, ReadRequest, WalManagerRef, ReadBoundary, ScanRequest, RegionId, ScanContext}, log_batch::LogEntry};
use crate::{instance::{engine::{Result, ReplayWalNoCause, ReplayWalWithCause}, 
flush_compaction::{Flusher, TableFlushOptions}, serial_executor::TableOpSerialExecutor, self, write::MemTableWriter}, 
table::data::TableDataRef, payload::{ReadPayload, WalDecoder}};

/// Wal replayer
pub struct WalReplayer {
    stages: HashMap<TableId, ReplayStage>,
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

    pub async fn replay(&mut self) -> Result<HashMap<TableId, Result<()>>> {
        // Build core according to mode.
        let core = Self::build_core(self.mode); 
        info!(
            "Replay wal logs begin, context:{}, states:{:?}", self.context, self.stages
        );
        core.replay(&self.context, &mut self.stages).await?;
        info!(
            "Replay wal logs finish, context:{}, states:{:?}", self.context, self.stages,
        );        

        // Return the replay results.        
        let stages = std::mem::take(&mut self.stages);
        let mut results = HashMap::with_capacity(self.stages.len());
        for (table_id, stage) in stages {
            let result = match stage {
                ReplayStage::Failed(e) => Err(e),
                ReplayStage::Success(_) => Ok(()),
                ReplayStage::Replay(_) => return ReplayWalNoCause {
                    msg: Some(format!("invalid stage, stage:{stage:?}, table_id:{table_id}, context:{}, mode:{:?}", self.context, self.mode)),
                }.fail(),
            };
            results.insert(table_id, result);
        }

        Ok(results)
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

#[derive(Debug)]
enum ReplayStage {
    Replay(TableDataRef),
    Failed(crate::instance::engine::Error),
    Success(()),
}

#[derive(Debug, Clone, Copy)]
enum ReplayMode {
    RegionBased,
    TableBased,
}

/// Replay core, the abstract of different replay strategies
#[async_trait]
trait ReplayCore {
    async fn replay(&self, context: &ReplayContext, states: &mut HashMap<TableId, ReplayStage>) -> Result<()>;
}

/// Table based wal replay core
struct TableBasedCore;

#[async_trait]
impl ReplayCore for TableBasedCore {
    async fn replay(&self, context: &ReplayContext, states: &mut HashMap<TableId, ReplayStage>) -> Result<()> {
        debug!(
            "Replay wal logs on table mode, context:{context}, states:{states:?}",
        );

        for (table_id, stage) in states.iter_mut() {
            match stage {
                ReplayStage::Replay(table_data) => {
                    let read_ctx = ReadContext {
                        batch_size: context.wal_replay_batch_size,
                        ..Default::default()
                    };
        
                    let result = Self::recover_table_logs(
                        &context.flusher,
                        context.max_retry_flush_limit,
                        context.wal_manager.as_ref(),
                        table_data.clone(),
                        context.wal_replay_batch_size,
                        &read_ctx,
                    ).await;

                    match result {
                        Ok(()) => {
                            *stage = ReplayStage::Success(());
                        },
                        Err(e) => {
                            *stage = ReplayStage::Failed(e);
                        },
                    }
                }

                ReplayStage::Failed(_) | ReplayStage::Success(_) => {
                    return ReplayWalNoCause {
                        msg: Some(format!("invalid stage, stage:{stage:?}, table_id:{}, shard_id:{}", table_id, context.shard_id))
                    }.fail();
                }
            }
        }

        Ok(())
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
        table_data: TableDataRef,
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
            .context(ReplayWalWithCause {
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
                .context(ReplayWalWithCause {
                    msg: None,
                })?;

            if log_entry_buf.is_empty() {
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

/// Region based wal replay core
struct RegionBasedCore;

#[async_trait]
impl ReplayCore for RegionBasedCore {
    async fn replay(&self, context: &ReplayContext, states: &mut HashMap<TableId, ReplayStage>) -> Result<()> {
        debug!(
            "Replay wal logs on region mode, context:{context}, states:{states:?}",
        );

        for (table_id, state) in states.iter_mut() {
            match state {
                ReplayStage::Replay(table_data) => {
                    let read_ctx = ReadContext {
                        batch_size: context.wal_replay_batch_size,
                        ..Default::default()
                    };
                    
                    let result = Self::replay_region_logs(
                        &context.flusher,
                        context.max_retry_flush_limit,
                        context.wal_manager.as_ref(),
                        table_data.clone(),
                        context.wal_replay_batch_size,
                        &read_ctx,
                        states,
                    ).await;

                    match result {
                        Ok(()) => {
                            *state = ReplayStage::Success(());
                        },
                        Err(e) => {
                            *state = ReplayStage::Failed(e);
                        },
                    }
                },
                ReplayStage::Failed(_) | ReplayStage::Success(_) => {
                    return ReplayWalNoCause {
                        msg: Some(format!("table_id:{}, shard_id:{}", table_id, context.shard_id))
                    }.fail();
                }
            }
        }

        Ok(())
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
        shard_id: ShardId,
        flusher: &Flusher,
        max_retry_flush_limit: usize,
        wal_manager: &dyn WalManager,
        replay_batch_size: usize,
        scan_ctx: &ScanContext,
        states: &mut HashMap<TableId, ReplayStage>
    ) -> Result<()> {
        // Scan all wal logs of current shard.
        let scan_req = ScanRequest {
            region_id: shard_id as RegionId,
        };

        let mut log_iter = wal_manager
            .scan(scan_ctx, &scan_req)
            .await
            .box_err()
            .context(ReplayWalWithCause {
                msg: None,
            })?;
        let mut log_entry_buf = VecDeque::with_capacity(replay_batch_size);
            
        // Lock all related tables.
        let mut table_serial_execs = HashMap::with_capacity(states.len());
        for (table_id, state) in states {
            match state {
                ReplayStage::Replay(table_data) => {
                    let mut serial_exec = table_data.serial_exec.lock().await;
                    table_serial_execs.insert(*table_id, &mut *serial_exec);
                }
                ReplayStage::Failed(_) | ReplayStage::Success(_) => {
                    return ReplayWalNoCause {
                        msg: Some(format!("table_id:{table_id}, shard_id:{shard_id}"))
                    }.fail();
                } 
            }
        }

        // Split and replay logs.
        loop {
            let decoder = WalDecoder::default();
            log_entry_buf = log_iter
                .next_log_entries(decoder, log_entry_buf)
                .await
                .box_err()
                .context(ReplayWalWithCause {
                    msg: None,
                })?;

            if log_entry_buf.is_empty() {
                break;
            }

            Self::replay_single_batch(&log_entry_buf, flusher, max_retry_flush_limit, wal_manager, replay_batch_size, read_ctx, &table_serial_execs, states).await;
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
            stages: &mut HashMap<TableId, ReplayStage>,
        ) -> Result<()>
    {
        let mut table_batches = Vec::with_capacity(stages.len());
        Self::split_log_batch_by_table(&log_batch, &mut table_batches);
        for table_batch in table_batches {
            // Replay all log entries of current table
            let last_sequence = table_batch.last_sequence;
            replay_table_log_entries(
                flusher,
                max_retry_flush_limit,
                &mut serial_exec,
                &table_data,
                log_batch.range(table_batch.start_log_idx..table_batch.end_log_idx),
                last_sequence
            )
            .await?;
        }

        Ok(())
    }

    fn split_log_batch_by_table<P>(log_batch: &VecDeque<LogEntry<P>>, table_batches: &mut Vec<TableBatch>) {
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
        for log_idx in 0..log_batch.len() {
            let last_round = log_idx + 1 == log_batch.len();
            
            let start_log_entry = log_batch.get(start_log_idx).unwrap();
            let current_log_entry = log_batch.get(log_idx).unwrap();
            
            let start_table_id = start_log_entry.table_id;
            let current_table_id = current_log_entry.table_id;
            let current_sequence = current_log_entry.sequence;

            if (current_table_id !=  start_table_id) || last_round {
                let end_log_idx = if last_round {
                    log_batch.len()
                } else {
                    start_log_idx = log_idx;
                    log_idx
                };
                
                table_batches.push(TableBatch {
                    table_id: TableId::new(start_table_id),
                    last_sequence: current_sequence,
                    start_log_idx,
                    end_log_idx,
                });
            }
        }
    }
}

struct TableBatch {
    table_id: TableId,
    last_sequence: SequenceNumber,
    start_log_idx: usize,
    end_log_idx: usize,
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
                        .context(ReplayWalWithCause {
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
        "Replay table log entries finish, table:{}, table_id:{:?}, last_sequence:{}",
        table_data.name, table_data.id, last_sequence
    );

    table_data.set_last_sequence(last_sequence);

    Ok(())
}
