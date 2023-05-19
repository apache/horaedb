// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};

use common_util::{runtime::Runtime, time::InstantExt};
use futures::Future;
use log::{error, info, warn};
use table_engine::table::TableId;
use tokio::sync::{
    oneshot,
    watch::{self, Receiver, Sender},
};

use super::flush_compaction::{BackgroundFlushFailed, TableFlushOptions};
use crate::{
    instance::flush_compaction::{Other, Result},
    table::data::TableData,
};

#[derive(Default)]
enum FlushState {
    #[default]
    Ready,
    Flushing,
    Failed {
        err_msg: String,
    },
}

type ScheduleSyncRef = Arc<ScheduleSync>;

struct ScheduleSync {
    state: Mutex<FlushState>,
    notifier: Sender<()>,
    continuous_flush_failure_count: AtomicUsize,
}

impl ScheduleSync {
    #[inline]
    pub fn should_retry_flush(&self, max_retry_limit: usize) -> bool {
        self.continuous_flush_failure_count.load(Ordering::Relaxed) < max_retry_limit
    }

    #[inline]
    pub fn reset_flush_failure_count(&self) {
        self.continuous_flush_failure_count
            .store(0, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_flush_failure_count(&self) {
        self.continuous_flush_failure_count
            .fetch_add(1, Ordering::Relaxed);
    }
}

pub struct TableFlushScheduler {
    schedule_sync: ScheduleSyncRef,
    state_watcher: Receiver<()>,
}

impl Default for TableFlushScheduler {
    fn default() -> Self {
        let (tx, rx) = watch::channel(());
        let schedule_sync = ScheduleSync {
            state: Mutex::new(FlushState::Ready),
            notifier: tx,
            continuous_flush_failure_count: AtomicUsize::new(0),
        };
        Self {
            schedule_sync: Arc::new(schedule_sync),
            state_watcher: rx,
        }
    }
}

/// All operations on tables must hold the mutable reference of this
/// [TableOpSerialExecutor].
///
/// To ensure the consistency of a table's data, these rules are required:
/// - The write procedure (write wal + write memtable) should be serialized as a
///   whole, that is to say, it is not allowed to write wal and memtable
///   concurrently or interleave the two sub-procedures;
/// - Any operation that may change the data of a table should be serialized,
///   including altering table schema, dropping table, etc;
/// - The flush procedure of a table should be serialized;
pub struct TableOpSerialExecutor {
    table_id: TableId,
    flush_scheduler: TableFlushScheduler,
}

impl TableOpSerialExecutor {
    pub fn new(table_id: TableId) -> Self {
        Self {
            table_id,
            flush_scheduler: TableFlushScheduler::default(),
        }
    }

    #[inline]
    pub fn table_id(&self) -> TableId {
        self.table_id
    }
}

impl TableOpSerialExecutor {
    pub fn flush_scheduler(&mut self) -> &mut TableFlushScheduler {
        &mut self.flush_scheduler
    }
}

impl TableFlushScheduler {
    pub fn is_in_flush(&self) -> bool {
        let state = self.schedule_sync.state.lock().unwrap();
        matches!(&*state, FlushState::Flushing)
    }

    /// Control the flush procedure and ensure multiple flush procedures to be
    /// sequential.
    ///
    /// REQUIRE: should only be called by the write thread.
    pub async fn flush_sequentially<F, T>(
        &mut self,
        flush_job: F,
        on_flush_success: T,
        block_on_write_thread: bool,
        opts: TableFlushOptions,
        runtime: &Runtime,
        table_data: Arc<TableData>,
    ) -> Result<()>
    where
        F: Future<Output = Result<()>> + Send + 'static,
        T: Future<Output = ()> + Send + 'static,
    {
        let metrics = &table_data.metrics;
        // If flush operation is running, then we need to wait for it to complete first.
        // Actually, the loop waiting ensures the multiple flush procedures to be
        // sequential, that is to say, at most one flush is being executed at
        // the same time.
        let mut stall_begin: Option<Instant> = None;

        loop {
            {
                // Check if the flush procedure is running and the lock will be dropped when
                // leaving the block.
                let mut flush_state = self.schedule_sync.state.lock().unwrap();
                match &*flush_state {
                    FlushState::Ready => {
                        // Mark the worker is flushing.
                        *flush_state = FlushState::Flushing;
                        break;
                    }
                    FlushState::Flushing => (),
                    FlushState::Failed { err_msg } => {
                        if self
                            .schedule_sync
                            .should_retry_flush(opts.max_retry_flush_limit)
                        {
                            warn!("Re-flush memory tables after background flush failed:{err_msg}");
                            // Mark the worker is flushing.
                            *flush_state = FlushState::Flushing;
                            break;
                        } else {
                            return BackgroundFlushFailed {
                                msg: err_msg,
                                retry_count: opts.max_retry_flush_limit,
                            }
                            .fail();
                        }
                    }
                }

                if stall_begin.is_none() {
                    stall_begin = Some(Instant::now());
                }
            }

            if self.state_watcher.changed().await.is_err() {
                return Other {
                    msg: "State notifier is dropped unexpectedly",
                }
                .fail();
            }
        }

        // Record the write stall cost.
        if let Some(stall_begin) = stall_begin {
            let time = stall_begin.saturating_elapsed();
            metrics.on_write_stall(time);
        }

        // TODO(yingwen): Store pending flush requests and retry flush on
        // recoverable error,  or try to recover from background
        // error.

        let schedule_sync = self.schedule_sync.clone();
        let task = async move {
            let flush_res = flush_job.await;
            on_flush_finished(schedule_sync, &flush_res);
            if flush_res.is_ok() {
                on_flush_success.await;
            }
            send_flush_result(opts.res_sender, flush_res);
        };

        if block_on_write_thread {
            task.await;
        } else {
            runtime.spawn(task);
        }

        Ok(())
    }
}

fn on_flush_finished(schedule_sync: ScheduleSyncRef, res: &Result<()>) {
    {
        let mut flush_state = schedule_sync.state.lock().unwrap();
        match res {
            Ok(()) => {
                schedule_sync.reset_flush_failure_count();
                *flush_state = FlushState::Ready;
            }
            Err(e) => {
                schedule_sync.inc_flush_failure_count();
                let err_msg = e.to_string();
                *flush_state = FlushState::Failed { err_msg };
            }
        }
    }

    if schedule_sync.notifier.send(()).is_err() {
        error!("Fail to notify flush state change, flush_res:{res:?}");
    }
}

fn send_flush_result(res_sender: Option<oneshot::Sender<Result<()>>>, res: Result<()>) {
    if let Some(tx) = res_sender {
        if let Err(send_res) = tx.send(res) {
            error!("Fail to send flush result, send_res:{:?}", send_res);
        }
    }
}
