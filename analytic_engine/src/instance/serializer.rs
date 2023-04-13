// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{Arc, Condvar, Mutex},
    time::Instant,
};

use common_util::{runtime::Runtime, time::InstantExt};
use futures::Future;
use log::error;
use table_engine::table::{Error as TableError, Result as TableResult, TableId};
use tokio::sync::oneshot;

use crate::{
    instance::flush_compaction::{self, BackgroundFlushFailed},
    table::metrics::Metrics,
};

#[derive(Default)]
enum FlushState {
    #[default]
    Ok,
    Flushing,
    Failed {
        err_msg: String,
    },
}

type ScheduleLock = Arc<(Mutex<FlushState>, Condvar)>;

#[derive(Default)]
pub struct TableFlushScheduler {
    schedule_lock: ScheduleLock,
}

/// To ensure the consistency of a table's data, these rules are required:
/// - The write procedure (write wal + write memtable) should be serialized as a
///   whole, that is to say, it is not allowed to write wal and memtable
///   concurrently or interleave the two sub-procedures;
/// - Any operation that may change the data of a table should be serialized,
///   including altering table schema, dropping table, etc;
/// - The flush procedure of a table should be serialized;
pub struct TableOpSerializer {
    table_id: TableId,
    flush_scheduler: TableFlushScheduler,
}

impl TableOpSerializer {
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

impl TableOpSerializer {
    pub fn flush_scheduler(&mut self) -> &mut TableFlushScheduler {
        &mut self.flush_scheduler
    }
}

impl TableFlushScheduler {
    /// Control the flush procedure and ensure multiple flush procedures to be
    /// sequential.
    ///
    /// REQUIRE: should only be called by the write thread.
    #[allow(clippy::too_many_arguments)]
    pub async fn flush_sequentially<F, T>(
        &mut self,
        table: String,
        metrics: &Metrics,
        flush_job: F,
        on_flush_success: T,
        block_on_write_thread: bool,
        runtime: &Runtime,
        res_sender: Option<oneshot::Sender<TableResult<()>>>,
    ) -> flush_compaction::Result<()>
    where
        F: Future<Output = flush_compaction::Result<()>> + Send + 'static,
        T: Future<Output = ()> + Send + 'static,
    {
        // If flush operation is running, then we need to wait for it to complete first.
        // Actually, the loop waiting ensures the multiple flush procedures to be
        // sequential, that is to say, at most one flush is being executed at
        // the same time.
        {
            let mut stall_begin: Option<Instant> = None;
            let mut flush_state = self.schedule_lock.0.lock().unwrap();
            loop {
                match &*flush_state {
                    FlushState::Ok => {
                        break;
                    }
                    FlushState::Flushing => (),
                    FlushState::Failed { err_msg } => {
                        return BackgroundFlushFailed { msg: err_msg }.fail();
                    }
                }

                if stall_begin.is_none() {
                    stall_begin = Some(Instant::now());
                }
                flush_state = self.schedule_lock.1.wait(flush_state).unwrap();
            }
            if let Some(stall_begin) = stall_begin {
                metrics.on_write_stall(stall_begin.saturating_elapsed());
            }

            // TODO(yingwen): Store pending flush requests and retry flush on recoverable
            // error,  or try to recover from background error.

            // Mark the worker is flushing.
            *flush_state = FlushState::Flushing;
        }

        let schedule_lock = self.schedule_lock.clone();
        let task = async move {
            let flush_res = flush_job.await;
            on_flush_finished(schedule_lock, &flush_res);

            match flush_res {
                Ok(()) => {
                    on_flush_success.await;
                    send_flush_result(res_sender, Ok(()));
                }
                Err(e) => {
                    let e = Arc::new(e);
                    send_flush_result(
                        res_sender,
                        Err(TableError::Flush {
                            source: Box::new(e),
                            table,
                        }),
                    );
                }
            }
        };

        if block_on_write_thread {
            task.await;
        } else {
            runtime.spawn(task);
        }

        Ok(())
    }
}

fn on_flush_finished(schedule_lock: ScheduleLock, res: &flush_compaction::Result<()>) {
    let mut flush_state = schedule_lock.0.lock().unwrap();
    match res {
        Ok(()) => {
            *flush_state = FlushState::Ok;
        }
        Err(e) => {
            let err_msg = e.to_string();
            *flush_state = FlushState::Failed { err_msg };
        }
    }
    schedule_lock.1.notify_all();
}

fn send_flush_result(res_sender: Option<oneshot::Sender<TableResult<()>>>, res: TableResult<()>) {
    if let Some(tx) = res_sender {
        if let Err(send_res) = tx.send(res) {
            error!("Fail to send flush result, send_res:{:?}", send_res);
        }
    }
}
