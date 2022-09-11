// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Write workers

use std::{
    collections::HashMap,
    future::Future,
    sync::{
        atomic::{AtomicBool, AtomicI64, Ordering},
        Arc,
    },
    time::Instant,
};

use common_util::{
    define_result,
    runtime::{JoinHandle, Runtime},
    time::InstantExt,
};
use futures::future;
use log::{error, info};
use snafu::{Backtrace, ResultExt, Snafu};
use table_engine::{
    engine::{CloseTableRequest, DropTableRequest},
    table::{
        AlterSchemaRequest, Error as TableError, Result as TableResult, TableId, WriteRequest,
    },
};
use tokio::sync::{mpsc, oneshot, watch, watch::Ref, Mutex, Notify};

use super::alter::TableAlterSchemaPolicy;
use crate::{
    compaction::{TableCompactionRequest, WaitResult},
    instance::{
        engine,
        flush_compaction::{self, TableFlushOptions},
        write, write_worker, InstanceRef,
    },
    space::{SpaceId, SpaceRef},
    table::{data::TableDataRef, metrics::Metrics},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to wait flush completed, channel disconnected, err:{}", source))]
    WaitFlush {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Background flush failed, cannot write more data, err:{}.\nBacktrace:\n{}",
        msg,
        backtrace
    ))]
    BackgroundFlushFailed { msg: String, backtrace: Backtrace },

    #[snafu(display(
    "Failed to receive cmd result, channel disconnected, table:{}, worker_id:{}.\nBacktrace:\n{}",
    table,
    worker_id,
    backtrace,
    ))]
    ReceiveFromWorker {
        table: String,
        worker_id: usize,
        backtrace: Backtrace,
    },

    #[snafu(display("Channel error, err:{}", source))]
    Channel {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to manipulate table data, this table:{} does not belong to this worker: {}",
        table,
        worker_id
    ))]
    DataNotLegal { table: String, worker_id: usize },
}

define_result!(Error);

#[derive(Debug)]
pub enum BackgroundStatus {
    Ok,
    FlushFailed(Arc<flush_compaction::Error>),
    CompactionFailed(Arc<flush_compaction::Error>),
}

/// Local state of worker
///
/// The worker is single threaded and holding this is equivalent to holding a
/// write lock
#[derive(Debug)]
pub struct WorkerLocal {
    data: Arc<WorkerSharedData>,
    background_rx: watch::Receiver<BackgroundStatus>,
}

/// Notifier for the write worker when finishing flushing.
struct FlushNotifier(Arc<WorkerSharedData>);

impl FlushNotifier {
    fn new(data: Arc<WorkerSharedData>) -> Self {
        data.num_background_jobs.fetch_add(1, Ordering::SeqCst);

        Self(data)
    }

    /// Mark flush is done and notify the waiter status ok (write thread).
    /// Concurrency:
    /// - Caller should guarantee that there is only one thread (the flush
    ///   thread) calling this method
    pub fn notify_ok(self) {
        // Mark the worker is not flushing.
        self.0.set_is_flushing(false);
        // Send message to notify waiter, ignore send result.
        let _ = self.0.background_tx.send(BackgroundStatus::Ok);
    }

    /// Mark flush is done and notify the waiter error (write thread).
    /// Concurrency:
    /// - Caller should guarantee that there is only one thread (the flush
    ///   thread) calling this method
    pub fn notify_err(self, err: Arc<flush_compaction::Error>) {
        // Mark the worker is not flushing.
        self.0.set_is_flushing(false);
        // Send message to notify waiter, ignore send result.
        let _ = self
            .0
            .background_tx
            .send(BackgroundStatus::FlushFailed(err));
    }
}

impl Drop for FlushNotifier {
    fn drop(&mut self) {
        // SeqCst to ensure subtraction num_background_jobs won't be reordered.
        self.0.num_background_jobs.fetch_sub(1, Ordering::SeqCst);
        self.0.background_notify.notify_one();
    }
}

/// Notifier to notify compaction result. If no compaction happened, then the
/// notifier may not be signaled.
pub struct CompactionNotifier(Arc<WorkerSharedData>);

impl CompactionNotifier {
    fn new(data: Arc<WorkerSharedData>) -> Self {
        data.num_background_jobs.fetch_add(1, Ordering::SeqCst);

        Self(data)
    }

    pub fn notify_ok(self) {
        // Send message to notify waiter, ignore send result.
        let _ = self.0.background_tx.send(BackgroundStatus::Ok);
    }

    pub fn notify_err(self, err: Arc<flush_compaction::Error>) {
        // Send message to notify waiter, ignore send result.
        let _ = self
            .0
            .background_tx
            .send(BackgroundStatus::CompactionFailed(err));
    }
}

impl Clone for CompactionNotifier {
    fn clone(&self) -> Self {
        // It will add num_background_jobs in CompactionNotifier::new,
        // so we can't derive Clone for CompactionNotifier.
        CompactionNotifier::new(self.0.clone())
    }
}

impl Drop for CompactionNotifier {
    fn drop(&mut self) {
        // SeqCst to ensure subtraction num_background_jobs won't be reordered.
        self.0.num_background_jobs.fetch_sub(1, Ordering::SeqCst);
        self.0.background_notify.notify_one();
    }
}

fn send_flush_result(res_sender: Option<oneshot::Sender<TableResult<()>>>, res: TableResult<()>) {
    if let Some(tx) = res_sender {
        if let Err(send_res) = tx.send(res) {
            error!("Fail to send flush result, send_res: {:?}", send_res);
        }
    }
}

impl WorkerLocal {
    #[inline]
    pub fn background_status(&self) -> Ref<'_, BackgroundStatus> {
        self.background_rx.borrow()
    }

    /// Control the flush procedure and ensure multiple flush procedures to be
    /// sequential.
    ///
    /// REQUIRE: should only be called by the write thread.
    pub async fn flush_sequentially<F, T>(
        &mut self,
        table: String,
        metrics: &Metrics,
        flush_job: F,
        on_flush_success: T,
        block_on_write_thread: bool,
        res_sender: Option<oneshot::Sender<TableResult<()>>>,
    ) -> Result<()>
    where
        F: Future<Output = flush_compaction::Result<()>> + Send + 'static,
        T: Future<Output = ()> + Send + 'static,
    {
        // If flush operation is running, then we need to wait for it to complete first.
        // Actually, the loop waiting ensures the multiple flush procedures to be
        // sequential, that is to say, at most one flush is being executed at
        // the same time.
        let mut stall_begin = None;
        while self.data.is_flushing() {
            if stall_begin.is_none() {
                stall_begin = Some(Instant::now());
            }

            self.background_rx
                .changed()
                .await
                .map_err(|e| Box::new(e) as _)
                .context(WaitFlush)?;
        }
        assert!(!self.data.is_flushing());

        // Report write stall.
        if let Some(instant) = stall_begin {
            metrics.on_write_stall(instant.saturating_elapsed());
        }

        // Check background status, if background error occurred, current flush is not
        // allowed.
        match &*self.background_status() {
            // Now background compaction error is ignored.
            BackgroundStatus::Ok | BackgroundStatus::CompactionFailed(_) => (),
            BackgroundStatus::FlushFailed(e) => {
                return BackgroundFlushFailed { msg: e.to_string() }.fail();
            }
        }

        // TODO(yingwen): Store pending flush requests and retry flush on recoverable
        // error,  or try to recover from background error.

        // Mark the worker is flushing.
        self.data.set_is_flushing(true);

        let worker_data = self.data.clone();
        // Create a notifier, remember to mark flushed and notify wait when we done!
        let notifier = FlushNotifier::new(worker_data);
        let task = async move {
            let flush_res = flush_job.await;

            match flush_res {
                Ok(()) => {
                    notifier.notify_ok();
                    on_flush_success.await;
                    send_flush_result(res_sender, Ok(()));
                }
                Err(e) => {
                    let e = Arc::new(e);
                    notifier.notify_err(e.clone());
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
            self.data.runtime.spawn(task);
        }

        Ok(())
    }

    pub fn compaction_notifier(&self) -> CompactionNotifier {
        let data = self.data.clone();
        CompactionNotifier::new(data)
    }

    pub fn worker_id(&self) -> usize {
        self.data.as_ref().id
    }

    pub fn validate_table_data(
        &self,
        table_name: &str,
        table_id: usize,
        worker_num: usize,
    ) -> Result<()> {
        let worker_id = self.data.as_ref().id;
        if table_id % worker_num != worker_id {
            return DataNotLegal {
                table: table_name,
                worker_id,
            }
            .fail();
        }
        Ok(())
    }
}

/// Write table command.
pub struct WriteTableCommand {
    pub space: SpaceRef,
    pub table_data: TableDataRef,
    pub request: WriteRequest,
    /// Sender for the worker to return result of write
    pub tx: oneshot::Sender<write::Result<usize>>,
}

impl WriteTableCommand {
    /// Convert into [Command]
    pub fn into_command(self) -> Command {
        Command::Write(self)
    }
}

/// Recover table command.
pub struct RecoverTableCommand {
    pub space: SpaceRef,
    /// Table to recover
    pub table_data: TableDataRef,
    /// Sender for the worker to return result of recover
    pub tx: oneshot::Sender<engine::Result<Option<TableDataRef>>>,

    // Options for recover:
    /// Batch size to read records from wal to replay
    pub replay_batch_size: usize,
}

impl RecoverTableCommand {
    /// Convert into [Command]
    pub fn into_command(self) -> Command {
        Command::Recover(self)
    }
}

/// Close table command.
pub struct CloseTableCommand {
    /// The space of the table to close
    pub space: SpaceRef,
    pub request: CloseTableRequest,
    pub tx: oneshot::Sender<engine::Result<()>>,
}

impl CloseTableCommand {
    /// Convert into [Command]
    pub fn into_command(self) -> Command {
        Command::Close(self)
    }
}

/// Drop table command
pub struct DropTableCommand {
    /// The space of the table to drop
    pub space: SpaceRef,
    pub request: DropTableRequest,
    pub tx: oneshot::Sender<engine::Result<bool>>,
}

impl DropTableCommand {
    /// Convert into [Command]
    pub fn into_command(self) -> Command {
        Command::Drop(self)
    }
}

/// Create table command
pub struct CreateTableCommand {
    /// The space of the table to drop
    pub space: SpaceRef,
    pub table_data: TableDataRef,
    pub tx: oneshot::Sender<engine::Result<TableDataRef>>,
}

impl CreateTableCommand {
    /// Convert into [Command]
    pub fn into_command(self) -> Command {
        Command::Create(self)
    }
}

/// Alter table command.
pub struct AlterSchemaCommand {
    pub table_data: TableDataRef,
    pub request: AlterSchemaRequest,
    /// Sender for the worker to return result of alter schema
    pub tx: oneshot::Sender<write_worker::Result<()>>,
}

impl AlterSchemaCommand {
    /// Convert into [Command]
    pub fn into_command(self) -> Command {
        Command::AlterSchema(self)
    }
}

/// Alter table options command.
pub struct AlterOptionsCommand {
    pub table_data: TableDataRef,
    pub options: HashMap<String, String>,
    /// Sender for the worker to return result of alter schema
    pub tx: oneshot::Sender<engine::Result<()>>,
}

impl AlterOptionsCommand {
    /// Convert into [Command]
    pub fn into_command(self) -> Command {
        Command::AlterOptions(self)
    }
}

/// Flush table request.
pub struct FlushTableCommand {
    pub table_data: TableDataRef,
    pub flush_opts: TableFlushOptions,
    pub tx: oneshot::Sender<flush_compaction::Result<()>>,
}

impl FlushTableCommand {
    /// Convert into [Command]
    pub fn into_command(self) -> Command {
        Command::Flush(self)
    }
}

/// Compact table request.
pub struct CompactTableCommand {
    pub table_data: TableDataRef,
    pub waiter: Option<oneshot::Sender<WaitResult<()>>>,
    pub tx: oneshot::Sender<flush_compaction::Result<()>>,
}

impl CompactTableCommand {
    /// Convert into [Command]
    pub fn into_command(self) -> Command {
        Command::Compact(self)
    }
}

/// Command sent to write worker
pub enum Command {
    /// Write to table
    Write(WriteTableCommand),

    /// Drop table
    Create(CreateTableCommand),

    /// Drop table
    Drop(DropTableCommand),

    /// Recover table
    Recover(RecoverTableCommand),

    /// Close table
    Close(CloseTableCommand),

    /// Alter table schema
    AlterSchema(AlterSchemaCommand),

    /// Alter table modify setting
    AlterOptions(AlterOptionsCommand),

    /// Flush table
    Flush(FlushTableCommand),

    /// Compact table
    Compact(CompactTableCommand),

    /// Exit the worker
    Exit,
}

/// Write handle hold by a table
#[derive(Debug, Clone)]
pub struct WriteHandle {
    worker_data: Arc<WorkerSharedData>,
}

impl WriteHandle {
    /// Send command to write worker.
    ///
    /// Panic if channel is disconnected
    pub async fn send_command(&self, cmd: Command) {
        if self.worker_data.tx.send(cmd).await.is_err() {
            error!(
                "Failed to send command to worker, worker_id:{}",
                self.worker_id()
            );

            panic!("write worker {} disconnected", self.worker_id());
        }
    }

    /// Returns the id of the worker
    pub fn worker_id(&self) -> usize {
        self.worker_data.id
    }
}

pub async fn send_command_to_write_worker(cmd: Command, table_data: &TableDataRef) {
    table_data.write_handle.send_command(cmd).await;
}

pub async fn process_command_in_write_worker<T, E: std::error::Error + Send + Sync + 'static>(
    cmd: Command,
    table_data: &TableDataRef,
    rx: oneshot::Receiver<std::result::Result<T, E>>,
) -> Result<T> {
    send_command_to_write_worker(cmd, table_data).await;

    // Receive alter options result.
    match rx.await {
        Ok(res) => res.map_err(|e| Box::new(e) as _).context(Channel),
        Err(_) => ReceiveFromWorker {
            table: &table_data.name,
            worker_id: table_data.write_handle.worker_id(),
        }
        .fail(),
    }
}

#[allow(dead_code)]
pub async fn join_all<T, E: std::error::Error + Send + Sync + 'static>(
    table_vec: &[TableDataRef],
    rx_vec: Vec<oneshot::Receiver<std::result::Result<T, E>>>,
) -> Result<()> {
    let results = future::join_all(rx_vec).await;
    for (pos, res) in results.into_iter().enumerate() {
        let table_data = &table_vec[pos];
        match res {
            Ok(res) => {
                res.map_err(|e| Box::new(e) as _).context(Channel)?;
            }
            Err(_) => {
                return ReceiveFromWorker {
                    table: &table_data.name,
                    worker_id: table_data.write_handle.worker_id(),
                }
                .fail()
            }
        }
    }

    Ok(())
}

/// Write group options
pub struct Options {
    pub space_id: SpaceId,
    pub worker_num: usize,
    pub runtime: Arc<Runtime>,
    /// Capacity of the command channel for each worker
    pub command_channel_capacity: usize,
}

// TODO(yingwen): Add method to stop all workers
/// Write group manages all write worker of a space
#[derive(Debug)]
pub struct WriteGroup {
    /// Space of the write group.
    space_id: SpaceId,
    /// Shared datas of workers.
    worker_datas: Vec<Arc<WorkerSharedData>>,
    /// Join handles of workers.
    handles: Mutex<Vec<JoinHandle<()>>>,
}

impl WriteGroup {
    pub fn new(opts: Options, instance: InstanceRef) -> Self {
        let mut worker_datas = Vec::with_capacity(opts.worker_num);
        let mut handles = Vec::with_capacity(opts.worker_num);
        for id in 0..opts.worker_num {
            let (tx, rx) = mpsc::channel(opts.command_channel_capacity);
            let (background_tx, background_rx) = watch::channel(BackgroundStatus::Ok);

            let data = Arc::new(WorkerSharedData {
                space_id: opts.space_id,
                id,
                tx,
                is_flushing: AtomicBool::new(false),
                background_tx,
                runtime: opts.runtime.clone(),
                num_background_jobs: AtomicI64::new(0),
                background_notify: Notify::new(),
            });

            let mut worker = WriteWorker {
                rx,
                instance: instance.clone(),
                local: WorkerLocal {
                    data: data.clone(),
                    background_rx,
                },
            };

            let space_id = opts.space_id;
            // Spawn a task to run the worker
            let handle = opts.runtime.spawn(async move {
                worker.run().await;

                info!(
                    "Write worker waiting background jobs, space_id:{}, id:{}",
                    space_id, id
                );

                worker.wait_background_jobs_done().await;

                info!("Write worker exit, space_id:{}, id:{}", space_id, id);
            });

            worker_datas.push(data);
            handles.push(handle);
        }

        Self {
            space_id: opts.space_id,
            worker_datas,
            handles: Mutex::new(handles),
        }
    }

    /// Stop the write group.
    pub async fn stop(&self) {
        for data in &self.worker_datas {
            if data.tx.send(Command::Exit).await.is_err() {
                error!(
                    "Failed to send exit command, space_id:{}, worker_id:{}",
                    self.space_id, data.id
                );
            }
        }

        let mut handles = self.handles.lock().await;
        for (i, handle) in handles.iter_mut().enumerate() {
            if let Err(e) = handle.await {
                error!(
                    "Failed to join handle, space_id:{}, index:{}, err:{}",
                    self.space_id, i, e
                );
            }
        }

        // Clear all handles to avoid await again.
        handles.clear();
    }

    /// Choose worker for table with `table_id`. The worker chose should be
    /// consistent, so the caller can cached the handle of the worker
    ///
    /// Returns the WriteHandle of the worker
    pub fn choose_worker(&self, table_id: TableId) -> WriteHandle {
        let index = table_id.as_u64() as usize % self.worker_datas.len();
        let worker_data = self.worker_datas[index].clone();

        WriteHandle { worker_data }
    }

    pub fn worker_num(&self) -> usize {
        self.worker_datas.len()
    }
}

/// Data of write worker
#[derive(Debug)]
struct WorkerSharedData {
    /// Space this worker belongs to
    space_id: SpaceId,
    /// Id of the write worker
    id: usize,
    /// Sender to send command to this worker
    tx: mpsc::Sender<Command>,

    /// Whether the flush job is already running
    ///
    /// When `is_flushing` is true, no more flush job should be scheduled
    is_flushing: AtomicBool,
    /// Channel to notify background status
    background_tx: watch::Sender<BackgroundStatus>,

    /// Background job runtime.
    runtime: Arc<Runtime>,
    /// Numbers of background jobs.
    num_background_jobs: AtomicI64,
    /// Notify when background job finished.
    background_notify: Notify,
}

impl WorkerSharedData {
    fn is_flushing(&self) -> bool {
        self.is_flushing.load(Ordering::Relaxed)
    }

    fn set_is_flushing(&self, is_flushing: bool) {
        self.is_flushing.store(is_flushing, Ordering::Relaxed);
    }
}

/// Table write worker
///
/// Each table is managed by exactly one write worker. Write request to a table
/// will be sent to this thread and done in this worker.
///
/// The write worker should ensure there is only one flush thread (task) is
/// running.
struct WriteWorker {
    /// Command receiver
    rx: mpsc::Receiver<Command>,
    /// Engine instance
    instance: InstanceRef,
    /// Worker local states
    local: WorkerLocal,
}

impl WriteWorker {
    /// Runs the write loop until stopped
    async fn run(&mut self) {
        // TODO(yingwen): Maybe batch write tasks to improve performance (group commit)
        loop {
            let command = match self.rx.recv().await {
                Some(cmd) => cmd,
                None => {
                    info!(
                        "Write worker recv None, exit, space_id:{}, id:{}",
                        self.space_id(),
                        self.id()
                    );
                    return;
                }
            };

            match command {
                Command::Write(cmd) => {
                    self.handle_write_table(cmd).await;
                }
                Command::Create(cmd) => {
                    self.handle_create_table(cmd).await;
                }
                Command::Drop(cmd) => {
                    self.handle_drop_table(cmd).await;
                }
                Command::Recover(cmd) => {
                    self.handle_recover_table(cmd).await;
                }
                Command::Close(cmd) => {
                    self.handle_close_table(cmd).await;
                }
                Command::AlterSchema(cmd) => {
                    self.handle_alter_schema(cmd).await;
                }
                Command::AlterOptions(cmd) => {
                    self.handle_alter_options(cmd).await;
                }
                Command::Flush(cmd) => {
                    self.handle_flush_table(cmd).await;
                }
                Command::Compact(cmd) => {
                    self.handle_compact_table(cmd).await;
                }
                Command::Exit => {
                    info!(
                        "Write worker recv Command::Exit, exit, space_id:{}, id:{}",
                        self.space_id(),
                        self.id()
                    );
                    return;
                }
            }
        }
    }

    async fn wait_background_jobs_done(&self) {
        while self.num_background_jobs() > 0 {
            self.wait_for_notify().await;
        }
    }

    async fn handle_write_table(&mut self, cmd: WriteTableCommand) {
        let WriteTableCommand {
            space,
            table_data,
            request,
            tx,
        } = cmd;

        let write_res = self
            .instance
            .process_write_table_command(
                &mut self.local,
                &space,
                &table_data,
                request,
                write::TableWritePolicy::Unknown,
            )
            .await;
        if let Err(res) = tx.send(write_res) {
            error!(
                "handle write table failed to send result, write_res:{:?}",
                res
            );
        }
    }

    async fn handle_recover_table(&mut self, cmd: RecoverTableCommand) {
        let RecoverTableCommand {
            space,
            table_data,
            tx,
            replay_batch_size,
        } = cmd;

        let open_res = self
            .instance
            .process_recover_table_command(&mut self.local, space, table_data, replay_batch_size)
            .await;

        if let Err(open_res) = tx.send(open_res) {
            error!(
                "handle open table failed to send result, open_res:{:?}",
                open_res
            );
        }
    }

    async fn handle_close_table(&mut self, cmd: CloseTableCommand) {
        let CloseTableCommand { space, request, tx } = cmd;

        let close_res = self
            .instance
            .process_close_table_command(&mut self.local, space, request)
            .await;
        if let Err(close_res) = tx.send(close_res) {
            error!(
                "handle close table failed to send result, close:{:?}",
                close_res
            );
        }
    }

    async fn handle_create_table(&mut self, cmd: CreateTableCommand) {
        let CreateTableCommand {
            space,
            table_data,
            tx,
        } = cmd;

        let create_res = self
            .instance
            .process_create_table_command(&mut self.local, space, table_data)
            .await;
        if let Err(create_res) = tx.send(create_res) {
            error!(
                "handle create table failed to send result, create_res:{:?}",
                create_res
            );
        }
    }

    async fn handle_drop_table(&mut self, cmd: DropTableCommand) {
        let DropTableCommand { space, request, tx } = cmd;

        let drop_res = self
            .instance
            .process_drop_table_command(&mut self.local, space, request)
            .await;
        if let Err(res) = tx.send(drop_res) {
            error!(
                "handle drop table failed to send result, drop_res:{:?}",
                res
            );
        }
    }

    async fn handle_alter_schema(&mut self, cmd: AlterSchemaCommand) {
        let AlterSchemaCommand {
            table_data,
            request,
            tx,
        } = cmd;

        let alter_res = self
            .instance
            .process_alter_schema_command(
                &mut self.local,
                &table_data,
                request,
                TableAlterSchemaPolicy::Unknown,
            )
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            .context(Channel);
        if let Err(res) = tx.send(alter_res) {
            error!(
                "handle alter schema failed to send result, alter_res:{:?}",
                res
            );
        }
    }

    async fn handle_alter_options(&mut self, cmd: AlterOptionsCommand) {
        let AlterOptionsCommand {
            table_data,
            options,
            tx,
        } = cmd;

        let alter_res = self
            .instance
            .process_alter_options_command(&mut self.local, &table_data, options)
            .await;
        if let Err(res) = tx.send(alter_res) {
            error!(
                "handle alter schema failed to send result, alter_res:{:?}",
                res
            );
        }
    }

    async fn handle_flush_table(&mut self, cmd: FlushTableCommand) {
        let FlushTableCommand {
            table_data,
            flush_opts,
            tx,
        } = cmd;

        let flush_res = self
            .instance
            .flush_table_in_worker(&mut self.local, &table_data, flush_opts)
            .await;
        if let Err(res) = tx.send(flush_res) {
            error!(
                "handle flush table failed to send result, flush_res:{:?}",
                res
            );
        }
    }

    async fn handle_compact_table(&mut self, cmd: CompactTableCommand) {
        let CompactTableCommand {
            table_data,
            waiter,
            tx,
        } = cmd;

        let request = TableCompactionRequest {
            table_data,
            compaction_notifier: self.local.compaction_notifier(),
            waiter,
        };

        self.instance.schedule_table_compaction(request).await;
        if let Err(_res) = tx.send(Ok(())) {
            error!("handle compact table failed to send result");
        }
    }

    #[inline]
    fn space_id(&self) -> SpaceId {
        self.local.data.space_id
    }

    #[inline]
    fn id(&self) -> usize {
        self.local.data.id
    }

    #[inline]
    fn num_background_jobs(&self) -> i64 {
        self.local.data.num_background_jobs.load(Ordering::SeqCst)
    }

    async fn wait_for_notify(&self) {
        self.local.data.background_notify.notified().await;
    }
}

#[cfg(test)]
pub mod tests {
    use common_util::runtime;

    use super::*;

    pub struct MockedWriteHandle {
        pub write_handle: WriteHandle,
        pub rx: mpsc::Receiver<Command>,
        pub worker_local: WorkerLocal,
    }

    pub struct WriteHandleMocker {
        space_id: SpaceId,
        runtime: Option<Arc<Runtime>>,
    }

    impl Default for WriteHandleMocker {
        fn default() -> Self {
            Self {
                space_id: 1,
                runtime: None,
            }
        }
    }

    impl WriteHandleMocker {
        pub fn space_id(mut self, space_id: SpaceId) -> Self {
            self.space_id = space_id;
            self
        }

        pub fn build(self) -> MockedWriteHandle {
            let (tx, rx) = mpsc::channel(1);
            let (background_tx, background_rx) = watch::channel(BackgroundStatus::Ok);
            let runtime = self.runtime.unwrap_or_else(|| {
                let rt = runtime::Builder::default().build().unwrap();
                Arc::new(rt)
            });

            let worker_data = Arc::new(WorkerSharedData {
                space_id: self.space_id,
                id: 0,
                tx,
                is_flushing: AtomicBool::new(false),
                background_tx,
                runtime,
                num_background_jobs: AtomicI64::new(0),
                background_notify: Notify::new(),
            });

            let write_handle = WriteHandle {
                worker_data: worker_data.clone(),
            };

            MockedWriteHandle {
                write_handle,
                rx,
                worker_local: WorkerLocal {
                    data: worker_data,
                    background_rx,
                },
            }
        }
    }
}
