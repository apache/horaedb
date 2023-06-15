// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

// Compaction scheduler.

use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, RwLock,
    },
    time::Duration,
};

use async_trait::async_trait;
use common_types::request_id::RequestId;
use common_util::{
    config::{ReadableDuration, ReadableSize},
    define_result,
    runtime::{JoinHandle, Runtime},
    time::DurationExt,
};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use table_engine::table::TableId;
use tokio::{
    sync::{
        mpsc::{self, error::TrySendError, Receiver, Sender},
        Mutex,
    },
    time,
};

use crate::{
    compaction::{
        metrics::COMPACTION_PENDING_REQUEST_GAUGE, picker::PickerContext, CompactionTask,
        PickerManager, TableCompactionRequest, WaitError, WaiterNotifier,
    },
    instance::{
        flush_compaction::{Flusher, TableFlushOptions},
        SpaceStore,
    },
    sst::factory::{ScanOptions, SstWriteOptions},
    table::data::TableDataRef,
    TableOptions,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to join compaction schedule worker, err:{}", source))]
    JoinWorker { source: common_util::runtime::Error },
}

define_result!(Error);

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct SchedulerConfig {
    pub schedule_channel_len: usize,
    pub schedule_interval: ReadableDuration,
    pub max_ongoing_tasks: usize,
    pub max_unflushed_duration: ReadableDuration,
    pub memory_limit: ReadableSize,
    pub max_pending_compaction_tasks: usize,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            schedule_channel_len: 16,
            // 30 seconds schedule interval.
            schedule_interval: ReadableDuration(Duration::from_secs(30)),
            max_ongoing_tasks: 8,
            // flush_interval default is 5h.
            max_unflushed_duration: ReadableDuration(Duration::from_secs(60 * 60 * 5)),
            memory_limit: ReadableSize::gb(4),
            max_pending_compaction_tasks: 1024,
        }
    }
}

enum ScheduleTask {
    Request(TableCompactionRequest),
    Schedule,
    Exit,
}

#[async_trait]
pub trait CompactionScheduler {
    /// Stop the scheduler.
    async fn stop_scheduler(&self) -> Result<()>;

    /// Schedule a compaction job to background workers.
    async fn schedule_table_compaction(&self, request: TableCompactionRequest) -> bool;
}

// A FIFO queue that remove duplicate values by key.
struct RequestQueue<K: Eq + Hash + Clone, V> {
    keys: VecDeque<K>,
    values: HashMap<K, V>,
}

impl<K: Eq + Hash + Clone, V> Default for RequestQueue<K, V> {
    fn default() -> Self {
        Self {
            keys: VecDeque::default(),
            values: HashMap::default(),
        }
    }
}

impl<K: Eq + Hash + Clone, V> RequestQueue<K, V> {
    fn push_back(&mut self, key: K, value: V) -> bool {
        if self.values.insert(key.clone(), value).is_none() {
            self.keys.push_back(key);
            return true;
        }
        false
    }

    fn pop_front(&mut self) -> Option<V> {
        if let Some(key) = self.keys.pop_front() {
            return self.values.remove(&key);
        }
        None
    }

    #[inline]
    fn len(&self) -> usize {
        self.values.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

type RequestBuf = RwLock<RequestQueue<TableId, TableCompactionRequest>>;

/// Combined with [`MemoryUsageToken`], [`MemoryLimit`] provides a mechanism to
/// impose limit on the memory usage.
#[derive(Clone, Debug)]
struct MemoryLimit {
    usage: Arc<AtomicUsize>,
    // TODO: support to adjust this threshold dynamically.
    limit: usize,
}

/// The token for the memory usage, which should not derive Clone.
/// The applied memory will be subtracted from the global memory usage.
#[derive(Debug)]
struct MemoryUsageToken {
    global_usage: Arc<AtomicUsize>,
    applied_usage: usize,
}

impl Drop for MemoryUsageToken {
    fn drop(&mut self) {
        self.global_usage
            .fetch_sub(self.applied_usage, Ordering::Relaxed);
    }
}

impl MemoryLimit {
    fn new(limit: usize) -> Self {
        Self {
            usage: Arc::new(AtomicUsize::new(0)),
            limit,
        }
    }

    /// Try to apply a token if possible.
    fn try_apply_token(&self, bytes: usize) -> Option<MemoryUsageToken> {
        let token = self.apply_token(bytes);
        if self.is_exceeded() {
            None
        } else {
            Some(token)
        }
    }

    fn apply_token(&self, bytes: usize) -> MemoryUsageToken {
        self.usage.fetch_add(bytes, Ordering::Relaxed);

        MemoryUsageToken {
            global_usage: self.usage.clone(),
            applied_usage: bytes,
        }
    }

    #[inline]
    fn is_exceeded(&self) -> bool {
        self.usage.load(Ordering::Relaxed) > self.limit
    }
}

struct OngoingTaskLimit {
    ongoing_tasks: AtomicUsize,
    /// Buffer to hold pending requests
    request_buf: RequestBuf,
    max_pending_compaction_tasks: usize,
}

impl OngoingTaskLimit {
    #[inline]
    fn start_task(&self) {
        self.ongoing_tasks.fetch_add(1, Ordering::SeqCst);
    }

    #[inline]
    fn finish_task(&self) {
        self.ongoing_tasks.fetch_sub(1, Ordering::SeqCst);
    }

    #[inline]
    fn add_request(&self, request: TableCompactionRequest) {
        let mut dropped = 0;

        {
            let mut req_buf = self.request_buf.write().unwrap();

            // Remove older requests
            if req_buf.len() >= self.max_pending_compaction_tasks {
                while req_buf.len() >= self.max_pending_compaction_tasks {
                    req_buf.pop_front();
                    dropped += 1;
                }
                COMPACTION_PENDING_REQUEST_GAUGE.sub(dropped)
            }

            if req_buf.push_back(request.table_data.id, request) {
                COMPACTION_PENDING_REQUEST_GAUGE.add(1)
            }
        }

        if dropped > 0 {
            warn!(
                "Too many compaction pending tasks,  limit: {}, dropped {} older tasks.",
                self.max_pending_compaction_tasks, dropped,
            );
        }
    }

    fn drain_requests(&self, max_num: usize) -> Vec<TableCompactionRequest> {
        let mut result = Vec::with_capacity(max_num);
        let mut req_buf = self.request_buf.write().unwrap();

        while result.len() < max_num {
            if let Some(req) = req_buf.pop_front() {
                result.push(req);
            } else {
                break;
            }
        }
        COMPACTION_PENDING_REQUEST_GAUGE.sub(result.len() as i64);

        result
    }

    #[inline]
    fn has_pending_requests(&self) -> bool {
        !self.request_buf.read().unwrap().is_empty()
    }

    #[inline]
    fn request_buf_len(&self) -> usize {
        self.request_buf.read().unwrap().len()
    }

    #[inline]
    fn ongoing_tasks(&self) -> usize {
        self.ongoing_tasks.load(Ordering::SeqCst)
    }
}

pub type CompactionSchedulerRef = Arc<dyn CompactionScheduler + Send + Sync>;

pub struct SchedulerImpl {
    sender: Sender<ScheduleTask>,
    running: Arc<AtomicBool>,
    handle: Mutex<JoinHandle<()>>,
}

impl SchedulerImpl {
    pub fn new(
        space_store: Arc<SpaceStore>,
        runtime: Arc<Runtime>,
        config: SchedulerConfig,
        write_sst_max_buffer_size: usize,
        scan_options: ScanOptions,
    ) -> Self {
        let (tx, rx) = mpsc::channel(config.schedule_channel_len);
        let running = Arc::new(AtomicBool::new(true));

        let mut worker = ScheduleWorker {
            sender: tx.clone(),
            receiver: rx,
            space_store,
            runtime: runtime.clone(),
            schedule_interval: config.schedule_interval.0,
            picker_manager: PickerManager::default(),
            max_ongoing_tasks: config.max_ongoing_tasks,
            max_unflushed_duration: config.max_unflushed_duration.0,
            write_sst_max_buffer_size,
            scan_options,
            limit: Arc::new(OngoingTaskLimit {
                ongoing_tasks: AtomicUsize::new(0),
                request_buf: RwLock::new(RequestQueue::default()),
                max_pending_compaction_tasks: config.max_pending_compaction_tasks,
            }),
            running: running.clone(),
            memory_limit: MemoryLimit::new(config.memory_limit.as_byte() as usize),
        };

        let handle = runtime.spawn(async move {
            worker.schedule_loop().await;
        });

        Self {
            sender: tx,
            running,
            handle: Mutex::new(handle),
        }
    }
}

#[async_trait]
impl CompactionScheduler for SchedulerImpl {
    async fn stop_scheduler(&self) -> Result<()> {
        self.running.store(false, Ordering::Relaxed);
        // Wake up the receiver, if the channel is full, the worker should be busy and
        // check the running flag later.
        let _ = self.sender.try_send(ScheduleTask::Exit);

        let mut handle = self.handle.lock().await;
        (&mut *handle).await.context(JoinWorker)?;

        Ok(())
    }

    async fn schedule_table_compaction(&self, request: TableCompactionRequest) -> bool {
        let send_res = self.sender.try_send(ScheduleTask::Request(request));

        match send_res {
            Err(TrySendError::Full(_)) => {
                debug!("Compaction scheduler is busy, drop compaction request");
                false
            }
            Err(TrySendError::Closed(_)) => {
                error!("Compaction scheduler is closed, drop compaction request");
                false
            }
            Ok(_) => true,
        }
    }
}

struct OngoingTask {
    limit: Arc<OngoingTaskLimit>,
    sender: Sender<ScheduleTask>,
}

impl OngoingTask {
    async fn schedule_worker_if_need(&self) {
        if self.limit.has_pending_requests() {
            if let Err(e) = self.sender.send(ScheduleTask::Schedule).await {
                error!("Fail to schedule worker, err:{}", e);
            }
        }
    }
}

struct ScheduleWorker {
    sender: Sender<ScheduleTask>,
    receiver: Receiver<ScheduleTask>,
    space_store: Arc<SpaceStore>,
    runtime: Arc<Runtime>,
    schedule_interval: Duration,
    max_unflushed_duration: Duration,
    picker_manager: PickerManager,
    max_ongoing_tasks: usize,
    write_sst_max_buffer_size: usize,
    scan_options: ScanOptions,
    limit: Arc<OngoingTaskLimit>,
    running: Arc<AtomicBool>,
    memory_limit: MemoryLimit,
}

#[inline]
async fn schedule_table_compaction(sender: Sender<ScheduleTask>, request: TableCompactionRequest) {
    if let Err(e) = sender.send(ScheduleTask::Request(request)).await {
        error!("Fail to send table compaction request, err:{}", e);
    }
}

impl ScheduleWorker {
    async fn schedule_loop(&mut self) {
        while self.running.load(Ordering::Relaxed) {
            // TODO(yingwen): Maybe add a random offset to the interval.
            match time::timeout(self.schedule_interval, self.receiver.recv()).await {
                Ok(Some(schedule_task)) => {
                    self.handle_schedule_task(schedule_task).await;
                }
                Ok(None) => {
                    // The channel is disconnected.
                    info!("Channel disconnected, compaction schedule worker exit");
                    break;
                }
                Err(_) => {
                    // Timeout.
                    info!("Periodical compaction schedule start");

                    self.schedule().await;

                    info!("Periodical compaction schedule end");
                }
            }
        }

        info!("Compaction schedule loop exit");
    }

    // This function is called sequentially, so we can mark files in compaction
    // without race.
    async fn handle_schedule_task(&self, schedule_task: ScheduleTask) {
        let ongoing = self.limit.ongoing_tasks();
        match schedule_task {
            ScheduleTask::Request(compact_req) => {
                debug!("Ongoing compaction tasks:{}", ongoing);
                if ongoing >= self.max_ongoing_tasks {
                    self.limit.add_request(compact_req);
                    warn!(
                        "Too many compaction ongoing tasks:{}, max:{}, buf_len:{}",
                        ongoing,
                        self.max_ongoing_tasks,
                        self.limit.request_buf_len()
                    );
                } else {
                    self.handle_table_compaction_request(compact_req).await;
                }
            }
            ScheduleTask::Schedule => {
                if self.max_ongoing_tasks > ongoing {
                    let pending = self.limit.drain_requests(self.max_ongoing_tasks - ongoing);
                    let len = pending.len();
                    for compact_req in pending {
                        self.handle_table_compaction_request(compact_req).await;
                    }
                    debug!("Scheduled {} pending compaction tasks.", len);
                }else {
                    warn!(
                        "Too many compaction ongoing tasks:{}, max:{}, buf_len:{}",
                        ongoing,
                        self.max_ongoing_tasks,
                        self.limit.request_buf_len()
                    );
                }
            }
            ScheduleTask::Exit => (),
        };
    }

    fn do_table_compaction_task(
        &self,
        table_data: TableDataRef,
        compaction_task: CompactionTask,
        waiter_notifier: WaiterNotifier,
        token: MemoryUsageToken,
    ) {
        // Mark files being in compaction.
        compaction_task.mark_files_being_compacted(true);

        let keep_scheduling_compaction = !compaction_task.compaction_inputs.is_empty();

        let runtime = self.runtime.clone();
        let space_store = self.space_store.clone();
        self.limit.start_task();
        let task = OngoingTask {
            sender: self.sender.clone(),
            limit: self.limit.clone(),
        };

        let sender = self.sender.clone();
        let request_id = RequestId::next_id();
        let storage_format_hint = table_data.table_options().storage_format_hint;
        let sst_write_options = SstWriteOptions {
            storage_format_hint,
            num_rows_per_row_group: table_data.table_options().num_rows_per_row_group,
            compression: table_data.table_options().compression,
            max_buffer_size: self.write_sst_max_buffer_size,
        };
        let scan_options = self.scan_options.clone();

        // Do actual costly compact job in background.
        self.runtime.spawn(async move {
            // Release the token after compaction finished.
            let _token = token;

            let res = space_store
                .compact_table(
                    request_id,
                    &table_data,
                    &compaction_task,
                    scan_options,
                    &sst_write_options,
                    runtime,
                )
                .await;

            if let Err(e) = &res {
                // Compaction is failed, we need to unset the compaction mark.
                compaction_task.mark_files_being_compacted(false);

                error!(
                    "Failed to compact table, table_name:{}, table_id:{}, request_id:{}, err:{}",
                    table_data.name, table_data.id, request_id, e
                );
            }

            task.limit.finish_task();
            task.schedule_worker_if_need().await;

            // Notify the background compact table result.
            match res {
                Ok(()) => {
                    waiter_notifier.notify_wait_result(Ok(()));

                    if keep_scheduling_compaction {
                        schedule_table_compaction(
                            sender,
                            TableCompactionRequest::no_waiter(table_data.clone()),
                        )
                        .await;
                    }
                }
                Err(e) => {
                    let e = Arc::new(e);

                    let wait_err = WaitError::Compaction { source: e };
                    waiter_notifier.notify_wait_result(Err(wait_err));
                }
            }
        });
    }

    // Try to apply the memory usage token. Return `None` if the current memory
    // usage exceeds the limit.
    fn try_apply_memory_usage_token_for_task(
        &self,
        task: &CompactionTask,
    ) -> Option<MemoryUsageToken> {
        let input_size = task.estimated_total_input_file_size();
        // Currently sst build is in a streaming way, so it wouldn't consume memory more
        // than its size.
        let estimate_memory_usage = input_size;

        let token = self.memory_limit.try_apply_token(estimate_memory_usage);

        debug!(
            "Apply memory for compaction, current usage:{}, applied:{}, applied_result:{:?}",
            self.memory_limit.usage.load(Ordering::Relaxed),
            estimate_memory_usage,
            token,
        );

        token
    }

    async fn handle_table_compaction_request(&self, compact_req: TableCompactionRequest) {
        let table_data = compact_req.table_data.clone();
        let table_options = table_data.table_options();
        let compaction_strategy = table_options.compaction_strategy;
        let picker = self.picker_manager.get_picker(compaction_strategy);
        let picker_ctx = match new_picker_context(&table_options) {
            Some(v) => v,
            None => {
                warn!("No valid context can be created, compaction request will be ignored, table_id:{}, table_name:{}",
                    table_data.id, table_data.name);
                return;
            }
        };
        let version = table_data.current_version();

        // Pick compaction task.
        let compaction_task = version.pick_for_compaction(picker_ctx, &picker);
        let compaction_task = match compaction_task {
            Ok(v) => v,
            Err(e) => {
                error!(
                    "Compaction scheduler failed to pick compaction, table:{}, table_id:{}, err:{}",
                    table_data.name, table_data.id, e
                );
                // Now the error of picking compaction is considered not fatal and not sent to
                // compaction notifier.
                return;
            }
        };

        let token = match self.try_apply_memory_usage_token_for_task(&compaction_task) {
            Some(v) => v,
            None => {
                // Memory usage exceeds the threshold, let's put pack the
                // request.
                warn!(
                    "Compaction task is ignored, because of high memory usage:{}, task:{:?}, table:{}",
                    self.memory_limit.usage.load(Ordering::Relaxed),
                    compaction_task, table_data.name
                );
                return;
            }
        };

        let waiter_notifier = WaiterNotifier::new(compact_req.waiter);

        self.do_table_compaction_task(table_data, compaction_task, waiter_notifier, token);
    }

    async fn schedule(&mut self) {
        self.compact_tables().await;
        self.flush_tables().await;
    }

    async fn compact_tables(&mut self) {
        let mut tables_buf = Vec::new();
        self.space_store.list_all_tables(&mut tables_buf);

        let request_id = RequestId::next_id();
        for table_data in tables_buf {
            info!(
                "Period purge, table:{}, table_id:{}, request_id:{}",
                table_data.name, table_data.id, request_id
            );

            // This will add a compaction request to queue and avoid schedule thread
            // blocked.
            self.limit
                .add_request(TableCompactionRequest::no_waiter(table_data));
        }
        if let Err(e) = self.sender.send(ScheduleTask::Schedule).await {
            error!("Fail to schedule table compaction request, err:{}", e);
        }
    }

    async fn flush_tables(&self) {
        let mut tables_buf = Vec::new();
        self.space_store.list_all_tables(&mut tables_buf);
        let flusher = Flusher {
            space_store: self.space_store.clone(),
            runtime: self.runtime.clone(),
            write_sst_max_buffer_size: self.write_sst_max_buffer_size,
        };

        for table_data in &tables_buf {
            let last_flush_time = table_data.last_flush_time();
            let flush_deadline_ms = last_flush_time + self.max_unflushed_duration.as_millis_u64();
            let now_ms = common_util::time::current_time_millis();
            if now_ms > flush_deadline_ms {
                info!(
                    "Scheduled flush is triggered, table:{}, last_flush_time:{last_flush_time}ms, max_unflushed_duration:{:?}",
                    table_data.name,
                    self.max_unflushed_duration,
                );

                let mut serial_exec = table_data.serial_exec.lock().await;
                let flush_scheduler = serial_exec.flush_scheduler();
                // Instance flush the table asynchronously.
                if let Err(e) = flusher
                    .schedule_flush(flush_scheduler, table_data, TableFlushOptions::default())
                    .await
                {
                    error!("Failed to flush table, err:{}", e);
                }
            }
        }
    }
}

// If segment duration is None, then no compaction should be triggered, but we
// return a None context instead of panic here.
fn new_picker_context(table_opts: &TableOptions) -> Option<PickerContext> {
    table_opts
        .segment_duration()
        .map(|segment_duration| PickerContext {
            segment_duration,
            ttl: table_opts.ttl().map(|ttl| ttl.0),
            strategy: table_opts.compaction_strategy,
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_usage_limit_apply() {
        let limit = MemoryLimit::new(100);
        let cases = vec![
            // One case is (applied_requests, applied_results).
            (vec![10, 20, 90, 30], vec![true, true, false, true]),
            (vec![100, 10], vec![true, false]),
            (vec![0, 90, 10], vec![true, true, true]),
        ];

        for (apply_requests, expect_applied_results) in cases {
            assert_eq!(limit.usage.load(Ordering::Relaxed), 0);

            let mut applied_tokens = Vec::with_capacity(apply_requests.len());
            for bytes in &apply_requests {
                let token = limit.try_apply_token(*bytes);
                applied_tokens.push(token);
            }
            assert_eq!(applied_tokens.len(), expect_applied_results.len());
            assert_eq!(applied_tokens.len(), applied_tokens.len());

            for (token, (apply_bytes, applied)) in applied_tokens.into_iter().zip(
                apply_requests
                    .into_iter()
                    .zip(expect_applied_results.into_iter()),
            ) {
                if applied {
                    let token = token.unwrap();
                    assert_eq!(token.applied_usage, apply_bytes);
                    assert_eq!(
                        token.global_usage.load(Ordering::Relaxed),
                        limit.usage.load(Ordering::Relaxed),
                    );
                }
            }
        }
    }

    #[test]
    fn test_memory_usage_limit_release() {
        let limit = MemoryLimit::new(100);

        let cases = vec![
            // One case includes the operation consisting of (applied bytes, whether to keep the
            // applied token) and final memory usage.
            (vec![(10, false), (20, false)], 0),
            (vec![(100, false), (10, true), (20, true), (30, true)], 60),
            (vec![(0, false), (100, false), (20, true), (30, false)], 20),
        ];

        for (ops, expect_memory_usage) in cases {
            assert_eq!(limit.usage.load(Ordering::Relaxed), 0);

            let mut tokens = Vec::new();
            for (applied_bytes, keep_token) in ops {
                let token = limit.try_apply_token(applied_bytes);
                if keep_token {
                    tokens.push(token);
                }
            }

            assert_eq!(limit.usage.load(Ordering::Relaxed), expect_memory_usage);
        }
    }

    #[test]
    fn test_request_queue() {
        let mut q: RequestQueue<i32, String> = RequestQueue::default();
        assert!(q.is_empty());
        assert_eq!(0, q.len());

        q.push_back(1, "task1".to_string());
        q.push_back(2, "task2".to_string());
        q.push_back(3, "task3".to_string());

        assert_eq!(3, q.len());
        assert!(!q.is_empty());

        assert_eq!("task1", q.pop_front().unwrap());
        assert_eq!("task2", q.pop_front().unwrap());
        assert_eq!("task3", q.pop_front().unwrap());
        assert!(q.pop_front().is_none());
        assert!(q.is_empty());

        q.push_back(1, "task1".to_string());
        q.push_back(2, "task2".to_string());
        q.push_back(3, "task3".to_string());
        q.push_back(1, "task11".to_string());
        q.push_back(3, "task33".to_string());
        q.push_back(3, "task333".to_string());

        assert_eq!(3, q.len());
        assert_eq!("task11", q.pop_front().unwrap());
        assert_eq!("task2", q.pop_front().unwrap());
        assert_eq!("task333", q.pop_front().unwrap());
        assert!(q.pop_front().is_none());
        assert!(q.is_empty());
        assert_eq!(0, q.len());
    }
}
