// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

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
use common_types::{request_id::RequestId, time::Timestamp};
use common_util::{
    config::ReadableDuration,
    define_result,
    runtime::{JoinHandle, Runtime},
    time::DurationExt,
};
use log::{debug, error, info, warn};
use serde_derive::Deserialize;
use snafu::{ResultExt, Snafu};
use table_engine::table::TableId;
use tokio::{
    sync::{
        mpsc::{self, Receiver, Sender},
        Mutex,
    },
    time,
};

use crate::{
    compaction::{
        metrics::COMPACTION_PENDING_REQUEST_GAUGE, picker::PickerContext, CompactionTask,
        PickerManager, TableCompactionRequest, WaitError, WaiterNotifier,
    },
    instance::{flush_compaction::TableFlushOptions, Instance, SpaceStore},
    TableOptions,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to join compaction schedule worker, err:{}", source))]
    JoinWorker { source: common_util::runtime::Error },
}

define_result!(Error);

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SchedulerConfig {
    pub schedule_channel_len: usize,
    pub schedule_interval: ReadableDuration,
    pub max_ongoing_tasks: usize,
    pub max_unflushed_duration: ReadableDuration,
}

// TODO(boyan), a better default value?
const MAX_GOING_COMPACTION_TASKS: usize = 8;
const MAX_PENDING_COMPACTION_TASKS: usize = 1024;

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            schedule_channel_len: 16,
            // 30 minutes schedule interval.
            schedule_interval: ReadableDuration(Duration::from_secs(60 * 30)),
            max_ongoing_tasks: MAX_GOING_COMPACTION_TASKS,
            // flush_interval default is 5h.
            max_unflushed_duration: ReadableDuration(Duration::from_secs(60 * 60 * 5)),
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
    async fn schedule_table_compaction(&self, request: TableCompactionRequest);
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

struct OngoingTaskLimit {
    ongoing_tasks: AtomicUsize,
    /// Buffer to hold pending requests
    request_buf: RequestBuf,
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
            if req_buf.len() >= MAX_PENDING_COMPACTION_TASKS {
                while req_buf.len() >= MAX_PENDING_COMPACTION_TASKS {
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
                MAX_PENDING_COMPACTION_TASKS, dropped,
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
            limit: Arc::new(OngoingTaskLimit {
                ongoing_tasks: AtomicUsize::new(0),
                request_buf: RwLock::new(RequestQueue::default()),
            }),
            running: running.clone(),
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

    async fn schedule_table_compaction(&self, request: TableCompactionRequest) {
        let send_res = self.sender.send(ScheduleTask::Request(request)).await;

        if let Err(e) = send_res {
            error!("Compaction scheduler failed to send request, err:{}", e);
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
    limit: Arc<OngoingTaskLimit>,
    running: Arc<AtomicBool>,
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
                    self.do_table_compaction_request(compact_req).await;
                }
            }
            ScheduleTask::Schedule => {
                if self.max_ongoing_tasks > ongoing {
                    let pending = self.limit.drain_requests(self.max_ongoing_tasks - ongoing);
                    let len = pending.len();
                    for compact_req in pending {
                        self.do_table_compaction_request(compact_req).await;
                    }
                    debug!("Scheduled {} pending compaction tasks.", len);
                }
            }
            ScheduleTask::Exit => (),
        };
    }

    async fn do_table_compaction_request(&self, compact_req: TableCompactionRequest) {
        let table_data = compact_req.table_data;
        let compaction_notifier = compact_req.compaction_notifier;
        let waiter_notifier = WaiterNotifier::new(compact_req.waiter);

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

        // Mark files are in compaction.
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
        // Do actual costly compact job in background.
        self.runtime.spawn(async move {
            let res = space_store
                .compact_table(runtime, &table_data, request_id, &compaction_task)
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
                    let new_compaction_notifier = compaction_notifier.clone();
                    compaction_notifier.notify_ok();
                    waiter_notifier.notify_wait_result(Ok(()));

                    if keep_scheduling_compaction {
                        schedule_table_compaction(
                            sender,
                            TableCompactionRequest::no_waiter(
                                table_data.clone(),
                                new_compaction_notifier,
                            ),
                        )
                        .await;
                    }
                }
                Err(e) => {
                    let e = Arc::new(e);
                    compaction_notifier.notify_err(e.clone());
                    let wait_err = WaitError::Compaction { source: e };
                    waiter_notifier.notify_wait_result(Err(wait_err));
                }
            }
        });
    }

    async fn schedule(&mut self) {
        self.purge_tables();
        self.flush_tables().await;
    }

    fn purge_tables(&mut self) {
        let mut tables_buf = Vec::new();
        self.space_store.list_all_tables(&mut tables_buf);

        let mut to_purge = Vec::new();

        let now = Timestamp::now();
        for table_data in &tables_buf {
            let expire_time = table_data
                .table_options()
                .ttl()
                .map(|ttl| now.sub_duration_or_min(ttl.0));

            let version = table_data.current_version();
            if !version.has_expired_sst(expire_time) {
                debug!(
                    "Table has no expired sst, table:{}, table_id:{}, expire_time:{:?}",
                    table_data.name, table_data.id, expire_time
                );

                continue;
            }

            // Create a compaction task that only purge expired files.
            let compaction_task = CompactionTask {
                expired: version.expired_ssts(expire_time),
                ..Default::default()
            };

            // Marks being compacted.
            compaction_task.mark_files_being_compacted(true);

            to_purge.push((table_data.clone(), compaction_task));
        }

        let runtime = self.runtime.clone();
        let space_store = self.space_store.clone();
        let request_id = RequestId::next_id();
        // Spawn a background job to purge ssts and avoid schedule thread blocked.
        self.runtime.spawn(async move {
            for (table_data, compaction_task) in to_purge {
                info!("Period purge expired files, table:{}, table_id:{}, request_id:{}", table_data.name, table_data.id, request_id);

                if let Err(e) = space_store
                    .compact_table(runtime.clone(), &table_data, request_id, &compaction_task)
                    .await
                {
                    error!(
                        "Failed to purge expired files of table, table:{}, table_id:{}, request_id:{}, err:{}",
                        table_data.name, table_data.id, request_id, e
                    );

                    // Unset the compaction mark.
                    compaction_task.mark_files_being_compacted(false);
                }
            }
        });
    }

    async fn flush_tables(&self) {
        let mut tables_buf = Vec::new();
        self.space_store.list_all_tables(&mut tables_buf);

        for table_data in &tables_buf {
            let last_flush_time = table_data.last_flush_time();
            if last_flush_time + self.max_unflushed_duration.as_millis_u64()
                > common_util::time::current_time_millis()
            {
                // Instance flush the table asynchronously.
                if let Err(e) =
                    Instance::flush_table(table_data.clone(), TableFlushOptions::default()).await
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
