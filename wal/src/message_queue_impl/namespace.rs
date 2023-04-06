// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Namespace of wal on message queue

use std::{collections::HashMap, fmt, sync::Arc, time::Duration};

use common_types::SequenceNumber;
use common_util::{
    define_result,
    runtime::Runtime,
    timed_task::{TaskHandle, TimedTask},
};
use log::{debug, error, info};
use message_queue::{ConsumeIterator, MessageQueue};
use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};
use tokio::sync::RwLock;

use crate::{
    kv_encoder::LogEncoding,
    log_batch::{LogEntry, LogWriteBatch},
    manager::{
        ReadContext, ReadRequest, RegionId, ScanContext, ScanRequest, WalLocation, WriteContext,
    },
    message_queue_impl::{
        config::Config,
        encoding::MetaEncoding,
        region::{self, MessageQueueLogIterator, Region},
    },
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to open region, namespace:{}, location:{:?}, err:{}",
        namespace,
        location,
        source
    ))]
    GetSequence {
        namespace: String,
        location: WalLocation,
        source: region::Error,
    },

    #[snafu(display(
        "Failed to open region, namespace:{}, request:{:?}, err:{}",
        namespace,
        request,
        source
    ))]
    ReadWithCause {
        namespace: String,
        request: ReadRequest,
        msg: String,
        source: region::Error,
    },

    #[snafu(display(
        "Failed to open region, namespace:{}, request:{:?}, \nBacktrace:\n{}",
        namespace,
        request,
        backtrace,
    ))]
    ReadNoCause {
        namespace: String,
        request: ReadRequest,
        msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to open region, namespace:{}, request:{:?}, err:{}",
        namespace,
        request,
        source
    ))]
    ScanWithCause {
        namespace: String,
        request: ScanRequest,
        msg: String,
        source: region::Error,
    },

    #[snafu(display(
        "Failed to open region, namespace:{}, request:{:?}, \nBacktrace:\n{}",
        namespace,
        request,
        backtrace,
    ))]
    ScanNoCause {
        namespace: String,
        request: ReadRequest,
        msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to open region, namespace:{}, location:{:?}, batch_size:{}, err:{}",
        namespace,
        location,
        batch_size,
        source
    ))]
    Write {
        namespace: String,
        location: WalLocation,
        batch_size: usize,
        source: region::Error,
    },

    #[snafu(display(
        "Failed to open region, namespace:{}, location:{:?}, sequence_num:{}, err:{}",
        namespace,
        location,
        sequence_num,
        source
    ))]
    MarkDeleteTo {
        namespace: String,
        location: WalLocation,
        sequence_num: SequenceNumber,
        source: region::Error,
    },

    #[snafu(display("Failed to clean logs, namespace:{}, err:{}", namespace, source))]
    CleanLogs {
        namespace: String,
        source: region::Error,
    },

    #[snafu(display("Failed to close namespace, namespace:{}, err:{}", namespace, source))]
    Close {
        namespace: String,
        source: common_util::runtime::Error,
    },
}

define_result!(Error);

pub struct Namespace<M: MessageQueue> {
    /// Namespace inner
    inner: Arc<NamespaceInner<M>>,

    /// Handle for cleaner routine
    cleaner_handle: TaskHandle,
}

impl<M: MessageQueue> Namespace<M> {
    /// Open namespace
    ///
    /// Mainly start logs cleaning routine.
    pub fn open(
        namespace: String,
        message_queue: Arc<M>,
        bg_runtime: Arc<Runtime>,
        config: Config,
    ) -> Self {
        let inner = Arc::new(NamespaceInner::new(namespace, message_queue));
        let cleaner_handle =
            start_log_cleaner(bg_runtime.as_ref(), config.clean_period.0, inner.clone());

        Self {
            inner,
            cleaner_handle,
        }
    }

    pub async fn close_region(&self, region_id: RegionId) -> Result<()> {
        let mut regions = self.inner.regions.write().await;
        regions.remove(&region_id);
        Ok(())
    }

    /// Close namespace
    ///
    /// Mainly clear the regions and wait logs cleaning routine to stop.
    pub async fn close(&self) -> Result<()> {
        let mut regions = self.inner.regions.write().await;
        regions.clear();

        self.cleaner_handle.stop_task().await.context(Close {
            namespace: self.inner.namespace.clone(),
        })
    }

    /// Get table's sequence number.
    pub async fn sequence_num(&self, location: WalLocation) -> Result<SequenceNumber> {
        self.inner.sequence_num(location).await
    }

    /// Scan logs of specific table from region.
    ///
    /// You can define the read range in `ReadRequest`.
    pub async fn read(
        &self,
        ctx: &ReadContext,
        request: &ReadRequest,
    ) -> Result<ReadTableIterator<M::ConsumeIterator>> {
        self.inner.read(ctx, request).await
    }

    /// Scan all logs from a `Region`.
    pub async fn scan(
        &self,
        ctx: &ScanContext,
        request: &ScanRequest,
    ) -> Result<ScanRegionIterator<M::ConsumeIterator>> {
        self.inner.scan(ctx, request).await
    }

    /// Write a batch of log entries to region.
    ///
    /// Returns the max sequence number for the batch of log entries.
    pub async fn write(
        &self,
        ctx: &WriteContext,
        log_batch: &LogWriteBatch,
    ) -> Result<SequenceNumber> {
        self.inner.write(ctx, log_batch).await
    }

    /// Mark the logs of table in the region whose sequence number is in [0,
    /// `sequence_number`] to be deleted in the future.
    pub async fn mark_delete_to(
        &self,
        location: WalLocation,
        sequence_num: SequenceNumber,
    ) -> Result<()> {
        self.inner.mark_delete_to(location, sequence_num).await
    }
}

// TODO: more information should be included.
// Need a solution to get information of regions guarded by tokio's `RwLock`.
// Also implement `Debug` for `MessageQueue`.
impl<M: MessageQueue> fmt::Debug for Namespace<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Namespace")
            .field("namespace", &self.inner.namespace)
            .field("message_queue", &self.inner.message_queue)
            .field("meta_encoding", &self.inner.meta_encoding)
            .field("log_encoding", &self.inner.log_encoding)
            .finish()
    }
}

struct NamespaceInner<M: MessageQueue> {
    namespace: String,
    // TODO: should use some strategies(such as lru) to clean the invalid region.
    regions: Arc<RwLock<HashMap<RegionId, RegionRef<M>>>>,
    message_queue: Arc<M>,
    meta_encoding: MetaEncoding,
    log_encoding: LogEncoding,
}

impl<M: MessageQueue> NamespaceInner<M> {
    pub fn new(namespace: String, message_queue: Arc<M>) -> Self {
        Self {
            namespace,
            regions: Default::default(),
            message_queue,
            meta_encoding: MetaEncoding::newest(),
            log_encoding: LogEncoding::newest(),
        }
    }

    // TODO: If it is a region just initialized or not exists, return None.
    /// Get or open the region of `region_id`.
    ///
    /// NOTICE: If the region doesn't exist yet, it will be created no matter
    /// the high-level caller is `write`, `scan`, `read_batch` or
    /// `mark_delete_entries_up_to`. So we should consider situations
    /// about above operations on an empty region.
    async fn get_or_open_region(
        &self,
        region_id: RegionId,
    ) -> std::result::Result<RegionRef<M>, region::Error> {
        {
            let regions = self.regions.read().await;
            if let Some(region) = regions.get(&region_id) {
                debug!(
                    "Region exists and return it, namespace:{}, region id:{:?}",
                    self.namespace, region_id
                );
                return Ok(region.clone());
            }
        }

        let mut regions = self.regions.write().await;
        // Multiple tables share one region, so double check here is needed.
        if let Some(region) = regions.get(&region_id) {
            debug!(
                "Region exists and return it, namespace:{}, region id:{:?}",
                self.namespace, region_id
            );
            return Ok(region.clone());
        }

        let region =
            Arc::new(Region::open(&self.namespace, region_id, self.message_queue.clone()).await?);
        regions.insert(region_id, region.clone());

        info!(
            "Region open successfully, namespace:{}, region id:{:?}",
            self.namespace, region_id
        );

        Ok(region)
    }

    pub async fn sequence_num(&self, location: WalLocation) -> Result<SequenceNumber> {
        let region = self
            .get_or_open_region(location.region_id)
            .await
            .context(GetSequence {
                namespace: self.namespace.clone(),
                location,
            })?;

        let sequence_num = region
            .get_table_meta(location.table_id)
            .await
            .context(GetSequence {
                namespace: self.namespace.clone(),
                location,
            })?
            .map_or(SequenceNumber::MIN, |table_meta| {
                table_meta.next_sequence_num - 1
            });

        Ok(sequence_num)
    }

    pub async fn read(
        &self,
        ctx: &ReadContext,
        request: &ReadRequest,
    ) -> Result<ReadTableIterator<M::ConsumeIterator>> {
        debug!(
            "Read table logs in namespace, namespace:{}, ctx:{:?}, request:{:?}",
            self.namespace, ctx, request
        );

        let region = self
            .get_or_open_region(request.location.region_id)
            .await
            .context(ReadWithCause {
                namespace: self.namespace.clone(),
                request: request.clone(),
                msg: "failed while creating iterator",
            })?;

        let iter = region
            .scan_table(request.location.table_id, ctx)
            .await
            .context(ReadWithCause {
                namespace: self.namespace.clone(),
                request: request.clone(),
                msg: "failed while creating iterator",
            })?;

        match iter {
            Some(iter) => ReadTableIterator::new(self.namespace.clone(), iter, request.clone()),

            None => Ok(ReadTableIterator::new_empty()),
        }
    }

    pub async fn scan(
        &self,
        ctx: &ScanContext,
        request: &ScanRequest,
    ) -> Result<ScanRegionIterator<M::ConsumeIterator>> {
        info!(
            "Scan region logs in namespace, namespace:{}, ctx:{:?}, request:{:?}",
            self.namespace, ctx, request
        );

        let region = self
            .get_or_open_region(request.region_id)
            .await
            .context(ScanWithCause {
                namespace: self.namespace.clone(),
                request: request.clone(),
                msg: "failed while creating iterator",
            })?;

        let iter = region.scan_region(ctx).await.context(ScanWithCause {
            namespace: self.namespace.clone(),
            request: request.clone(),
            msg: "failed while creating iterator",
        })?;

        match iter {
            Some(iter) => Ok(ScanRegionIterator::new(
                self.namespace.clone(),
                iter,
                request.clone(),
            )),

            None => Ok(ScanRegionIterator::new_empty()),
        }
    }

    pub async fn write(
        &self,
        ctx: &WriteContext,
        log_batch: &LogWriteBatch,
    ) -> Result<SequenceNumber> {
        debug!(
            "Write table logs in namespace, namespace:{}, ctx:{:?}, location:{:?}, batch size:{}",
            self.namespace,
            ctx,
            log_batch.location,
            log_batch.entries.len()
        );

        let region = self
            .get_or_open_region(log_batch.location.region_id)
            .await
            .context(Write {
                namespace: self.namespace.clone(),
                location: log_batch.location,
                batch_size: log_batch.entries.len(),
            })?;

        region.write(ctx, log_batch).await.context(Write {
            namespace: self.namespace.clone(),
            location: log_batch.location,
            batch_size: log_batch.entries.len(),
        })
    }

    pub async fn mark_delete_to(
        &self,
        location: WalLocation,
        sequence_num: SequenceNumber,
    ) -> Result<()> {
        debug!("Mark table logs delete in namespace, namespace:{}, location:{:?}, delete to sequence number:{}", self.namespace, location, sequence_num);

        let region = self
            .get_or_open_region(location.region_id)
            .await
            .context(MarkDeleteTo {
                namespace: self.namespace.clone(),
                location,
                sequence_num,
            })?;

        region
            .mark_delete_to(location.table_id, sequence_num)
            .await
            .context(MarkDeleteTo {
                namespace: self.namespace.clone(),
                location,
                sequence_num,
            })
    }

    async fn clean_logs(&self) -> Result<()> {
        let regions = { self.regions.read().await.clone() };

        for region in regions.values() {
            region.clean_logs().await.context(CleanLogs {
                namespace: self.namespace.clone(),
            })?;
        }

        Ok(())
    }
}

type RegionRef<M> = Arc<Region<M>>;

/// Iterator of scanning the whole region
#[derive(Debug)]
pub struct ScanRegionIterator<C: ConsumeIterator> {
    /// Namespace name
    namespace: String,

    /// Log iterator
    ///
    /// It can be `None`, when there isn't any log of the table in the region
    /// actually.
    iter: Option<MessageQueueLogIterator<C>>,

    /// Request triggering this scanning
    request: ScanRequest,

    /// Terminated flag
    is_terminated: bool,

    /// See the same problem in https://github.com/CeresDB/ceresdb/issues/120
    current_log_payload: Vec<u8>,
}

impl<C: ConsumeIterator> ScanRegionIterator<C> {
    fn new_empty() -> Self {
        Self {
            namespace: String::default(),
            iter: None,
            request: ScanRequest::default(),
            is_terminated: true,
            current_log_payload: Vec::new(),
        }
    }

    fn new(namespace: String, iter: MessageQueueLogIterator<C>, request: ScanRequest) -> Self {
        Self {
            namespace,
            iter: Some(iter),
            request,
            current_log_payload: Vec::new(),
            is_terminated: false,
        }
    }

    pub async fn next_log_entry(&mut self) -> Result<Option<LogEntry<&'_ [u8]>>> {
        // If terminated, just return.
        if self.is_terminated {
            return Ok(None);
        }

        // If inner iter is `Some`, poll it to get required log entry.
        if let Some(iter) = self.iter.as_mut() {
            iter.next_log_entry()
                .await
                .context(ScanWithCause {
                    namespace: self.namespace.clone(),
                    request: self.request.clone(),
                    msg: "failed while polling log",
                })
                .map(|log_entry| {
                    if log_entry.is_none() {
                        self.is_terminated = true;
                    }

                    log_entry.map(|log_entry| {
                        self.current_log_payload = log_entry.payload.to_owned();
                        LogEntry {
                            table_id: log_entry.table_id,
                            sequence: log_entry.sequence,
                            payload: self.current_log_payload.as_slice(),
                        }
                    })
                })
        } else {
            Ok(None)
        }
    }
}

/// Iterator of reading table among a setting range
#[derive(Debug)]
pub struct ReadTableIterator<C: ConsumeIterator> {
    /// Namespace name
    namespace: String,

    /// Log iterator
    ///
    /// It will return some logs belonging to other tables or belonging to
    /// current table but with a out-of-range sequence number, so we need to
    /// filter before returning.
    /// It can be `None`, while there isn't any log of the table in the region
    /// actually.
    iter: Option<MessageQueueLogIterator<C>>,

    /// Request triggering this reading
    request: ReadRequest,

    /// Start sequence number, read range:[start, end]
    start: SequenceNumber,

    /// End sequence number
    end: SequenceNumber,

    /// Terminated flag
    is_terminated: bool,

    /// See the same problem in https://github.com/CeresDB/ceresdb/issues/120
    previous_value: Vec<u8>,
}

impl<C: ConsumeIterator> ReadTableIterator<C> {
    fn new_empty() -> Self {
        Self {
            namespace: String::default(),
            request: ReadRequest::default(),
            start: 0,
            end: 0,
            is_terminated: true,
            iter: None,
            previous_value: Vec::new(),
        }
    }

    fn new(
        namespace: String,
        iter: MessageQueueLogIterator<C>,
        request: ReadRequest,
    ) -> Result<Self> {
        // Check read range's validity.
        let start = request
            .start
            .as_start_sequence_number()
            .with_context(|| ReadNoCause {
                namespace: namespace.clone(),
                request: request.clone(),
                msg: "invalid read range",
            })?;
        let end = request
            .end
            .as_end_sequence_number()
            .with_context(|| ReadNoCause {
                namespace: namespace.clone(),
                request: request.clone(),
                msg: "invalid read range",
            })?;
        ensure!(
            start <= end,
            ReadNoCause {
                namespace,
                request,
                msg: "invalid read range"
            }
        );

        Ok(Self {
            namespace,
            request,
            start,
            end,
            is_terminated: false,
            iter: Some(iter),
            previous_value: Vec::new(),
        })
    }

    pub async fn next_log_entry(&mut self) -> Result<Option<LogEntry<&'_ [u8]>>> {
        // If terminated, just return.
        if self.is_terminated {
            return Ok(None);
        }

        // If inner iter is `Some`, poll it to get required log entry.
        if let Some(iter) = self.iter.as_mut() {
            let start_sequence = self.start;
            let end_sequence = self.end;
            loop {
                let poll_result = iter.next_log_entry().await.context(ReadWithCause {
                    namespace: self.namespace.clone(),
                    request: self.request.clone(),
                    msg: "failed while polling log",
                })?;

                if let Some(log_entry) = poll_result {
                    // While polling result is `Some`, if:
                    //  + the table id equals to the one in `request`
                    //  + the sequence number of log entry is among [start, end], return this log
                    // entry. Otherwise, just continue return the `log_entry`.
                    // Otherwise, just continue to poll.
                    if log_entry.table_id == self.request.location.table_id
                        && log_entry.sequence >= start_sequence
                        && log_entry.sequence <= end_sequence
                    {
                        self.is_terminated = log_entry.sequence == self.end;
                        self.previous_value = log_entry.payload.to_owned();

                        return Ok(Some(LogEntry {
                            table_id: log_entry.table_id,
                            sequence: log_entry.sequence,
                            payload: &self.previous_value,
                        }));
                    }
                } else {
                    // While polling result is `None`, it represents has finished to poll inner
                    // iter.
                    self.is_terminated = true;

                    break;
                }
            }
        }

        Ok(None)
    }
}

/// Loop all [Region] in [Namespace] and clean deleted logs.
async fn log_cleaner_routine<M: MessageQueue>(inner: Arc<NamespaceInner<M>>) {
    debug!(
        "Periodical log cleaning process start, namespace:{}",
        inner.namespace,
    );

    if let Err(e) = inner.clean_logs().await {
        error!(
            "Failed to clean deleted logs, namespace:{}, err:{}",
            inner.namespace, e,
        );
    }

    debug!(
        "Periodical log cleaning process end, namespace:{}",
        inner.namespace,
    );
}

fn start_log_cleaner<M: MessageQueue>(
    runtime: &Runtime,
    period: Duration,
    inner: Arc<NamespaceInner<M>>,
) -> TaskHandle {
    let name = format!("Log-cleaner-wal-on-mq-{}", inner.namespace);
    let builder = move || {
        let inner = inner.clone();

        log_cleaner_routine(inner)
    };

    TimedTask::start_timed_task(name, runtime, period, builder)
}
