// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Region context

use std::{
    cmp,
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use common_types::{bytes::BytesMut, table::TableId, SequenceNumber};
use generic_error::{BoxError, GenericError};
use log::{debug, warn};
use macros::define_result;
use message_queue::{MessageQueue, Offset};
use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};
use tokio::sync::{Mutex, RwLock};

use crate::{
    kv_encoder::{CommonLogEncoding, CommonLogKey},
    log_batch::LogWriteBatch,
    manager::{self},
    message_queue_impl,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to mark deleted for table, region id:{} table id:{}, msg:{}\nBacktrace:\n{}",
        region_id,
        table_id,
        msg,
        backtrace
    ))]
    MarkDeleteTo {
        region_id: u64,
        table_id: TableId,
        msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to get table meta data, table meta not found, region id:{}, table id:{}\nBacktrace:\n{}",
        region_id,
        table_id,
        backtrace
    ))]
    GetTableMeta {
        region_id: u64,
        table_id: TableId,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to build region meta, msg:{}, \nBacktrace:\n{}",
        msg,
        backtrace
    ))]
    Build { msg: String, backtrace: Backtrace },

    #[snafu(display(
        "Failed to write logs of table, region_id:{}, table id:{}, msg:{}, err:{}",
        region_id,
        table_id,
        msg,
        source
    ))]
    WriteWithCause {
        region_id: u64,
        table_id: TableId,
        msg: String,
        source: GenericError,
    },

    #[snafu(display(
        "Failed to write logs of table with no cause, region_id:{}, table id:{}, msg:{}, \nBacktrace:\n{}",
        region_id,
        table_id,
        msg,
        backtrace
    ))]
    WriteNoCause {
        region_id: u64,
        table_id: TableId,
        msg: String,
        backtrace: Backtrace,
    },
}

define_result!(Error);

/// Context for `Region`, it just can be built by its [RegionContextBuilder]
#[derive(Default, Debug)]
pub struct RegionContext {
    /// Id of region
    region_id: u64,

    /// Region context inner
    inner: RwLock<RegionContextInner>,
}

impl RegionContext {
    /// Write table logs by [TableWriter].
    pub async fn write_table_logs<M: MessageQueue>(
        &self,
        ctx: &manager::WriteContext,
        log_batch: &LogWriteBatch,
        table_write_ctx: &TableWriteContext<M>,
    ) -> Result<SequenceNumber> {
        let table_id = log_batch.location.table_id;

        {
            let inner = self.inner.read().await;
            if let Some(table_context) = inner.table_contexts.get(&table_id) {
                return table_context
                    .write_logs(ctx, self.region_id, table_id, log_batch, table_write_ctx)
                    .await;
            }
        };

        let mut inner = self.inner.write().await;
        let table_context = inner
            .table_contexts
            .entry(table_id)
            .or_insert_with(|| TableContext::new(TableMeta::new(table_id)));

        table_context
            .write_logs(ctx, self.region_id, table_id, log_batch, table_write_ctx)
            .await
    }

    /// Mark the deleted sequence number to latest next sequence number.
    pub async fn mark_table_delete_to(
        &self,
        table_id: TableId,
        sequence_num: SequenceNumber,
    ) -> Result<()> {
        let inner = self.inner.read().await;
        let table_meta = inner
            .table_contexts
            .get(&table_id)
            .with_context(|| MarkDeleteTo {
                region_id: self.region_id,
                table_id,
                msg: format!(
                    "table not found while mark it delete to, table id:{table_id}, sequence number:{sequence_num}"
                ),
            })?;

        table_meta
            .mark_delete_to(self.region_id, sequence_num)
            .await?;

        Ok(())
    }

    /// Scan the table meta entry in it and get the snapshot about tables' meta
    /// data.
    ///
    /// NOTICE: Need to freeze the whole region meta on high-level before
    /// calling.
    pub async fn make_snapshot(&self) -> RegionMetaSnapshot {
        let inner = self.inner.read().await;
        // Calc the min offset in message queue.
        let mut entries = Vec::with_capacity(inner.table_contexts.len());
        for table_meta in inner.table_contexts.values() {
            let meta_data = table_meta.get_meta_data().await;
            entries.push(meta_data);
        }

        RegionMetaSnapshot { entries }
    }

    /// Get table meta data by table id.
    pub async fn get_table_meta_data(&self, table_id: TableId) -> Result<Option<TableMetaData>> {
        let inner = self.inner.read().await;

        if let Some(table_meta) = inner.table_contexts.get(&table_id) {
            Ok(Some(table_meta.get_meta_data().await))
        } else {
            Ok(None)
        }
    }

    pub fn region_id(&self) -> u64 {
        self.region_id
    }
}

/// Region meta data.
#[derive(Default, Debug)]
struct RegionContextInner {
    table_contexts: HashMap<TableId, TableContext>,
}

/// Table context
#[derive(Debug)]
struct TableContext {
    meta: TableMeta,
    writer: Mutex<TableWriter>,
}

impl TableContext {
    fn new(meta: TableMeta) -> Self {
        Self {
            meta,
            writer: Mutex::new(TableWriter),
        }
    }

    async fn write_logs<M: MessageQueue>(
        &self,
        ctx: &manager::WriteContext,
        region_id: u64,
        table_id: TableId,
        log_batch: &LogWriteBatch,
        table_write_ctx: &TableWriteContext<M>,
    ) -> Result<SequenceNumber> {
        let writer = self.writer.lock().await;
        writer
            .write(
                &self.meta,
                ctx,
                region_id,
                table_id,
                log_batch,
                table_write_ctx,
            )
            .await
    }

    async fn mark_delete_to(&self, region_id: u64, sequence_num: SequenceNumber) -> Result<()> {
        let writer = self.writer.lock().await;
        writer
            .mark_delete_to(region_id, &self.meta, sequence_num)
            .await
    }

    async fn get_meta_data(&self) -> TableMetaData {
        self.meta.get_meta_data().await
    }
}

/// Wrapper for the [TableMetaInner].
#[derive(Debug)]
struct TableMeta {
    table_id: TableId,
    /// The race condition may occur between writer thread
    /// and background flush thread.
    inner: Mutex<TableMetaInner>,
}

impl TableMeta {
    fn new(table_id: TableId) -> Self {
        Self {
            table_id,
            inner: Mutex::new(TableMetaInner::default()),
        }
    }

    #[inline]
    async fn prepare_for_write(&self) -> SequenceNumber {
        self.get_meta_data().await.next_sequence_num
    }

    async fn update_after_write(&self, write_offset_range: OffsetRange) {
        let updated_num = (write_offset_range.end - write_offset_range.start + 1) as u64;
        let mut inner = self.inner.lock().await;
        let old_next_sequence_num = inner.next_sequence_num;
        let next_sequence_num = old_next_sequence_num + updated_num;

        // Update:
        // + update `next_sequence_num`
        // + update `start_sequence_offset_mapping`
        // + update `current_high_watermark`
        inner.next_sequence_num = next_sequence_num;

        let sequences = old_next_sequence_num..next_sequence_num;
        let offsets = write_offset_range.start..write_offset_range.end + 1;
        inner
            .start_sequence_offset_mapping
            .extend(sequences.into_iter().zip(offsets.into_iter()));

        inner.current_high_watermark = write_offset_range.end + 1;
    }

    async fn mark_delete_to(
        &self,
        sequence_num: SequenceNumber,
    ) -> std::result::Result<(), String> {
        let mut inner = self.inner.lock().await;

        // Check the set sequence num's validity.
        if sequence_num > inner.next_sequence_num {
            return Err(format!(
                "latest marked deleted should be less than or 
                equal to next sequence number, now are:{} and {}",
                sequence_num, inner.next_sequence_num
            ));
        }

        if sequence_num < inner.latest_marked_deleted {
            return Err(format!("latest marked deleted should be greater than or equal to origin one now are:{} and {}",
                    sequence_num,
                    inner.latest_marked_deleted));
        }

        // The `start_sequence_offset_mapping` is possible to be incomplete during
        // recovery.
        let offset = inner.start_sequence_offset_mapping.get(&sequence_num);
        if offset.is_none() && inner.next_sequence_num != sequence_num {
            warn!("Start sequence offset mapping is incomplete, 
            just not update the marked deleted sequence in this flush, new marked deleted, sequence num:{}, previous:{}",
                sequence_num, inner.latest_marked_deleted);

            return Ok(());
        }

        inner.latest_marked_deleted = sequence_num;

        // Update the mapping, keep the range in description.
        inner
            .start_sequence_offset_mapping
            .retain(|k, _| k >= &sequence_num);

        Ok(())
    }

    async fn get_meta_data(&self) -> TableMetaData {
        let inner = self.inner.lock().await;

        // Only two situations exist:
        // + no log of the table has ever been written(after init and flush)
        //  (next sequence num == latest marked deleted).
        // + some logs have been written(after init and flush)
        //  (next_sequence_num > latest_marked_deleted).
        if inner.next_sequence_num == inner.latest_marked_deleted {
            TableMetaData {
                table_id: self.table_id,
                next_sequence_num: inner.next_sequence_num,
                latest_marked_deleted: inner.latest_marked_deleted,
                current_high_watermark: inner.current_high_watermark,
                safe_delete_offset: None,
            }
        } else {
            let offset = inner
                .start_sequence_offset_mapping
                .get(&inner.latest_marked_deleted);

            // Its inner state has been invalid now, it's proper to panic for protecting the
            // data.
            assert!(
                inner.next_sequence_num > inner.latest_marked_deleted,
                "next sequence should be greater than latest marked deleted, but now are {} and {}",
                inner.next_sequence_num,
                inner.latest_marked_deleted
            );
            assert!(
                offset.is_some(),
                "offset not found, but now next sequence num:{}, latest marked deleted:{}, mapping:{:?}",
                inner.next_sequence_num,
                inner.latest_marked_deleted,
                inner.start_sequence_offset_mapping
            );

            TableMetaData {
                table_id: self.table_id,
                next_sequence_num: inner.next_sequence_num,
                latest_marked_deleted: inner.latest_marked_deleted,
                current_high_watermark: inner.current_high_watermark,
                safe_delete_offset: offset.copied(),
            }
        }
    }
}

/// Table meta data, will be updated atomically by mutex.
#[derive(Debug)]
struct TableMetaInner {
    /// Next sequence number for the new log.
    ///
    /// It will be updated while having pushed logs successfully.
    next_sequence_num: SequenceNumber,

    /// The latest marked deleted sequence number. The log with
    /// a sequence number smaller than it can be deleted safely.
    ///
    /// It will be updated while having flushed successfully.
    latest_marked_deleted: SequenceNumber,

    /// The high watermark after this table's latest writing.
    current_high_watermark: Offset,

    /// Map the start sequence number to start offset in every write.
    ///
    /// It will be removed to the mark deleted sequence number after flushing.
    start_sequence_offset_mapping: BTreeMap<SequenceNumber, Offset>,
}

/// Self defined default implementation
///
/// Because `SequenceNumber::MIN` is used as a special value, the normal value
/// should start from `SequenceNumber::MIN` + 1.
impl Default for TableMetaInner {
    fn default() -> Self {
        Self {
            next_sequence_num: SequenceNumber::MIN + 1,
            latest_marked_deleted: SequenceNumber::MIN + 1,
            current_high_watermark: Default::default(),
            start_sequence_offset_mapping: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TableMetaData {
    pub table_id: TableId,
    pub next_sequence_num: SequenceNumber,
    pub latest_marked_deleted: SequenceNumber,
    pub current_high_watermark: Offset,
    pub safe_delete_offset: Option<Offset>,
}

/// Message queue implementation's meta value.
///
/// Include all tables(of current shard) and their next sequence number.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RegionMetaSnapshot {
    pub entries: Vec<TableMetaData>,
}

impl RegionMetaSnapshot {
    pub fn safe_delete_offset(&self) -> Offset {
        let mut safe_delete_offset = Offset::MAX;
        let mut high_watermark = 0;
        // Calc the min offset in message queue.
        for table_meta in &self.entries {
            if let Some(offset) = table_meta.safe_delete_offset {
                safe_delete_offset = cmp::min(safe_delete_offset, offset);
            }
            high_watermark = cmp::max(high_watermark, table_meta.current_high_watermark);
        }

        if safe_delete_offset == Offset::MAX {
            // All tables are in such states: after init/flush, but not written.
            // So, we can directly delete it up to the high_watermark.
            high_watermark
        } else {
            safe_delete_offset
        }
    }
}

/// Message queue's offset range
///
/// The range should be [start, end], and it will never be empty.
#[derive(Debug)]
pub struct OffsetRange {
    pub start: Offset,
    pub end: Offset,
}

impl OffsetRange {
    pub fn new(start: Offset, end: Offset) -> Self {
        Self { start, end }
    }
}

/// Builder for `RegionMeta`
#[derive(Debug)]
pub struct RegionContextBuilder {
    region_id: u64,
    table_metas: HashMap<TableId, TableMetaInner>,
}

impl RegionContextBuilder {
    pub fn new(region_id: u64) -> Self {
        Self {
            region_id,
            table_metas: HashMap::default(),
        }
    }

    pub fn apply_region_meta_snapshot(&mut self, snapshot: RegionMetaSnapshot) -> Result<()> {
        debug!("Apply region meta snapshot, snapshot:{:?}", snapshot);

        for entry in snapshot.entries {
            let old_meta = self
                .table_metas
                .insert(entry.table_id, entry.clone().into());
            ensure!(old_meta.is_none(),
                Build { msg: format!("apply snapshot failed, shouldn't exist duplicated entry in snapshot, duplicated entry:{entry:?}") }
            );
        }

        Ok(())
    }

    pub fn apply_region_meta_delta(&mut self, delta: RegionMetaDelta) -> Result<()> {
        debug!("Apply region meta delta, delta:{:?}", delta);

        // It is likely that snapshot not exist(e.g. no table has ever flushed).
        let mut table_meta = self
            .table_metas
            .entry(delta.table_id)
            .or_insert_with(TableMetaInner::default);

        table_meta.next_sequence_num = delta.sequence_num + 1;
        table_meta.current_high_watermark = delta.offset + 1;

        // Because recover from the `region_safe_delete_offset`, some outdated logs will
        // be loaded.
        if delta.sequence_num >= table_meta.latest_marked_deleted {
            table_meta
                .start_sequence_offset_mapping
                .insert(delta.sequence_num, delta.offset);
        }

        Ok(())
    }

    pub fn build(self) -> RegionContext {
        debug!(
            "Region meta data before building, region meta data:{:?}",
            self.table_metas
        );

        let table_metas = self
            .table_metas
            .into_iter()
            .map(|(table_id, table_meta_inner)| {
                let table_meta = TableMeta {
                    table_id,
                    inner: Mutex::new(table_meta_inner),
                };

                (table_id, TableContext::new(table_meta))
            })
            .collect();

        RegionContext {
            inner: RwLock::new(RegionContextInner {
                table_contexts: table_metas,
            }),
            region_id: self.region_id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RegionMetaDelta {
    table_id: TableId,
    sequence_num: SequenceNumber,
    offset: Offset,
}

impl RegionMetaDelta {
    pub fn new(table_id: TableId, sequence_num: SequenceNumber, offset: Offset) -> Self {
        Self {
            table_id,
            sequence_num,
            offset,
        }
    }
}

impl From<TableMetaData> for TableMetaInner {
    fn from(table_meta_data: TableMetaData) -> Self {
        let mut start_sequence_offset_mapping = BTreeMap::new();
        if let Some(safe_delete_offset) = &table_meta_data.safe_delete_offset {
            start_sequence_offset_mapping
                .insert(table_meta_data.latest_marked_deleted, *safe_delete_offset);
        }

        TableMetaInner {
            next_sequence_num: table_meta_data.next_sequence_num,
            latest_marked_deleted: table_meta_data.latest_marked_deleted,
            current_high_watermark: table_meta_data.current_high_watermark,
            start_sequence_offset_mapping,
        }
    }
}

/// Table's logs writer.
///
/// NOTICE: Will lock it before writing to make writing to a specific table
/// sequentially.
#[derive(Debug)]
pub struct TableWriter;

impl TableWriter {
    async fn write<M: MessageQueue>(
        &self,
        table_meta: &TableMeta,
        _ctx: &manager::WriteContext,
        region_id: u64,
        table_id: TableId,
        log_batch: &LogWriteBatch,
        table_write_ctx: &TableWriteContext<M>,
    ) -> Result<SequenceNumber> {
        ensure!(
            !log_batch.is_empty(),
            WriteNoCause {
                region_id,
                table_id,
                msg: "log batch passed should not be empty"
            }
        );

        let location = &log_batch.location;
        let log_write_entries = &log_batch.entries;

        // Create messages and prepare for write.
        let mut next_sequence_num = table_meta.prepare_for_write().await;

        let mut messages = Vec::with_capacity(log_batch.entries.len());
        let mut key_buf = BytesMut::new();
        for entry in log_write_entries {
            let log_key = CommonLogKey::new(region_id, location.table_id, next_sequence_num);
            table_write_ctx
                .log_encoding
                .encode_key(&mut key_buf, &log_key)
                .box_err()
                .context(WriteWithCause {
                    region_id,
                    table_id,
                    msg: "encode key failed",
                })?;

            let message = message_queue_impl::to_message(key_buf.to_vec(), entry.payload.clone());
            messages.push(message);

            next_sequence_num += 1;
        }

        // Write.
        let offsets = table_write_ctx
            .message_queue
            .produce(&table_write_ctx.log_topic, messages)
            .await
            .box_err()
            .context(WriteWithCause {
                region_id,
                table_id,
                msg: "produce messages failed",
            })?;

        ensure!(
            !offsets.is_empty(),
            WriteNoCause {
                region_id,
                table_id,
                msg: "returned offsets after producing to message queue shouldn't be empty"
            }
        );

        debug!(
            "Produce to topic success, ctx:{:?}, region id:{}, location:{:?}, topic:{}",
            _ctx, region_id, log_batch.location, table_write_ctx.log_topic,
        );

        // Update after write.
        let offset_range = OffsetRange::new(*offsets.first().unwrap(), *offsets.last().unwrap());
        ensure!(
            offset_range.start <= offset_range.end,
            WriteNoCause {
                region_id,
                table_id,
                msg: format!("invalid offset range, offset range:{offset_range:?}"),
            }
        );

        table_meta.update_after_write(offset_range).await;

        Ok(next_sequence_num - 1)
    }

    async fn mark_delete_to(
        &self,
        region_id: u64,
        table_meta: &TableMeta,
        sequence_num: SequenceNumber,
    ) -> Result<()> {
        if let Err(e) = table_meta.mark_delete_to(sequence_num).await {
            MarkDeleteTo {
                region_id,
                table_id: table_meta.table_id,
                msg: e,
            }
            .fail()
        } else {
            Ok(())
        }
    }
}

pub struct TableWriteContext<M: MessageQueue> {
    pub log_encoding: CommonLogEncoding,
    pub log_topic: String,
    pub message_queue: Arc<M>,
}
