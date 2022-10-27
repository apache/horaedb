// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Region meta data

use std::{
    cmp,
    collections::{BTreeMap, HashMap},
    sync::atomic::{AtomicI64, Ordering},
};

use common_types::{table::TableId, SequenceNumber};
use common_util::define_result;
use message_queue::Offset;
use snafu::{ensure, Backtrace, OptionExt, Snafu};
use tokio::sync::{Mutex, RwLock};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to get meta data of table:{}, msg:{}\nBacktrace:{}",
        table_id,
        msg,
        backtrace
    ))]
    GetMetaData {
        table_id: TableId,
        msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to update meta data after write of table:{}, msg:{}\nBacktrace:{}",
        table_id,
        msg,
        backtrace
    ))]
    UpdateAfterWrite {
        table_id: TableId,
        msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to mark deleted for table:{}, msg:{}\nBacktrace:{}",
        table_id,
        msg,
        backtrace
    ))]
    MarkDeleted {
        table_id: TableId,
        msg: String,
        backtrace: Backtrace,
    },
}
define_result!(Error);

// TODO: will be made use of later.
#[allow(unused)]
#[derive(Default)]
pub struct RegionMeta {
    inner: RwLock<RegionMetaInner>,
}

// TODO: will be made use of later.
#[allow(unused)]
impl RegionMeta {
     // TODO: Need to implement the init method using the [RegionMetaSnapshot] which will be persisted.

     pub async fn prepare_for_table_write(&self, table_id: TableId) -> Result<SequenceNumber> {
        {
            let inner = self.inner.read().await;
            if let Some(table_meta) = inner.table_metas.get(&table_id) {
                return Ok(table_meta.get_meta_data().await?.next_sequence_num);
            }
        }

        // Double check is not needed, due to just one task will write the specific
        // table.
        let mut inner = self.inner.write().await;
        debug_assert!(inner
            .table_metas
            .insert(table_id, TableMeta::new(table_id))
            .is_none());
        // New table, so returned next sequence num is zero.
        Ok(SequenceNumber::MIN)
    }

    pub async fn update_after_table_write(
        &self,
        table_id: TableId,
        next_sequence_num: SequenceNumber,
        offset: Offset,
    ) -> Result<()> {
        let inner = self.inner.read().await;
        let table_meta = inner
            .table_metas
            .get(&table_id)
            .with_context(|| UpdateAfterWrite {
                table_id,
                msg: format!(
                    "table:{}'s meta not found while update after its write",
                    table_id
                ),
            })?;
        table_meta
            .update_after_write(next_sequence_num, offset)
            .await;

        Ok(())
    }

    pub  async fn mark_table_deleted(
        &self,
        table_id: TableId,
        next_sequence_num: SequenceNumber,
    ) -> Result<()> {
        let inner = self.inner.read().await;
        let table_meta = inner
            .table_metas
            .get(&table_id)
            .with_context(|| MarkDeleted {
                table_id,
                msg: format!("table:{}'s meta not found while mark its deleted", table_id),
            })?;
        table_meta.mark_deleted(next_sequence_num);

        Ok(())
    }

    /// Scan the table meta entry in it and get the safe(minimum) offset among them to return.
    /// 
    /// NOTICE: Need to freeze the whole region meta on high-level before calling. 
    pub  async fn get_safe_delete_offset(&self) -> Result<Offset> {
        let inner = self.inner.read().await;
        let mut min_offset = Offset::MAX;
        // Calc the min offset in message queue.
        for table_meta in inner.table_metas.values() {
            let meta_data = table_meta.get_meta_data().await?;
            if let Some(offset) = meta_data.safe_deleted_offset {
                min_offset = cmp::min(min_offset, offset);
            }
        }

        if min_offset == Offset::MAX {
            // All tables are in such states:
            // + has init, but not written
            // + has written, but not flushed
            // + has flushed, but not written again
            // So, we can directly delete it up to the latest offset.
            Ok(inner.local_latest_offset.load(Ordering::Relaxed))
        } else {
            Ok(min_offset)
        }
    }

    /// Scan the table meta entry in it and get the snapshot about tables' next sequences.
    /// 
    /// NOTICE: Need to freeze the whole region meta on high-level before calling. 
    pub async fn get_snapshot(&self) -> Result<RegionMetaSnapshot> {
        let inner = self.inner.read().await;
        // Calc the min offset in message queue.
        let mut entries = Vec::with_capacity(inner.table_metas.len());
        for (table_id, table_meta) in &inner.table_metas {
            let meta_data = table_meta.get_meta_data().await?;
            entries.push(meta_data);
        }

        Ok(RegionMetaSnapshot {
            next_offset: inner.local_latest_offset.load(Ordering::Relaxed),
            entries,
        })
    }
}

/// Region meta data.
#[derive(Default)]
struct RegionMetaInner {
    table_metas: HashMap<TableId, TableMeta>,
    /// It will fall behind the high watermark for ensuring successive
    /// increment.
    local_latest_offset: AtomicI64,
}

/// Wrapper for the [TableMetaInner].
// TODO: will be made use of later.
#[allow(unused)]
#[derive(Debug)]
struct TableMeta {
    table_id: TableId,
    /// The race condition may occur between writer thread 
    /// and background flush tread.
    inner: Mutex<TableMetaInner>,
}

// TODO: will be made use of later.
#[allow(unused)]
impl TableMeta {
    fn new(table_id: TableId) -> Self {
        Self {
            table_id,
            inner: Mutex::new(TableMetaInner::default()),
        }
    }

    async fn prepare_for_write(&self) -> SequenceNumber {
        let inner = self.inner.lock().await;
        inner.next_sequence_num
    }

    async fn update_after_write(&self, next_sequence_num: SequenceNumber, offset: Offset) {
        let mut inner = self.inner.lock().await;
        inner.next_sequence_num = next_sequence_num;

        // Update the mapping.
        let _ = inner.seq_hw_mapping.insert(next_sequence_num, offset);
    }

    async fn mark_deleted(&self, latest_marked_deleted: SequenceNumber) {
        let mut inner = self.inner.lock().await;
        inner.latest_marked_deleted = latest_marked_deleted;

        // Update the mapping, keep the range in description.
        inner
            .seq_hw_mapping
            .retain(|k, _| k < &latest_marked_deleted);
    }

    async fn get_meta_data(&self) -> Result<TableMetaData> {
        let inner = self.inner.lock().await;

        // TODO: make a state to represent it?
        if inner.seq_hw_mapping.is_empty() {
            ensure!(inner.next_sequence_num == inner.latest_marked_deleted,
                GetMetaData {
                    table_id: self.table_id,
                    msg: format!("unexpected state, next sequence should equal to latest marked deleted
                        while has flushed/init but not written, but now are {} and {}", inner.next_sequence_num,
                        inner.latest_marked_deleted),
                });

            Ok(TableMetaData {
                next_sequence_num: inner.next_sequence_num,
                latest_marked_deleted: inner.latest_marked_deleted,
                safe_deleted_offset: None,
            })
        } else {
            let offset = inner.seq_hw_mapping.get(&inner.latest_marked_deleted);
            ensure!(inner.next_sequence_num > inner.latest_marked_deleted && offset.is_some(),
                GetMetaData {
                    table_id: self.table_id,
                    msg: format!("unexpected state, offset should be found, next sequence should be greater to latest marked deleted
                        while has new writing after having flushed/init, but now are {:?}, {} and {}", offset, inner.next_sequence_num,
                        inner.latest_marked_deleted),
                });

            Ok(TableMetaData {
                next_sequence_num: inner.next_sequence_num,
                latest_marked_deleted: inner.latest_marked_deleted,
                safe_deleted_offset: None,
            })
        }
    }
}

// TODO: will be made use of later.
#[allow(unused)]
/// Table meta data, will be updated atomically by mutex.
#[derive(Debug, Default)]
struct TableMetaInner {
    /// Next sequence number for the new log.
    ///
    /// It will be updated while having pushed logs successfully.
    next_sequence_num: SequenceNumber,

    /// The lasted marked deleted sequence number. The log with
    /// a sequence number smaller than it can be deleted safely.
    ///
    /// It will be updated while having flushed successfully.
    latest_marked_deleted: SequenceNumber,

    /// Map next sequence number to message queue high watermark.
    ///
    /// It will keep the mapping information among following range:
    /// (next sequence number while last marked deleted,  
    /// next sequence number while latest marked deleted].
    seq_hw_mapping: BTreeMap<SequenceNumber, Offset>,
}

// TODO: will be made use of later.
#[allow(unused)]
#[derive(Debug, Default)]
pub struct TableMetaData {
    pub next_sequence_num: SequenceNumber,
    pub latest_marked_deleted: SequenceNumber,
    pub safe_deleted_offset: Option<Offset>,
}

/// Message queue implementation's meta value.
///
/// Include all tables(of current shard) and their next sequence number.
pub struct RegionMetaSnapshot {
    pub next_offset: Offset,
    pub entries: Vec<TableMetaData>,
}
