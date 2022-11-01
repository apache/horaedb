// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Region meta data

use std::collections::{BTreeMap, HashMap};

use common_types::{table::TableId, SequenceNumber};
use common_util::define_result;
use message_queue::Offset;
use snafu::{ensure, Backtrace, OptionExt, Snafu};
use tokio::sync::{Mutex, RwLock};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to update meta data after write of table:{}, msg:{}\nBacktrace:\n{}",
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
        "Failed to mark deleted for table:{}, msg:{}\nBacktrace:\n{}",
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
#[derive(Default, Debug)]
pub struct RegionMeta {
    inner: RwLock<RegionMetaInner>,
}

// TODO: will be made use of later.
#[allow(unused)]
impl RegionMeta {
    // TODO: Need to implement the init method using the [RegionMetaSnapshot] which
    // will be persisted.

    pub async fn prepare_for_table_write(&self, table_id: TableId) -> SequenceNumber {
        {
            let inner = self.inner.read().await;
            if let Some(table_meta) = inner.table_metas.get(&table_id) {
                return table_meta.get_meta_data().await.next_sequence_num;
            }
        }

        // Double check is not needed, due to just one task will write the specific
        // table.
        let mut inner = self.inner.write().await;
        assert!(inner
            .table_metas
            .insert(table_id, TableMeta::new(table_id))
            .is_none(),
            "now just support the thread model: one writer to one table, make no sense to occur race here");
        // New table, so returned next sequence num is zero.
        SequenceNumber::MIN
    }

    /// Update following meta data of table after each writing:
    /// + mapping of start sequence number to start offset
    /// + high watermark
    /// + next sequence number
    pub async fn update_after_table_write(
        &self,
        table_id: TableId,
        write_offset_range: OffsetRange,
    ) -> Result<()> {
        ensure!(
            write_offset_range.start <= write_offset_range.end,
            UpdateAfterWrite {
                table_id, msg: format!("write offset range's start should not be larger than its end, offset range:{:?}", 
                write_offset_range)
            },
        );

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

        table_meta.update_after_write(write_offset_range).await;

        Ok(())
    }

    /// Mark the deleted sequence number to latest next sequence number.
    pub async fn mark_table_deleted(
        &self,
        table_id: TableId,
        sequence_num: SequenceNumber,
    ) -> Result<()> {
        let inner = self.inner.read().await;
        let table_meta = inner
            .table_metas
            .get(&table_id)
            .with_context(|| MarkDeleted {
                table_id,
                msg: format!("table:{}'s meta not found while mark its deleted", table_id),
            })?;
        table_meta.mark_deleted(sequence_num).await;

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
        let mut entries = Vec::with_capacity(inner.table_metas.len());
        for (table_id, table_meta) in &inner.table_metas {
            let meta_data = table_meta.get_meta_data().await;
            entries.push(meta_data);
        }

        RegionMetaSnapshot { entries }
    }
}

/// Region meta data.
#[derive(Default, Debug)]
struct RegionMetaInner {
    table_metas: HashMap<TableId, TableMeta>,
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

    async fn update_after_write(&self, write_offset_range: OffsetRange) {
        let updated_num = (write_offset_range.end - write_offset_range.start + 1) as u64;
        let mut inner = self.inner.lock().await;
        let old_next_sequence_num = inner.next_sequence_num;
        inner.next_sequence_num += updated_num;

        // Update the mapping and high water mark.
        let _ = inner
            .start_sequence_offset_mapping
            .insert(old_next_sequence_num, write_offset_range.start);
        inner.current_high_watermark = write_offset_range.end + 1;
    }

    async fn mark_deleted(&self, latest_marked_deleted: SequenceNumber) -> Result<()> {
        let mut inner = self.inner.lock().await;

        ensure!(
            latest_marked_deleted <= inner.next_sequence_num,
            MarkDeleted {
                table_id: self.table_id,
                msg: format!(
                    "latest marked deleted should be less than or 
                    equal to next sequence number, now are:{} and {}",
                    latest_marked_deleted, inner.next_sequence_num
                ),
            }
        );

        ensure!(
            latest_marked_deleted >= inner.latest_marked_deleted,
            MarkDeleted {
                table_id: self.table_id,
                msg: format!("latest marked deleted should be greater than or equal to origin one now are:{} and {}",
                latest_marked_deleted,
                inner.latest_marked_deleted),
            }
        );

        inner.latest_marked_deleted = latest_marked_deleted;

        // Update the mapping, keep the range in description.
        inner
            .start_sequence_offset_mapping
            .retain(|k, _| k >= &latest_marked_deleted);

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

    /// The high watermark after this table's latest writing.
    current_high_watermark: Offset,

    /// Map the start sequence number to start offset in every write.
    ///
    /// It will be removed to the mark deleted sequence number after flushing.
    start_sequence_offset_mapping: BTreeMap<SequenceNumber, Offset>,
}

// TODO: will be made use of later.
#[allow(unused)]
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

/// Message queue's offset range
///
/// The range should be [start, end], and it will never be empty.
#[derive(Debug)]
pub struct OffsetRange {
    pub start: Offset,
    pub end: Offset,
}

#[allow(unused)]
impl OffsetRange {
    pub fn new(start: Offset, end: Offset) -> Self {
        Self { start, end }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    };

    use common_types::{table::TableId, SequenceNumber};
    use message_queue::Offset;
    use tokio::time;

    use super::{OffsetRange, RegionMeta};

    #[tokio::test]
    async fn test_basic_work_flow() {
        let region_meta = RegionMeta::default();

        // New table meta.
        let init_seq = region_meta.prepare_for_table_write(0).await;
        let snapshot = region_meta.make_snapshot().await;
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].next_sequence_num, SequenceNumber::MIN);
        assert_eq!(
            snapshot.entries[0].latest_marked_deleted,
            snapshot.entries[0].next_sequence_num
        );
        assert_eq!(snapshot.entries[0].safe_delete_offset, None);

        // Update.
        region_meta
            .update_after_table_write(0, OffsetRange::new(20, 29))
            .await
            .unwrap();
        let snapshot = region_meta.make_snapshot().await;
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].next_sequence_num, init_seq + 10);
        assert_eq!(snapshot.entries[0].latest_marked_deleted, 0);
        assert_eq!(snapshot.entries[0].current_high_watermark, 30);
        assert_eq!(snapshot.entries[0].safe_delete_offset, Some(20));

        // Update again, and delete to a fall behind point.
        region_meta
            .update_after_table_write(0, OffsetRange::new(42, 51))
            .await
            .unwrap();
        let snapshot = region_meta.make_snapshot().await;
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].next_sequence_num, init_seq + 20);
        assert_eq!(snapshot.entries[0].latest_marked_deleted, 0);
        assert_eq!(snapshot.entries[0].current_high_watermark, 52);
        assert_eq!(snapshot.entries[0].safe_delete_offset, Some(20));

        region_meta
            .mark_table_deleted(0, init_seq + 10)
            .await
            .unwrap();
        let snapshot = region_meta.make_snapshot().await;
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].next_sequence_num, init_seq + 20);
        assert_eq!(snapshot.entries[0].latest_marked_deleted, init_seq + 10);
        assert_eq!(snapshot.entries[0].current_high_watermark, 52);
        assert_eq!(snapshot.entries[0].safe_delete_offset, Some(42));

        // delete to latest
        region_meta
            .mark_table_deleted(0, init_seq + 20)
            .await
            .unwrap();
        let snapshot = region_meta.make_snapshot().await;
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].next_sequence_num, init_seq + 20);
        assert_eq!(snapshot.entries[0].latest_marked_deleted, init_seq + 20);
        assert_eq!(snapshot.entries[0].current_high_watermark, 52);
        assert_eq!(snapshot.entries[0].safe_delete_offset, None);
    }

    #[tokio::test]
    async fn test_table_write_delete_race() {
        for _ in 0..50 {
            test_table_write_delete_race_once().await;
        }
    }

    async fn test_table_write_delete_race_once() {
        let region_meta = Arc::new(RegionMeta::default());
        let mut expected_offset_range = (42, 51);
        let mut expected_next_sequence_num = 0;

        // New table meta.
        create_and_check_table_meta(&region_meta, 0).await;

        // Spawn a task for deletion, and simultaneously update in current task.
        let can_delete = Arc::new(AtomicBool::new(false));

        let region_meta_clone = region_meta.clone();
        let can_delete_clone = can_delete.clone();
        let expected_next_sequence_num_copy = expected_next_sequence_num;
        let expected_offset_range_copy = expected_offset_range;
        let handle = tokio::spawn(async move {
            let region_meta = region_meta_clone;

            while !can_delete_clone.load(Ordering::SeqCst) {
                time::sleep(Duration::from_millis(1)).await;
            }

            region_meta
                .mark_table_deleted(0, expected_next_sequence_num_copy + 10)
                .await
                .unwrap();
            let snapshot = region_meta.make_snapshot().await;
            assert_eq!(snapshot.entries.len(), 1);
            assert_eq!(snapshot.entries[0].latest_marked_deleted, 10);
            assert_eq!(
                snapshot.entries[0].safe_delete_offset,
                Some(expected_offset_range_copy.0 + 10)
            );
        });

        // Update once and make deletion task able to continue.
        expected_next_sequence_num += 10;
        region_meta
            .update_after_table_write(
                0,
                OffsetRange::new(expected_offset_range.0, expected_offset_range.1),
            )
            .await
            .unwrap();
        let snapshot = region_meta.make_snapshot().await;
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(
            snapshot.entries[0].next_sequence_num,
            expected_next_sequence_num
        );
        assert_eq!(snapshot.entries[0].latest_marked_deleted, 0);
        assert_eq!(
            snapshot.entries[0].safe_delete_offset,
            Some(expected_offset_range.0)
        );
        assert_eq!(
            snapshot.entries[0].current_high_watermark,
            expected_offset_range.1 + 1
        );
        expected_offset_range.0 += 10;
        expected_offset_range.1 += 10;

        let rnd_ms = rand::random::<u64>() % 30;
        time::sleep(Duration::from_millis(rnd_ms)).await;

        can_delete.store(true, Ordering::SeqCst);

        // Continue to update.
        update_and_check_table_meta(
            &region_meta,
            0,
            expected_offset_range,
            expected_next_sequence_num,
            5,
        )
        .await;

        let snapshot = region_meta.make_snapshot().await;
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].next_sequence_num, 60);
        assert_eq!(snapshot.entries[0].latest_marked_deleted, 10);
        assert_eq!(snapshot.entries[0].current_high_watermark, 102);
        assert_eq!(snapshot.entries[0].safe_delete_offset, Some(52));

        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_region_write_create_race() {
        for _ in 0..50 {
            test_region_write_create_race_once().await;
        }
    }

    async fn test_region_write_create_race_once() {
        let region_meta = Arc::new(RegionMeta::default());
        let expected_offset_range = (42, 51);
        let expected_next_sequence_num = 0;

        // Spawn a task to create and update, and simultaneously update in current task.
        let region_meta_clone = region_meta.clone();
        let expected_next_sequence_num_copy = expected_next_sequence_num;
        let expected_offset_range_copy = expected_offset_range;
        let handle = tokio::spawn(async move {
            let region_meta = region_meta_clone;

            create_and_check_table_meta(&region_meta, 0).await;
            update_and_check_table_meta(
                &region_meta,
                0,
                expected_offset_range_copy,
                expected_next_sequence_num_copy,
                5,
            )
            .await;
        });

        create_and_check_table_meta(&region_meta, 1).await;
        update_and_check_table_meta(
            &region_meta,
            1,
            expected_offset_range,
            expected_next_sequence_num,
            5,
        )
        .await;

        handle.await.unwrap();

        // Check final result.
        let snapshot = region_meta.make_snapshot().await;
        assert_eq!(snapshot.entries.len(), 2);
        assert_eq!(snapshot.entries[0].next_sequence_num, 50);
        assert_eq!(snapshot.entries[0].latest_marked_deleted, 0);
        assert_eq!(snapshot.entries[0].current_high_watermark, 92);
        assert_eq!(snapshot.entries[0].safe_delete_offset, Some(42));
        assert_eq!(snapshot.entries[1].next_sequence_num, 50);
        assert_eq!(snapshot.entries[1].latest_marked_deleted, 0);
        assert_eq!(snapshot.entries[1].current_high_watermark, 92);
        assert_eq!(snapshot.entries[1].safe_delete_offset, Some(42));
    }

    async fn update_and_check_table_meta(
        region_meta: &RegionMeta,
        table_id: TableId,
        expected_offset_range: (Offset, Offset),
        expected_next_sequence_num: u64,
        cnt: u64,
    ) {
        let mut expected_offset_range = expected_offset_range;
        let mut expected_next_sequence_num = expected_next_sequence_num;
        for _ in 0..cnt {
            expected_next_sequence_num += 10;

            region_meta
                .update_after_table_write(
                    table_id,
                    OffsetRange::new(expected_offset_range.0, expected_offset_range.1),
                )
                .await
                .unwrap();
            let snapshot = region_meta.make_snapshot().await;
            for entry in snapshot.entries {
                if entry.table_id == table_id {
                    assert_eq!(entry.next_sequence_num, expected_next_sequence_num);
                    assert_eq!(entry.current_high_watermark, expected_offset_range.1 + 1);
                }
            }

            expected_offset_range.0 += 10;
            expected_offset_range.1 += 10;
            time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn create_and_check_table_meta(region_meta: &RegionMeta, table_id: TableId) {
        let init_seq = region_meta.prepare_for_table_write(table_id).await;
        assert_eq!(init_seq, 0);
        let snapshot = region_meta.make_snapshot().await;
        for entry in snapshot.entries {
            if entry.table_id == table_id {
                assert_eq!(entry.next_sequence_num, SequenceNumber::MIN);
                assert_eq!(entry.latest_marked_deleted, entry.next_sequence_num);
                assert_eq!(entry.current_high_watermark, 0);
                assert_eq!(entry.safe_delete_offset, None);
            }
        }
    }
}
