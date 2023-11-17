// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! WalManager implementation based on RocksDB

use std::{
    collections::HashMap,
    fmt,
    fmt::Formatter,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
};

use async_trait::async_trait;
use bytes_ext::BytesMut;
use common_types::{table::TableId, SequenceNumber, MAX_SEQUENCE_NUMBER, MIN_SEQUENCE_NUMBER};
use generic_error::BoxError;
use logger::{debug, info, warn};
use rocksdb::{
    rocksdb_options::ColumnFamilyDescriptor, ColumnFamilyOptions, DBCompactionStyle, DBIterator,
    DBOptions, FifoCompactionOptions, ReadOptions, SeekKey, Statistics, Writable, WriteBatch, DB,
};
use runtime::Runtime;
use snafu::ResultExt;
use tokio::sync::Mutex;

use super::config::RocksDBConfig;
use crate::{
    config::StorageConfig,
    kv_encoder::{CommonLogEncoding, CommonLogKey, MaxSeqMetaEncoding, MaxSeqMetaValue, MetaKey},
    log_batch::{LogEntry, LogWriteBatch},
    manager::{
        self, error::*, BatchLogIteratorAdapter, OpenedWals, ReadContext, ReadRequest, RegionId,
        ScanContext, ScanRequest, SyncLogIterator, WalLocation, WalManager, WalManagerRef,
        WalRuntimes, WalsOpener, WriteContext, MANIFEST_DIR_NAME, WAL_DIR_NAME,
    },
};

/// Table unit in the Wal.
struct TableUnit {
    /// id of the Region
    id: TableId,
    /// RocksDB instance
    db: Arc<DB>,
    /// `next_sequence_num` is ensured to be positive
    next_sequence_num: AtomicU64,
    /// Encoding for log entries
    log_encoding: CommonLogEncoding,
    /// Encoding for meta data of max sequence
    max_seq_meta_encoding: MaxSeqMetaEncoding,
    /// Runtime for write requests
    runtime: Arc<Runtime>,
    /// Ensure the delete procedure to be sequential
    delete_lock: Mutex<()>,
}

impl std::fmt::Debug for TableUnit {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableUnit")
            .field("id", &self.id)
            .field("next_sequence_num", &self.next_sequence_num)
            .finish()
    }
}

impl TableUnit {
    /// Allocate a continuous range of [SequenceNumber] and returns
    /// the start [SequenceNumber] of the range [start, start+`number`).
    #[inline]
    fn alloc_sequence_num(&self, number: u64) -> SequenceNumber {
        self.next_sequence_num.fetch_add(number, Ordering::Relaxed)
    }

    #[inline]
    /// Generate [LogKey] from `region_id`, `table_id` and `sequence_num`.
    fn log_key(&self, region_id: u64, sequence_num: SequenceNumber) -> CommonLogKey {
        CommonLogKey::new(region_id, self.id, sequence_num)
    }

    /// Returns the current sequence number which must be positive.
    fn sequence_num(&self) -> Result<u64> {
        let next_seq_num = self.next_sequence_num.load(Ordering::Relaxed);
        debug_assert!(next_seq_num > 0);

        Ok(next_seq_num - 1)
    }

    /// Delete entries in the range `[0, sequence_num]`.
    ///
    /// The delete procedure is ensured to be sequential.
    async fn delete_entries_up_to(
        &self,
        region_id: u64,
        mut sequence_num: SequenceNumber,
    ) -> Result<()> {
        debug!(
            "Wal table unit delete entries begin deleting, sequence_num:{:?}",
            sequence_num
        );

        let _delete_guard = self.delete_lock.lock().await;
        let max_seq = self.sequence_num()?;
        if sequence_num > max_seq {
            warn!(
                "Try to delete entries up to sequence number({}) greater than current max sequence \
                number({})",
                sequence_num,
                max_seq
            );
            sequence_num = max_seq;
        }

        let wb = {
            let wb = WriteBatch::default();

            // Delete the range [0, sequence_num]
            let start_log_key = CommonLogKey::new(region_id, self.id, 0);
            let end_log_key = if sequence_num < MAX_SEQUENCE_NUMBER {
                CommonLogKey::new(region_id, self.id, sequence_num + 1)
            } else {
                // Region id is unlikely to overflow.
                CommonLogKey::new(region_id, self.id + 1, 0)
            };
            let (mut start_key_buf, mut end_key_buf) = (BytesMut::new(), BytesMut::new());
            self.log_encoding
                .encode_key(&mut start_key_buf, &start_log_key)
                .box_err()
                .context(Encoding)?;
            self.log_encoding
                .encode_key(&mut end_key_buf, &end_log_key)
                .box_err()
                .context(Encoding)?;
            wb.delete_range(&start_key_buf, &end_key_buf)
                .map_err(|e| e.into())
                .context(Delete)?;

            // Update the max sequence number.
            let meta_key = MetaKey { table_id: self.id };
            let meta_value = MaxSeqMetaValue { max_seq };
            let (mut meta_key_buf, mut meta_value_buf) = (BytesMut::new(), BytesMut::new());
            self.max_seq_meta_encoding
                .encode_key(&mut meta_key_buf, &meta_key)?;
            self.max_seq_meta_encoding
                .encode_value(&mut meta_value_buf, &meta_value)?;
            wb.put(&meta_key_buf, &meta_value_buf)
                .map_err(|e| e.into())
                .context(Delete)?;

            wb
        };

        let db = self.db.clone();
        self.runtime
            .spawn_blocking(move || db.write(&wb).map_err(|e| e.into()).context(Delete))
            .await
            .box_err()
            .context(Delete)?
    }

    fn read(&self, ctx: &ReadContext, req: &ReadRequest) -> Result<RocksLogIterator> {
        debug!("Wal table unit begin reading, ctx:{:?}, req:{:?}", ctx, req);

        let read_opts = ReadOptions::default();
        let iter = DBIterator::new(self.db.clone(), read_opts);

        let start_sequence = if let Some(n) = req.start.as_start_sequence_number() {
            n
        } else {
            return Ok(RocksLogIterator::new_empty(self.log_encoding.clone(), iter));
        };

        let end_sequence = if let Some(n) = req.end.as_end_sequence_number() {
            n
        } else {
            return Ok(RocksLogIterator::new_empty(self.log_encoding.clone(), iter));
        };

        let region_id = req.location.region_id;
        let (min_log_key, max_log_key) = (
            self.log_key(region_id, start_sequence),
            self.log_key(region_id, end_sequence),
        );

        let log_iter =
            RocksLogIterator::with_data(self.log_encoding.clone(), iter, min_log_key, max_log_key);
        Ok(log_iter)
    }

    async fn write(&self, ctx: &WriteContext, batch: &LogWriteBatch) -> Result<u64> {
        debug!(
            "Wal table unit begin writing, ctx:{:?}, log_entries_num:{}",
            ctx,
            batch.entries.len()
        );

        manager::collect_write_log_metrics(batch);

        let entries_num = batch.len() as u64;
        let (wb, max_sequence_num) = {
            let wb = WriteBatch::default();
            let mut next_sequence_num = self.alloc_sequence_num(entries_num);
            let mut key_buf = BytesMut::new();

            for entry in &batch.entries {
                let region_id = batch.location.region_id;
                self.log_encoding
                    .encode_key(
                        &mut key_buf,
                        &CommonLogKey::new(region_id, batch.location.table_id, next_sequence_num),
                    )
                    .box_err()
                    .context(Encoding)?;
                wb.put(&key_buf, &entry.payload)
                    .map_err(|e| e.into())
                    .context(Write)?;

                next_sequence_num += 1;
            }

            (wb, next_sequence_num - 1)
        };

        let db = self.db.clone();
        self.runtime
            .spawn_blocking(move || {
                db.write(&wb)
                    .map(|_| max_sequence_num)
                    .map_err(|e| e.into())
                    .context(Write)
            })
            .await
            .box_err()
            .context(Write)?
    }
}

/// [WalManager] implementation based on RocksDB.
/// A [RocksImpl] consists of multiple [TableUnit]s and any read/write/delete
/// request is delegated to specific [TableUnit].
pub struct RocksImpl {
    /// Wal data path
    wal_path: String,
    /// RocksDB instance
    db: Arc<DB>,
    /// Runtime for read/write log entries
    runtime: Arc<Runtime>,
    /// Encoding for log entry
    log_encoding: CommonLogEncoding,
    /// Encoding for meta data of max sequence
    max_seq_meta_encoding: MaxSeqMetaEncoding,
    /// Table units
    table_units: RwLock<HashMap<TableId, Arc<TableUnit>>>,
    /// Stats of underlying rocksdb
    stats: Option<Statistics>,
}

impl Drop for RocksImpl {
    fn drop(&mut self) {
        // Clear all table_units.
        {
            let mut table_units = self.table_units.write().unwrap();
            table_units.clear();
        }

        info!("RocksImpl dropped, wal_path:{}", self.wal_path);
    }
}

impl RocksImpl {
    fn build_table_units(&self) -> Result<()> {
        let table_seqs = self.find_table_seqs_from_db()?;

        info!(
            "RocksImpl build table units, wal_path:{}, table_seqs:{:?}",
            self.wal_path, table_seqs
        );

        let mut table_units = self.table_units.write().unwrap();
        for (table_id, sequence_number) in table_seqs {
            let table_unit = TableUnit {
                id: table_id,
                db: self.db.clone(),
                next_sequence_num: AtomicU64::new(sequence_number + 1),
                log_encoding: self.log_encoding.clone(),
                max_seq_meta_encoding: self.max_seq_meta_encoding.clone(),
                runtime: self.runtime.clone(),
                delete_lock: Mutex::new(()),
            };

            table_units.insert(table_id, Arc::new(table_unit));
        }

        Ok(())
    }

    fn find_table_seqs_from_table_log(
        &self,
        table_max_seqs: &mut HashMap<TableId, SequenceNumber>,
    ) -> Result<()> {
        let mut current_region_id = u64::MAX;
        let mut end_boundary_key_buf = BytesMut::new();
        loop {
            debug!(
                "RocksImpl searches table logs for sequences by region id, region id:{}",
                current_region_id
            );

            let log_key = CommonLogKey::new(current_region_id, TableId::MAX, MAX_SEQUENCE_NUMBER);
            self.log_encoding
                .encode_key(&mut end_boundary_key_buf, &log_key)
                .box_err()
                .context(Encoding)?;
            let mut iter = self.db.iter();
            let seek_key = SeekKey::Key(&end_boundary_key_buf);

            let found = iter
                .seek_for_prev(seek_key)
                .map_err(|e| e.into())
                .context(Initialization)?;

            if !found {
                debug!(
                    "RocksImpl find table unit pairs stop scanning by region id, because of no entries to scan."
                );
                break;
            }

            if !self
                .log_encoding
                .is_log_key(iter.key())
                .box_err()
                .context(Decoding)?
            {
                debug!(
                    "RocksImpl find table unit pairs stop scanning by region id, because log keys are exhausted."
                );
                break;
            }

            let log_key = self
                .log_encoding
                .decode_key(iter.key())
                .box_err()
                .context(Decoding)?;

            // The max valid log key in a region is found, search table sequences by table
            // id.
            debug!(
                "RocksImpl has found log key by region id, log key:{:?}",
                log_key
            );
            self.find_table_seqs_from_table_log_by_table_id(log_key.region_id, table_max_seqs)?;

            if log_key.region_id == 0 {
                debug!("RocksImpl find table unit pairs stop scanning by region id, because region id 0 is reached.");
                break;
            }

            current_region_id = log_key.region_id - 1;
        }

        Ok(())
    }

    fn find_table_seqs_from_table_log_by_table_id(
        &self,
        region_id: u64,
        table_max_seqs: &mut HashMap<TableId, SequenceNumber>,
    ) -> Result<()> {
        let mut current_table_id = TableId::MAX;
        let mut end_boundary_key_buf = BytesMut::new();
        loop {
            debug!("RocksImpl searches table logs for sequences by table id, table id:{}, region id:{}", current_table_id, region_id);

            let log_key = CommonLogKey::new(region_id, current_table_id, MAX_SEQUENCE_NUMBER);
            self.log_encoding
                .encode_key(&mut end_boundary_key_buf, &log_key)
                .box_err()
                .context(Encoding)?;

            let mut iter = self.db.iter();
            let seek_key = SeekKey::Key(&end_boundary_key_buf);
            let found = iter
                .seek_for_prev(seek_key)
                .map_err(|e| e.into())
                .context(Initialization)?;

            if !found {
                debug!(
                    "RocksImpl find table unit pairs stop scanning by table id, because of no entries to scan."
                );
                break;
            }

            if !self
                .log_encoding
                .is_log_key(iter.key())
                .box_err()
                .context(Decoding)?
            {
                debug!(
                    "RocksImpl find table unit pairs stop scanning by table id, because log keys are exhausted."
                );
                break;
            }

            // Found a valid log key by `region_id` and `current_table_id`, check and maybe
            // insert the related information into `table_max_seqs`.
            let log_key = self
                .log_encoding
                .decode_key(iter.key())
                .box_err()
                .context(Decoding)?;
            debug!(
                "RocksImpl has found log key by table id, log key:{:?}",
                log_key
            );

            if log_key.region_id != region_id {
                debug!(
                    "RocksImpl find table unit pairs stop scanning by table id,
                    because has encountered next region id,
                    searching table id:{}, current region id:{}, next region id:{}",
                    current_table_id, region_id, log_key.region_id
                );
                break;
            }

            table_max_seqs.insert(log_key.table_id, log_key.sequence_num);

            if log_key.table_id == 0 {
                debug!("RocksImpl find table unit pairs stop scanning by table id, because table id 0 is reached");
                break;
            }

            current_table_id = log_key.table_id - 1;
        }

        Ok(())
    }

    fn find_table_seqs_from_table_meta(
        &self,
        table_max_seqs: &mut HashMap<TableId, SequenceNumber>,
    ) -> Result<()> {
        let meta_key = MetaKey { table_id: 0 };
        let mut start_boundary_key_buf = BytesMut::new();
        self.max_seq_meta_encoding
            .encode_key(&mut start_boundary_key_buf, &meta_key)?;
        let mut iter = self.db.iter();
        let seek_key = SeekKey::Key(&start_boundary_key_buf);
        iter.seek(seek_key)
            .map_err(|e| e.into())
            .context(Initialization)?;

        loop {
            if !iter.valid().map_err(|e| e.into()).context(Initialization)? {
                debug!("RocksImpl exhausts the iterator for meta information");
                break;
            }
            if !self.max_seq_meta_encoding.is_max_seq_meta_key(iter.key())? {
                debug!("RocksImpl exhausts max sequence meta key");
                break;
            }

            let meta_key = self.max_seq_meta_encoding.decode_key(iter.key())?;
            let meta_value = self.max_seq_meta_encoding.decode_value(iter.value())?;
            #[rustfmt::skip]
            // FIXME: In some cases, the `flushed sequence`
            // may be greater than the `actual last sequence of written logs`.
            //
            // Such as following case:
            //  + Write wal logs failed(last sequence stored in memory will increase when write failed).
            //  + Get last sequence from memory(greater then actual last sequence now).
            //  + Mark the got last sequence as flushed sequence.
            table_max_seqs
                .entry(meta_key.table_id)
                .and_modify(|v| {
                    if meta_value.max_seq > *v {
                        warn!(
                            "RocksDB WAL found flushed_seq greater than actual_last_sequence,
                        flushed_sequence:{}, actual_last_sequence:{}, table_id:{}",
                            meta_value.max_seq, *v, meta_key.table_id
                        );
                    }
                    *v = meta_value.max_seq.max(*v);
                })
                .or_insert(meta_value.max_seq);

            iter.next().map_err(|e| e.into()).context(Initialization)?;
        }

        Ok(())
    }

    /// Collect all the table units with its max sequence number from the db.
    ///
    /// Returns the mapping: table_id -> max_sequence_number
    fn find_table_seqs_from_db(&self) -> Result<HashMap<TableId, SequenceNumber>> {
        // build the mapping: table_id -> max_sequence_number
        let mut table_max_seqs = HashMap::new();

        // scan the table unit information from the data part.
        self.find_table_seqs_from_table_log(&mut table_max_seqs)?;

        // scan the table unit information from the meta part.
        self.find_table_seqs_from_table_meta(&mut table_max_seqs)?;

        Ok(table_max_seqs)
    }

    /// Get the table unit and create it if not found.
    fn get_or_create_table_unit(&self, location: WalLocation) -> Arc<TableUnit> {
        {
            let table_units = self.table_units.read().unwrap();
            if let Some(table_unit) = table_units.get(&location.table_id) {
                return table_unit.clone();
            }
        }

        let mut table_units = self.table_units.write().unwrap();
        if let Some(table_unit) = table_units.get(&location.table_id) {
            return table_unit.clone();
        }

        info!(
            "RocksImpl create new table unit, wal_path:{}, wal_location:{:?}",
            self.wal_path, location
        );

        // Create a new region.
        let table_unit = Arc::new(TableUnit {
            id: location.table_id,
            db: self.db.clone(),
            // ensure `next_sequence_number` to start from 1 (larger than MIN_SEQUENCE_NUMBER)
            next_sequence_num: AtomicU64::new(MIN_SEQUENCE_NUMBER + 1),
            log_encoding: self.log_encoding.clone(),
            max_seq_meta_encoding: self.max_seq_meta_encoding.clone(),
            runtime: self.runtime.clone(),
            delete_lock: Mutex::new(()),
        });

        table_units.insert(location.table_id, table_unit.clone());
        table_unit
    }

    /// Get the table unit.
    fn table_unit(&self, location: &WalLocation) -> Option<Arc<TableUnit>> {
        let table_units = self.table_units.read().unwrap();
        table_units.get(&location.table_id).cloned()
    }
}

/// Builder for `RocksImpl`.
pub struct Builder {
    wal_path: String,
    runtime: Arc<Runtime>,
    max_subcompactions: Option<u32>,
    max_background_jobs: Option<i32>,
    enable_statistics: Option<bool>,
    write_buffer_size: Option<u64>,
    max_write_buffer_number: Option<i32>,
    level_zero_file_num_compaction_trigger: Option<i32>,
    level_zero_slowdown_writes_trigger: Option<i32>,
    level_zero_stop_writes_trigger: Option<i32>,
    fifo_compaction_max_table_files_size: Option<u64>,
}

impl Builder {
    pub fn new(wal_path: impl Into<PathBuf>, runtime: Arc<Runtime>) -> Self {
        let wal_path: PathBuf = wal_path.into();
        Self {
            wal_path: wal_path.to_str().unwrap().to_owned(),
            runtime,
            max_subcompactions: None,
            max_background_jobs: None,
            enable_statistics: None,
            write_buffer_size: None,
            max_write_buffer_number: None,
            level_zero_file_num_compaction_trigger: None,
            level_zero_slowdown_writes_trigger: None,
            level_zero_stop_writes_trigger: None,
            fifo_compaction_max_table_files_size: None,
        }
    }

    pub fn max_subcompactions(mut self, v: u32) -> Self {
        self.max_subcompactions = Some(v);
        self
    }

    pub fn max_background_jobs(mut self, v: i32) -> Self {
        self.max_background_jobs = Some(v);
        self
    }

    pub fn enable_statistics(mut self, v: bool) -> Self {
        self.enable_statistics = Some(v);
        self
    }

    pub fn write_buffer_size(mut self, v: u64) -> Self {
        self.write_buffer_size = Some(v);
        self
    }

    pub fn max_write_buffer_number(mut self, v: i32) -> Self {
        self.max_write_buffer_number = Some(v);
        self
    }

    pub fn level_zero_file_num_compaction_trigger(mut self, v: i32) -> Self {
        self.level_zero_file_num_compaction_trigger = Some(v);
        self
    }

    pub fn level_zero_slowdown_writes_trigger(mut self, v: i32) -> Self {
        self.level_zero_slowdown_writes_trigger = Some(v);
        self
    }

    pub fn level_zero_stop_writes_trigger(mut self, v: i32) -> Self {
        self.level_zero_stop_writes_trigger = Some(v);
        self
    }

    pub fn fifo_compaction_max_table_files_size(mut self, v: u64) -> Self {
        self.fifo_compaction_max_table_files_size = Some(v);
        self
    }

    pub fn build(self) -> Result<RocksImpl> {
        let mut rocksdb_config = DBOptions::default();
        rocksdb_config.create_if_missing(true);

        if let Some(v) = self.max_subcompactions {
            rocksdb_config.set_max_subcompactions(v);
        }
        if let Some(v) = self.max_background_jobs {
            rocksdb_config.set_max_background_jobs(v);
        }

        let stats = if self.enable_statistics.unwrap_or_default() {
            let stats = Statistics::new();
            rocksdb_config.set_statistics(&stats);
            Some(stats)
        } else {
            None
        };

        let mut cf_opts = ColumnFamilyOptions::new();
        if let Some(v) = self.write_buffer_size {
            cf_opts.set_write_buffer_size(v);
        }
        if let Some(v) = self.max_write_buffer_number {
            cf_opts.set_max_write_buffer_number(v);
        }
        if let Some(v) = self.level_zero_file_num_compaction_trigger {
            cf_opts.set_level_zero_file_num_compaction_trigger(v);
        }
        if let Some(v) = self.level_zero_slowdown_writes_trigger {
            cf_opts.set_level_zero_slowdown_writes_trigger(v);
        }
        if let Some(v) = self.level_zero_stop_writes_trigger {
            cf_opts.set_level_zero_stop_writes_trigger(v);
        }

        // FIFO compaction strategy let rocksdb looks like a message queue.
        if let Some(v) = self.fifo_compaction_max_table_files_size {
            if v > 0 {
                let mut fifo_opts = FifoCompactionOptions::new();
                fifo_opts.set_max_table_files_size(v);
                cf_opts.set_fifo_compaction_options(fifo_opts);
                cf_opts.set_compaction_style(DBCompactionStyle::Fifo);
            }
        }

        let default_cfd = ColumnFamilyDescriptor {
            options: cf_opts,
            ..ColumnFamilyDescriptor::default()
        };
        let db = DB::open_cf(rocksdb_config, &self.wal_path, vec![default_cfd])
            .map_err(|e| e.into())
            .context(Open {
                wal_path: self.wal_path.clone(),
            })?;
        let rocks_impl = RocksImpl {
            wal_path: self.wal_path,
            db: Arc::new(db),
            runtime: self.runtime,
            log_encoding: CommonLogEncoding::newest(),
            max_seq_meta_encoding: MaxSeqMetaEncoding::newest(),
            table_units: RwLock::new(HashMap::new()),
            stats,
        };
        rocks_impl.build_table_units()?;

        Ok(rocks_impl)
    }
}

/// Iterator over log entries based on RocksDB iterator.
pub struct RocksLogIterator {
    log_encoding: CommonLogEncoding,
    /// denotes no more data to iterate and it is set to true when:
    ///  - initialized as no data iterator, or
    ///  - iterate to the end.
    no_more_data: bool,
    min_log_key: CommonLogKey,
    max_log_key: CommonLogKey,
    /// denote whether `iter` is seeked
    seeked: bool,
    /// RocksDB iterator
    iter: DBIterator<Arc<DB>>,
}

impl fmt::Debug for RocksLogIterator {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RocksLogIterator")
            .field("log_encoding", &self.log_encoding)
            .field("no_more_data", &self.no_more_data)
            .field("min_log_key", &self.min_log_key)
            .field("max_log_key", &self.max_log_key)
            .field("seeked", &self.seeked)
            .finish()
    }
}

impl RocksLogIterator {
    /// Create iterator maybe containing data.
    fn with_data(
        log_encoding: CommonLogEncoding,
        iter: DBIterator<Arc<DB>>,
        min_log_key: CommonLogKey,
        max_log_key: CommonLogKey,
    ) -> Self {
        Self {
            log_encoding,
            no_more_data: false,
            min_log_key,
            max_log_key,
            seeked: false,
            iter,
        }
    }

    /// Create empty iterator.
    fn new_empty(log_encoding: CommonLogEncoding, iter: DBIterator<Arc<DB>>) -> Self {
        Self {
            log_encoding,
            no_more_data: true,
            min_log_key: CommonLogKey::new(0, 0, 0),
            max_log_key: CommonLogKey::new(0, 0, 0),
            seeked: false,
            iter,
        }
    }

    /// it's a valid log key if it is in the range `[self.min_log_key,
    /// self.max_log_key]`.
    fn is_valid_log_key(&self, curr_log_key: &CommonLogKey) -> bool {
        curr_log_key <= &self.max_log_key && curr_log_key >= &self.min_log_key
    }

    /// End is reached iteration if `curr_log_key` is greater than
    /// `max_log_key`.
    fn is_end_reached(&self, curr_log_key: &CommonLogKey) -> bool {
        curr_log_key >= &self.max_log_key
    }

    /// let `iter` seek to `min_log_key`
    /// no guarantee on that `self.iter` is valid
    fn seek(&mut self) -> Result<()> {
        self.seeked = true;

        let mut seek_key_buf = BytesMut::new();
        self.log_encoding
            .encode_key(&mut seek_key_buf, &self.min_log_key)
            .box_err()
            .context(Encoding)?;
        let seek_key = SeekKey::Key(&seek_key_buf);
        self.iter
            .seek(seek_key)
            .map_err(|e| e.into())
            .context(Read)?;

        Ok(())
    }
}

impl SyncLogIterator for RocksLogIterator {
    fn next_log_entry(&mut self) -> Result<Option<LogEntry<&'_ [u8]>>> {
        if self.no_more_data {
            return Ok(None);
        }

        if !self.seeked {
            self.seek()?;

            let valid = self.iter.valid().map_err(|e| e.into()).context(Read)?;
            if !valid {
                self.no_more_data = true;
                return Ok(None);
            }
        } else {
            let found = self.iter.next().map_err(|e| e.into()).context(Read)?;
            if !found {
                self.no_more_data = true;
                return Ok(None);
            }
        }

        let curr_log_key = self
            .log_encoding
            .decode_key(self.iter.key())
            .box_err()
            .context(Decoding)?;
        self.no_more_data = self.is_end_reached(&curr_log_key);

        if self.is_valid_log_key(&curr_log_key) {
            let payload = self
                .log_encoding
                .decode_value(self.iter.value())
                .box_err()
                .context(Decoding)?;
            let log_entry = LogEntry {
                table_id: curr_log_key.table_id,
                sequence: curr_log_key.sequence_num,
                payload,
            };
            Ok(Some(log_entry))
        } else {
            Ok(None)
        }
    }
}

#[async_trait]
impl WalManager for RocksImpl {
    async fn sequence_num(&self, location: WalLocation) -> Result<u64> {
        if let Some(table_unit) = self.table_unit(&location) {
            return table_unit.sequence_num();
        }

        Ok(MIN_SEQUENCE_NUMBER)
    }

    async fn mark_delete_entries_up_to(
        &self,
        location: WalLocation,
        sequence_num: SequenceNumber,
    ) -> Result<()> {
        if let Some(table_unit) = self.table_unit(&location) {
            let region_id = location.region_id;
            return table_unit
                .delete_entries_up_to(region_id, sequence_num)
                .await;
        }

        Ok(())
    }

    async fn close_region(&self, region_id: RegionId) -> Result<()> {
        debug!(
            "Close region for RocksDB based WAL is noop operation, region_id:{}",
            region_id
        );

        Ok(())
    }

    async fn close_gracefully(&self) -> Result<()> {
        info!("Close rocksdb wal gracefully");

        Ok(())
    }

    async fn read_batch(
        &self,
        ctx: &ReadContext,
        req: &ReadRequest,
    ) -> Result<BatchLogIteratorAdapter> {
        let sync_iter = if let Some(table_unit) = self.table_unit(&req.location) {
            table_unit.read(ctx, req)?
        } else {
            let iter = DBIterator::new(self.db.clone(), ReadOptions::default());
            RocksLogIterator::new_empty(self.log_encoding.clone(), iter)
        };
        let runtime = self.runtime.clone();

        Ok(BatchLogIteratorAdapter::new_with_sync(
            Box::new(sync_iter),
            runtime,
            ctx.batch_size,
        ))
    }

    async fn write(&self, ctx: &WriteContext, batch: &LogWriteBatch) -> Result<SequenceNumber> {
        let table_unit = self.get_or_create_table_unit(batch.location);
        table_unit.write(ctx, batch).await
    }

    async fn scan(&self, ctx: &ScanContext, req: &ScanRequest) -> Result<BatchLogIteratorAdapter> {
        debug!("Wal region begin scanning, ctx:{:?}, req:{:?}", ctx, req);

        let read_opts = ReadOptions::default();
        let iter = DBIterator::new(self.db.clone(), read_opts);

        let region_id = req.region_id;
        let (min_log_key, max_log_key) = (
            CommonLogKey::new(region_id, TableId::MIN, SequenceNumber::MIN),
            CommonLogKey::new(region_id, TableId::MAX, SequenceNumber::MAX),
        );

        let log_iter =
            RocksLogIterator::with_data(self.log_encoding.clone(), iter, min_log_key, max_log_key);

        Ok(BatchLogIteratorAdapter::new_with_sync(
            Box::new(log_iter),
            self.runtime.clone(),
            ctx.batch_size,
        ))
    }

    async fn get_statistics(&self) -> Option<String> {
        // RocksDB stats.
        let rocksdb_stats = if let Some(stats) = &self.stats {
            stats.to_string()
        } else {
            None
        };
        let rocksdb_stats = rocksdb_stats.unwrap_or_default();

        // Wal stats.
        let table_units = self.table_units.read().unwrap();
        let mut wal_stats = Vec::with_capacity(table_units.len());
        for table_unit in table_units.values() {
            wal_stats.push(format!("{:?}", table_unit.as_ref()));
        }
        let wal_stats = wal_stats.join("\n");

        let stats = format!("#RocksDB stats:\n{rocksdb_stats}\n#RocksDBWal stats:\n{wal_stats}\n");

        Some(stats)
    }
}

impl fmt::Debug for RocksImpl {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RocksImpl")
            .field("wal_path", &self.wal_path)
            .finish()
    }
}

#[derive(Default)]
pub struct RocksDBWalsOpener;

impl RocksDBWalsOpener {
    fn build_manager(
        wal_path: PathBuf,
        runtime: Arc<Runtime>,
        config: RocksDBConfig,
    ) -> Result<WalManagerRef> {
        let rocks = Builder::new(wal_path, runtime)
            .max_subcompactions(config.max_subcompactions)
            .max_background_jobs(config.max_background_jobs)
            .enable_statistics(config.enable_statistics)
            .write_buffer_size(config.write_buffer_size.0)
            .max_write_buffer_number(config.max_write_buffer_number)
            .level_zero_file_num_compaction_trigger(config.level_zero_file_num_compaction_trigger)
            .level_zero_slowdown_writes_trigger(config.level_zero_slowdown_writes_trigger)
            .level_zero_stop_writes_trigger(config.level_zero_stop_writes_trigger)
            .fifo_compaction_max_table_files_size(config.fifo_compaction_max_table_files_size.0)
            .build()?;

        Ok(Arc::new(rocks))
    }
}

#[async_trait]
impl WalsOpener for RocksDBWalsOpener {
    async fn open_wals(&self, config: &StorageConfig, runtimes: WalRuntimes) -> Result<OpenedWals> {
        let rocksdb_wal_config = match &config {
            StorageConfig::RocksDB(config) => config.clone(),
            _ => {
                return InvalidWalConfig {
                    msg: format!(
                        "invalid wal storage config while opening rocksDB wal, config:{config:?}"
                    ),
                }
                .fail();
            }
        };

        let write_runtime = runtimes.write_runtime.clone();
        let data_path = Path::new(&rocksdb_wal_config.data_dir);

        // Build data wal
        let data_wal = if rocksdb_wal_config.disable_data {
            Arc::new(crate::dummy::DoNothing)
        } else {
            Self::build_manager(
                data_path.join(WAL_DIR_NAME),
                write_runtime.clone(),
                rocksdb_wal_config.data_namespace,
            )?
        };

        // Build manifest wal
        let manifest_wal = Self::build_manager(
            data_path.join(MANIFEST_DIR_NAME),
            write_runtime,
            rocksdb_wal_config.meta_namespace,
        )?;

        Ok(OpenedWals {
            data_wal,
            manifest_wal,
        })
    }
}
