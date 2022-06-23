//! WalManager implementation based on RocksDB

use std::{
    collections::HashMap,
    fmt,
    fmt::Formatter,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
};

use async_trait::async_trait;
use common_types::{bytes::BytesMut, SequenceNumber, MAX_SEQUENCE_NUMBER, MIN_SEQUENCE_NUMBER};
use common_util::runtime::Runtime;
use log::{debug, info, warn};
use rocksdb::{DBIterator, DBOptions, ReadOptions, SeekKey, Writable, WriteBatch, DB};
use snafu::ResultExt;
use tokio::sync::Mutex;

use crate::{
    log_batch::{LogEntry, LogWriteBatch, Payload, PayloadDecoder},
    manager::{
        error::*, BatchLogIteratorAdapter, BlockingLogIterator, LogReader, LogWriter, ReadContext,
        ReadRequest, RegionId, WalManager, WriteContext, MAX_REGION_ID,
    },
    rocks_impl::encoding::{LogEncoding, LogKey, MaxSeqMetaEncoding, MaxSeqMetaValue, MetaKey},
};

/// Region in the Wal.
struct Region {
    /// id of the Region
    id: RegionId,
    /// RocksDB instance
    db: Arc<DB>,
    /// `next_sequence_num` is ensured to be positive
    next_sequence_num: AtomicU64,
    /// Encoding for log entries
    log_encoding: LogEncoding,
    /// Encoding for meta data of max sequence
    max_seq_meta_encoding: MaxSeqMetaEncoding,
    /// Runtime for write requests
    runtime: Arc<Runtime>,
    /// Ensure the delete procedure to be sequential
    delete_lock: Mutex<()>,
}

impl Region {
    /// Allocate a continuous range of [SequenceNumber] and returns
    /// the start [SequenceNumber] of the range [start, start+`number`).
    #[inline]
    fn alloc_sequence_num(&self, number: u64) -> SequenceNumber {
        self.next_sequence_num.fetch_add(number, Ordering::Relaxed)
    }

    #[inline]
    /// Generate [LogKey] from `region_id` and `sequence_num`
    fn log_key(&self, sequence_num: SequenceNumber) -> LogKey {
        (self.id, sequence_num)
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
    async fn delete_entries_up_to(&self, mut sequence_num: SequenceNumber) -> Result<()> {
        debug!(
            "Wal Region delete entries begin deleting, sequence_num:{:?}",
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
            let start_log_key = (self.id, 0);
            let end_log_key = if sequence_num < MAX_SEQUENCE_NUMBER {
                (self.id, sequence_num + 1)
            } else {
                // Region id is unlikely to overflow.
                (self.id + 1, 0)
            };
            let (mut start_key_buf, mut end_key_buf) = (BytesMut::new(), BytesMut::new());
            self.log_encoding
                .encode_key(&mut start_key_buf, &start_log_key)?;
            self.log_encoding
                .encode_key(&mut end_key_buf, &end_log_key)?;
            wb.delete_range(&start_key_buf, &end_key_buf)
                .map_err(|e| e.into())
                .context(Delete)?;

            // Update the max sequence number.
            let meta_key = MetaKey { region_id: self.id };
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
            .map_err(|e| Box::new(e) as _)
            .context(Delete)?
    }

    fn read(&self, ctx: &ReadContext, req: &ReadRequest) -> Result<RocksLogIterator> {
        debug!("Wal region begin reading, ctx:{:?}, req:{:?}", ctx, req);

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

        let (min_log_key, max_log_key) = (self.log_key(start_sequence), self.log_key(end_sequence));

        let log_iter =
            RocksLogIterator::with_data(self.log_encoding.clone(), iter, min_log_key, max_log_key);
        Ok(log_iter)
    }

    async fn write<P: Payload>(&self, ctx: &WriteContext, batch: &LogWriteBatch<P>) -> Result<u64> {
        debug!(
            "Wal region begin writing, ctx:{:?}, log_entries_num:{}",
            ctx,
            batch.entries.len()
        );

        let entries_num = batch.len() as u64;
        let (wb, max_sequence_num) = {
            let wb = WriteBatch::default();
            let mut next_sequence_num = self.alloc_sequence_num(entries_num);
            let mut key_buf = BytesMut::new();
            let mut value_buf = BytesMut::new();

            for entry in &batch.entries {
                self.log_encoding
                    .encode_key(&mut key_buf, &(batch.region_id, next_sequence_num))?;
                self.log_encoding
                    .encode_value(&mut value_buf, &entry.payload)?;
                wb.put(&key_buf, &value_buf)
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
            .map_err(|e| Box::new(e) as _)
            .context(Write)?
    }
}

/// [WalManager] implementation based on RocksDB.
/// A [RocksImpl] consists of multiple [Region]s and any read/write/delete
/// request is delegated to specific [Region].
pub struct RocksImpl {
    /// Wal data path
    wal_path: String,
    /// RocksDB instance
    db: Arc<DB>,
    /// Runtime for read/write log entries
    runtime: Arc<Runtime>,
    /// Encoding for log entry
    log_encoding: LogEncoding,
    /// Encoding for meta data of max sequence
    max_seq_meta_encoding: MaxSeqMetaEncoding,
    /// Regions
    regions: RwLock<HashMap<RegionId, Arc<Region>>>,
}

impl Drop for RocksImpl {
    fn drop(&mut self) {
        // Clear all regions.
        {
            let mut regions = self.regions.write().unwrap();
            regions.clear();
        }

        info!("RocksImpl dropped, wal_path:{}", self.wal_path);
    }
}

impl RocksImpl {
    fn build_regions(&self) -> Result<()> {
        let region_seqs = self.find_region_seqs_from_db()?;

        info!(
            "RocksImpl build regions, wal_path:{}, region_seqs:{:?}",
            self.wal_path, region_seqs
        );

        let mut regions = self.regions.write().unwrap();
        for (region_id, sequence_number) in region_seqs {
            let region = Region {
                id: region_id,
                db: self.db.clone(),
                next_sequence_num: AtomicU64::new(sequence_number + 1),
                log_encoding: self.log_encoding.clone(),
                max_seq_meta_encoding: self.max_seq_meta_encoding.clone(),
                runtime: self.runtime.clone(),
                delete_lock: Mutex::new(()),
            };

            regions.insert(region_id, Arc::new(region));
        }

        Ok(())
    }

    fn find_region_seqs_from_region_data(
        &self,
        region_max_seqs: &mut HashMap<RegionId, SequenceNumber>,
    ) -> Result<()> {
        let mut current_region_id = MAX_REGION_ID;
        let mut end_boundary_key_buf = BytesMut::new();
        loop {
            let log_key = (current_region_id, MAX_SEQUENCE_NUMBER);
            self.log_encoding
                .encode_key(&mut end_boundary_key_buf, &log_key)?;
            let mut iter = self.db.iter();
            let seek_key = SeekKey::Key(&end_boundary_key_buf);

            let found = iter
                .seek_for_prev(seek_key)
                .map_err(|e| e.into())
                .context(Initialization)?;

            if !found {
                debug!("RocksImpl find region pairs stop scanning, because of no entries to scan");
                break;
            }

            if !self.log_encoding.is_log_key(iter.key())? {
                debug!("RocksImpl find region pairs stop scanning, because log keys are exhausted");
                break;
            }

            let log_key = self.log_encoding.decode_key(iter.key())?;
            region_max_seqs.insert(log_key.0, log_key.1);

            if log_key.0 == 0 {
                debug!("RocksImpl find region pairs stop scanning, because region 0 is reached");
                break;
            }
            current_region_id = log_key.0 - 1;
        }

        Ok(())
    }

    fn find_region_seqs_from_region_meta(
        &self,
        region_max_seqs: &mut HashMap<RegionId, SequenceNumber>,
    ) -> Result<()> {
        let meta_key = MetaKey { region_id: 0 };
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
            region_max_seqs
                .entry(meta_key.region_id)
                .and_modify(|v| {
                    *v = meta_value.max_seq.max(*v);
                })
                .or_insert(meta_value.max_seq);

            iter.next().map_err(|e| e.into()).context(Initialization)?;
        }

        Ok(())
    }

    /// Collect all the regions with its max sequence number from the db.
    ///
    /// Returns the mapping: region_id -> max_sequence_number
    fn find_region_seqs_from_db(&self) -> Result<HashMap<RegionId, SequenceNumber>> {
        // build the mapping: region_id -> max_sequence_number
        let mut region_max_seqs = HashMap::new();

        // scan the region information from the data part.
        self.find_region_seqs_from_region_data(&mut region_max_seqs)?;

        // scan the region information from the meta part.
        self.find_region_seqs_from_region_meta(&mut region_max_seqs)?;

        Ok(region_max_seqs)
    }

    /// Get the region and create it if not found.
    fn get_or_create_region(&self, region_id: RegionId) -> Arc<Region> {
        {
            let regions = self.regions.read().unwrap();
            if let Some(region) = regions.get(&region_id) {
                return region.clone();
            }
        }

        let mut regions = self.regions.write().unwrap();
        if let Some(region) = regions.get(&region_id) {
            return region.clone();
        }

        info!(
            "RocksImpl create new region, wal_path:{}, region_id:{}",
            self.wal_path, region_id
        );

        // create a new region
        let region = Arc::new(Region {
            id: region_id,
            db: self.db.clone(),
            // ensure `next_sequence_number` to start from 1 (larger than MIN_SEQUENCE_NUMBER)
            next_sequence_num: AtomicU64::new(MIN_SEQUENCE_NUMBER + 1),
            log_encoding: self.log_encoding.clone(),
            max_seq_meta_encoding: self.max_seq_meta_encoding.clone(),
            runtime: self.runtime.clone(),
            delete_lock: Mutex::new(()),
        });

        regions.insert(region_id, region.clone());
        region
    }

    /// Get the region
    fn region(&self, region_id: RegionId) -> Option<Arc<Region>> {
        let regions = self.regions.read().unwrap();
        regions.get(&region_id).cloned()
    }
}

/// Builder for `RocksImpl`.
pub struct Builder {
    wal_path: String,
    rocksdb_config: DBOptions,
    runtime: Arc<Runtime>,
}

impl Builder {
    pub fn with_default_rocksdb_config(
        wal_path: impl Into<PathBuf>,
        runtime: Arc<Runtime>,
    ) -> Self {
        let mut rocksdb_config = DBOptions::default();
        // TODO(yingwen): Move to another function?
        rocksdb_config.create_if_missing(true);
        Self::new(wal_path, runtime, rocksdb_config)
    }

    pub fn new(
        wal_path: impl Into<PathBuf>,
        runtime: Arc<Runtime>,
        rocksdb_config: DBOptions,
    ) -> Self {
        let wal_path: PathBuf = wal_path.into();
        Self {
            wal_path: wal_path.to_str().unwrap().to_owned(),
            rocksdb_config,
            runtime,
        }
    }

    pub fn build(self) -> Result<RocksImpl> {
        let db = DB::open(self.rocksdb_config, &self.wal_path)
            .map_err(|e| e.into())
            .context(Open {
                wal_path: self.wal_path.clone(),
            })?;
        let rocks_impl = RocksImpl {
            wal_path: self.wal_path,
            db: Arc::new(db),
            runtime: self.runtime,
            log_encoding: LogEncoding::newest(),
            max_seq_meta_encoding: MaxSeqMetaEncoding::newest(),
            regions: RwLock::new(HashMap::new()),
        };
        rocks_impl.build_regions()?;

        Ok(rocks_impl)
    }
}

/// Iterator over log entries based on RocksDB iterator.
pub struct RocksLogIterator {
    log_encoding: LogEncoding,
    /// denotes no more data to iterate and it is set to true when:
    ///  - initialized as no data iterator, or
    ///  - iterate to the end.
    no_more_data: bool,
    min_log_key: LogKey,
    max_log_key: LogKey,
    /// denote whether `iter` is seeked
    seeked: bool,
    /// RocksDB iterator
    iter: DBIterator<Arc<DB>>,
}

impl RocksLogIterator {
    /// Create iterator maybe containing data.
    fn with_data(
        log_encoding: LogEncoding,
        iter: DBIterator<Arc<DB>>,
        min_log_key: LogKey,
        max_log_key: LogKey,
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
    fn new_empty(log_encoding: LogEncoding, iter: DBIterator<Arc<DB>>) -> Self {
        Self {
            log_encoding,
            no_more_data: true,
            min_log_key: (0, 0),
            max_log_key: (0, 0),
            seeked: false,
            iter,
        }
    }

    /// it's a valid log key if it is in the range `[self.min_log_key,
    /// self.max_log_key]`.
    fn is_valid_log_key(&self, curr_log_key: &LogKey) -> bool {
        curr_log_key <= &self.max_log_key && curr_log_key >= &self.min_log_key
    }

    /// End is reached iteration if `curr_log_key` is greater than
    /// `max_log_key`.
    fn is_end_reached(&self, curr_log_key: &LogKey) -> bool {
        curr_log_key >= &self.max_log_key
    }

    /// let `iter` seek to `min_log_key`
    /// no guarantee on that `self.iter` is valid
    fn seek(&mut self) -> Result<()> {
        self.seeked = true;

        let mut seek_key_buf = BytesMut::new();
        self.log_encoding
            .encode_key(&mut seek_key_buf, &self.min_log_key)?;
        let seek_key = SeekKey::Key(&seek_key_buf);
        self.iter
            .seek(seek_key)
            .map_err(|e| e.into())
            .context(Read)?;

        Ok(())
    }
}

impl BlockingLogIterator for RocksLogIterator {
    fn next_log_entry<D: PayloadDecoder>(
        &mut self,
        decoder: &D,
    ) -> Result<Option<LogEntry<D::Target>>> {
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

        let curr_log_key = self.log_encoding.decode_key(self.iter.key())?;
        self.no_more_data = self.is_end_reached(&curr_log_key);

        if self.is_valid_log_key(&curr_log_key) {
            let payload = self.log_encoding.decode_value(self.iter.value(), decoder)?;
            let log_entry = LogEntry {
                sequence: curr_log_key.1,
                payload,
            };
            Ok(Some(log_entry))
        } else {
            Ok(None)
        }
    }
}

#[async_trait]
impl LogReader for RocksImpl {
    type BatchIter = BatchLogIteratorAdapter<RocksLogIterator>;

    async fn read_batch(&self, ctx: &ReadContext, req: &ReadRequest) -> Result<Self::BatchIter> {
        let blocking_iter = if let Some(region) = self.region(req.region_id) {
            region.read(ctx, req)?
        } else {
            let iter = DBIterator::new(self.db.clone(), ReadOptions::default());
            RocksLogIterator::new_empty(self.log_encoding.clone(), iter)
        };
        let runtime = self.runtime.clone();

        Ok(BatchLogIteratorAdapter::new(
            blocking_iter,
            runtime,
            ctx.batch_size,
        ))
    }
}

#[async_trait]
impl LogWriter for RocksImpl {
    async fn write<P: Payload>(
        &self,
        ctx: &WriteContext,
        batch: &LogWriteBatch<P>,
    ) -> Result<SequenceNumber> {
        let region = self.get_or_create_region(batch.region_id);
        region.write(ctx, batch).await
    }
}

#[async_trait]
impl WalManager for RocksImpl {
    async fn sequence_num(&self, region_id: RegionId) -> Result<u64> {
        if let Some(region) = self.region(region_id) {
            return region.sequence_num();
        }

        Ok(MIN_SEQUENCE_NUMBER)
    }

    async fn mark_delete_entries_up_to(
        &self,
        region_id: RegionId,
        sequence_num: SequenceNumber,
    ) -> Result<()> {
        if let Some(region) = self.region(region_id) {
            return region.delete_entries_up_to(sequence_num).await;
        }

        Ok(())
    }

    async fn close_gracefully(&self) -> Result<()> {
        info!("Close rocksdb wal gracefully");

        Ok(())
    }
}

impl fmt::Debug for RocksImpl {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RocksImpl")
            .field("wal_path", &self.wal_path)
            .finish()
    }
}
