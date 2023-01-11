// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Sst file and storage info

use std::{
    borrow::Borrow,
    collections::{BTreeMap, HashSet},
    fmt,
    fmt::Debug,
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use common_types::{
    time::{TimeRange, Timestamp},
    SequenceNumber,
};
use common_util::{
    define_result,
    metric::Meter,
    runtime::{JoinHandle, Runtime},
};
use log::{debug, error, info};
use object_store::ObjectStoreRef;
use snafu::{ResultExt, Snafu};
use table_engine::table::TableId;
use tokio::sync::{
    mpsc::{self, UnboundedReceiver, UnboundedSender},
    Mutex,
};

use crate::{space::SpaceId, sst::manager::FileId, table::sst_util, table_options::StorageFormat};

/// Error of sst file.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to join purger, err:{}", source))]
    StopPurger { source: common_util::runtime::Error },
}

define_result!(Error);

pub type Level = u16;

// TODO(yingwen): Order or split file by time range to speed up filter (even in
//  level 0).
/// Manage files of single level
pub struct LevelHandler {
    pub level: Level,
    /// All files in current level.
    files: FileHandleSet,
}

impl LevelHandler {
    pub fn new(level: u16) -> Self {
        Self {
            level,
            files: FileHandleSet::default(),
        }
    }

    #[inline]
    pub fn insert(&mut self, file: FileHandle) {
        self.files.insert(file);
    }

    pub fn latest_sst(&self) -> Option<FileHandle> {
        self.files.latest()
    }

    pub fn pick_ssts(&self, time_range: TimeRange) -> Vec<FileHandle> {
        if self.level == 0 {
            self.files.files_by_time_range(time_range)
        } else {
            Vec::new()
        }
    }

    #[inline]
    pub fn remove_ssts(&mut self, file_ids: &[FileId]) {
        self.files.remove_by_ids(file_ids);
    }

    pub fn iter_ssts(&self) -> Iter {
        let iter = self.files.file_map.values();
        Iter(iter)
    }

    #[inline]
    pub fn collect_expired(
        &self,
        expire_time: Option<Timestamp>,
        expired_files: &mut Vec<FileHandle>,
    ) {
        self.files.collect_expired(expire_time, expired_files);
    }

    #[inline]
    pub fn has_expired_sst(&self, expire_time: Option<Timestamp>) -> bool {
        self.files.has_expired_sst(expire_time)
    }
}

pub struct Iter<'a>(std::collections::btree_map::Values<'a, FileOrdKey, FileHandle>);

impl<'a> Iterator for Iter<'a> {
    type Item = &'a FileHandle;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

#[derive(Clone)]
pub struct FileHandle {
    inner: Arc<FileHandleInner>,
}

impl PartialEq for FileHandle {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}

impl Eq for FileHandle {}

impl Hash for FileHandle {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id().hash(state);
    }
}

impl FileHandle {
    pub fn new(meta: FileMeta, purge_queue: FilePurgeQueue) -> Self {
        Self {
            inner: Arc::new(FileHandleInner {
                meta,
                purge_queue,
                being_compacted: AtomicBool::new(false),
                metrics: SstMetrics::default(),
            }),
        }
    }

    #[inline]
    pub fn read_meter(&self) -> Arc<Meter> {
        self.inner.metrics.read_meter.clone()
    }

    #[inline]
    pub fn row_num(&self) -> u64 {
        self.inner.meta.row_num
    }

    #[inline]
    pub fn id(&self) -> FileId {
        self.inner.meta.id
    }

    #[inline]
    pub fn id_ref(&self) -> &FileId {
        &self.inner.meta.id
    }

    #[inline]
    pub fn intersect_with_time_range(&self, time_range: TimeRange) -> bool {
        self.inner.meta.intersect_with_time_range(time_range)
    }

    #[inline]
    pub fn time_range(&self) -> TimeRange {
        self.inner.meta.time_range
    }

    #[inline]
    pub fn time_range_ref(&self) -> &TimeRange {
        &self.inner.meta.time_range
    }

    #[inline]
    pub fn max_sequence(&self) -> SequenceNumber {
        self.inner.meta.max_seq
    }

    #[inline]
    pub fn being_compacted(&self) -> bool {
        self.inner.being_compacted.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn size(&self) -> u64 {
        self.inner.meta.size
    }

    #[inline]
    pub fn set_being_compacted(&self, value: bool) {
        self.inner.being_compacted.store(value, Ordering::Relaxed);
    }

    #[inline]
    pub fn storage_format(&self) -> StorageFormat {
        self.inner.meta.storage_format
    }
}

impl fmt::Debug for FileHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileHandle")
            .field("meta", &self.inner.meta)
            .field("being_compacted", &self.being_compacted())
            .field("metrics", &self.inner.metrics)
            .finish()
    }
}

struct SstMetrics {
    pub read_meter: Arc<Meter>,
    pub key_num: usize,
}

impl Default for SstMetrics {
    fn default() -> Self {
        SstMetrics {
            read_meter: Arc::new(Meter::new()),
            key_num: 0,
        }
    }
}

impl fmt::Debug for SstMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SstMetrics")
            .field("read_meter", &self.read_meter.h2_rate())
            .field("key_num", &self.key_num)
            .finish()
    }
}

struct FileHandleInner {
    meta: FileMeta,
    purge_queue: FilePurgeQueue,
    /// The file is being compacting.
    being_compacted: AtomicBool,
    metrics: SstMetrics,
}

impl Drop for FileHandleInner {
    fn drop(&mut self) {
        debug!("FileHandle is dropped, meta:{:?}", self.meta);

        // Push file cannot block or be async because we are in drop().
        self.purge_queue.push_file(self.meta.id);
    }
}

/// Used to order [FileHandle] by (end_time, start_time, file_id)
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct FileOrdKey {
    exclusive_end: Timestamp,
    inclusive_start: Timestamp,
    file_id: FileId,
}

impl FileOrdKey {
    fn for_seek(exclusive_end: Timestamp) -> Self {
        Self {
            exclusive_end,
            inclusive_start: Timestamp::MIN,
            file_id: 0,
        }
    }

    fn key_of(file: &FileHandle) -> Self {
        Self {
            exclusive_end: file.time_range().exclusive_end(),
            inclusive_start: file.time_range().inclusive_start(),
            file_id: file.id(),
        }
    }
}

/// Used to index [FileHandle] by file_id
struct FileHandleHash(FileHandle);

impl PartialEq for FileHandleHash {
    fn eq(&self, other: &Self) -> bool {
        self.0.id() == other.0.id()
    }
}

impl Eq for FileHandleHash {}

impl Hash for FileHandleHash {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.id().hash(state);
    }
}

impl Borrow<FileId> for FileHandleHash {
    #[inline]
    fn borrow(&self) -> &FileId {
        self.0.id_ref()
    }
}

#[derive(Default)]
struct FileHandleSet {
    /// Files ordered by time range and id.
    file_map: BTreeMap<FileOrdKey, FileHandle>,
    /// Files indexed by file id, used to speed up removal.
    id_to_files: HashSet<FileHandleHash>,
}

impl FileHandleSet {
    fn latest(&self) -> Option<FileHandle> {
        if let Some(file) = self.file_map.values().rev().next() {
            return Some(file.clone());
        }
        None
    }

    fn files_by_time_range(&self, time_range: TimeRange) -> Vec<FileHandle> {
        // Seek to first sst whose end time >= time_range.inclusive_start().
        let seek_key = FileOrdKey::for_seek(time_range.inclusive_start());
        self.file_map
            .range(seek_key..)
            .into_iter()
            .filter_map(|(_key, file)| {
                if file.intersect_with_time_range(time_range) {
                    Some(file.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    fn insert(&mut self, file: FileHandle) {
        self.file_map
            .insert(FileOrdKey::key_of(&file), file.clone());
        self.id_to_files.insert(FileHandleHash(file));
    }

    fn remove_by_ids(&mut self, file_ids: &[FileId]) {
        for file_id in file_ids {
            if let Some(file) = self.id_to_files.take(file_id) {
                let key = FileOrdKey::key_of(&file.0);
                self.file_map.remove(&key);
            }
        }
    }

    /// Collect ssts with time range is expired.
    fn collect_expired(&self, expire_time: Option<Timestamp>, expired_files: &mut Vec<FileHandle>) {
        for file in self.file_map.values() {
            if file.time_range().is_expired(expire_time) {
                expired_files.push(file.clone());
            } else {
                // Files are sorted by end time first, so there is no more file whose end time
                // is less than `expire_time`.
                break;
            }
        }
    }

    fn has_expired_sst(&self, expire_time: Option<Timestamp>) -> bool {
        // Files are sorted by end time first, so check first file is enough.
        if let Some(file) = self.file_map.values().next() {
            return file.time_range().is_expired(expire_time);
        }

        false
    }
}

/// Meta of a sst file, immutable once created
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMeta {
    /// Id of the sst file
    pub id: FileId,
    /// File size in bytes
    pub size: u64,
    /// Total row number
    pub row_num: u64,
    /// The time range of the file.
    pub time_range: TimeRange,
    /// The max sequence number of the file.
    pub max_seq: u64,
    /// The format of the file.
    pub storage_format: StorageFormat,
}

impl FileMeta {
    pub fn intersect_with_time_range(&self, time_range: TimeRange) -> bool {
        self.time_range.intersect_with(time_range)
    }
}

// Queue to store files to be deleted for a table.
#[derive(Clone)]
pub struct FilePurgeQueue {
    // Wrap a inner struct to avoid storing space/table ids for each file.
    inner: Arc<FilePurgeQueueInner>,
}

impl FilePurgeQueue {
    pub fn new(space_id: SpaceId, table_id: TableId, sender: UnboundedSender<Request>) -> Self {
        Self {
            inner: Arc::new(FilePurgeQueueInner {
                space_id,
                table_id,
                sender,
                closed: AtomicBool::new(false),
            }),
        }
    }

    /// Close the purge queue, then all request pushed to this queue will be
    /// ignored. This is mainly used to avoid files being deleted after the
    /// db is closed.
    pub fn close(&self) {
        self.inner.closed.store(true, Ordering::SeqCst);
    }

    fn push_file(&self, file_id: FileId) {
        if self.inner.closed.load(Ordering::SeqCst) {
            return;
        }

        // Send the file id via a channel to file purger and delete the file from sst
        // store in background.
        let request = FilePurgeRequest {
            space_id: self.inner.space_id,
            table_id: self.inner.table_id,
            file_id,
        };

        if let Err(send_res) = self.inner.sender.send(Request::Purge(request)) {
            error!(
                "Failed to send delete file request, request:{:?}",
                send_res.0
            );
        }
    }
}

struct FilePurgeQueueInner {
    space_id: SpaceId,
    table_id: TableId,
    closed: AtomicBool,
    sender: UnboundedSender<Request>,
}

#[derive(Debug)]
pub struct FilePurgeRequest {
    space_id: SpaceId,
    table_id: TableId,
    file_id: FileId,
}

#[derive(Debug)]
pub enum Request {
    Purge(FilePurgeRequest),
    Exit,
}

/// Background file purger.
pub struct FilePurger {
    sender: UnboundedSender<Request>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl FilePurger {
    pub fn start(runtime: &Runtime, store: ObjectStoreRef) -> Self {
        // We must use unbound channel, so the sender wont block when the handle is
        // dropped.
        let (tx, rx) = mpsc::unbounded_channel();

        // Spawn a background job to purge files.
        let handle = runtime.spawn(async {
            Self::purge_file_loop(store, rx).await;
        });

        Self {
            sender: tx,
            handle: Mutex::new(Some(handle)),
        }
    }

    pub async fn stop(&self) -> Result<()> {
        info!("Try to stop file purger");

        if self.sender.send(Request::Exit).is_err() {
            error!("File purge task already exited");
        }

        let mut handle = self.handle.lock().await;
        // Also clear the handle to avoid await a ready future.
        if let Some(h) = handle.take() {
            h.await.context(StopPurger)?;
        }

        Ok(())
    }

    pub fn create_purge_queue(&self, space_id: SpaceId, table_id: TableId) -> FilePurgeQueue {
        FilePurgeQueue::new(space_id, table_id, self.sender.clone())
    }

    async fn purge_file_loop(store: ObjectStoreRef, mut receiver: UnboundedReceiver<Request>) {
        info!("File purger start");

        while let Some(request) = receiver.recv().await {
            match request {
                Request::Purge(purge_request) => {
                    let sst_file_path = sst_util::new_sst_file_path(
                        purge_request.space_id,
                        purge_request.table_id,
                        purge_request.file_id,
                    );

                    info!(
                        "File purger delete file, purge_request:{:?}, sst_file_path:{}",
                        purge_request,
                        sst_file_path.to_string()
                    );

                    if let Err(e) = store.delete(&sst_file_path).await {
                        error!(
                            "File purger failed to delete file, sst_file_path:{}, err:{}",
                            sst_file_path.to_string(),
                            e
                        );
                    }
                }
                Request::Exit => break,
            }
        }

        info!("File purger exit");
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    pub struct FilePurgerMocker;

    impl FilePurgerMocker {
        pub fn mock() -> FilePurger {
            let (sender, _receiver) = mpsc::unbounded_channel();

            FilePurger {
                sender,
                handle: Mutex::new(None),
            }
        }
    }
}
