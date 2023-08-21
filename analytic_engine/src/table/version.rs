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

//! Table version

use std::{
    cmp,
    collections::{BTreeMap, HashMap},
    fmt,
    ops::Bound,
    sync::{Arc, RwLock},
    time::Duration,
};

use common_types::{
    row::Row,
    schema::{self, Schema},
    time::{TimeRange, Timestamp},
    SequenceNumber,
};
use macros::define_result;
use snafu::{ensure, Backtrace, ResultExt, Snafu};

use crate::{
    compaction::{
        picker::{self, CompactionPickerRef, PickerContext},
        CompactionTask, ExpiredFiles,
    },
    memtable::{self, key::KeySequence, MemTableRef, PutContext},
    sampler::{DefaultSampler, SamplerRef},
    sst::{
        file::{FileHandle, FilePurgeQueue, SST_LEVEL_NUM},
        manager::{FileId, LevelsController},
    },
    table::{
        data::{MemTableId, DEFAULT_ALLOC_STEP},
        version_edit::{AddFile, VersionEdit},
    },
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Schema mismatch, memtable_version:{}, given:{}.\nBacktrace:\n{}",
        memtable_version,
        given,
        backtrace
    ))]
    SchemaMismatch {
        memtable_version: schema::Version,
        given: schema::Version,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to put memtable, err:{}", source))]
    PutMemTable { source: crate::memtable::Error },

    #[snafu(display("Failed to collect timestamp, err:{}", source))]
    CollectTimestamp { source: crate::sampler::Error },
}

define_result!(Error);

/// Memtable for sampling timestamp.
#[derive(Clone)]
pub struct SamplingMemTable {
    pub mem: MemTableRef,
    pub id: MemTableId,
    /// If freezed is true, the sampling is finished and no more data should be
    /// inserted into this memtable. Otherwise, the memtable is active and all
    /// data should ONLY write to this memtable instead of mutable memtable.
    pub freezed: bool,
    pub sampler: SamplerRef,
}

impl SamplingMemTable {
    pub fn new(memtable: MemTableRef, id: MemTableId) -> Self {
        SamplingMemTable {
            mem: memtable,
            id,
            freezed: false,
            sampler: Arc::new(DefaultSampler::default()),
        }
    }

    pub fn last_sequence(&self) -> SequenceNumber {
        self.mem.last_sequence()
    }

    fn memory_usage(&self) -> usize {
        self.mem.approximate_memory_usage()
    }

    /// Suggest segment duration, if there is no sampled timestamp, returns
    /// default segment duration.
    fn suggest_segment_duration(&self) -> Duration {
        self.sampler.suggest_duration()
    }
}

impl fmt::Debug for SamplingMemTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SamplingMemTable")
            .field("id", &self.id)
            .field("freezed", &self.freezed)
            .finish()
    }
}

/// Memtable with additional meta data
#[derive(Clone)]
pub struct MemTableState {
    /// The mutable memtable
    pub mem: MemTableRef,
    /// The `time_range` is estimated via the time range of the first row group
    /// write to this memtable and is aligned to segment size
    pub time_range: TimeRange,
    /// Id of the memtable, newer memtable has greater id
    pub id: MemTableId,
}

impl MemTableState {
    #[inline]
    pub fn last_sequence(&self) -> SequenceNumber {
        self.mem.last_sequence()
    }
}

impl fmt::Debug for MemTableState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemTableState")
            .field("time_range", &self.time_range)
            .field("id", &self.id)
            .field("mem", &self.mem.approximate_memory_usage())
            .field("metrics", &self.mem.metrics())
            .field("last_sequence", &self.mem.last_sequence())
            .finish()
    }
}

// TODO(yingwen): Replace by Either.
#[derive(Clone)]
pub enum MemTableForWrite {
    Sampling(SamplingMemTable),
    Normal(MemTableState),
}

impl MemTableForWrite {
    #[inline]
    pub fn set_last_sequence(&self, seq: SequenceNumber) -> memtable::Result<()> {
        self.memtable().set_last_sequence(seq)
    }

    #[inline]
    pub fn accept_timestamp(&self, timestamp: Timestamp) -> bool {
        match self {
            MemTableForWrite::Sampling(_) => true,
            MemTableForWrite::Normal(v) => v.time_range.contains(timestamp),
        }
    }

    #[inline]
    pub fn put(
        &self,
        ctx: &mut PutContext,
        sequence: KeySequence,
        row: &Row,
        schema: &Schema,
        timestamp: Timestamp,
    ) -> Result<()> {
        match self {
            MemTableForWrite::Sampling(v) => {
                v.mem.put(ctx, sequence, row, schema).context(PutMemTable)?;

                // Collect the timestamp of this row.
                v.sampler.collect(timestamp).context(CollectTimestamp)?;

                Ok(())
            }
            MemTableForWrite::Normal(v) => {
                v.mem.put(ctx, sequence, row, schema).context(PutMemTable)
            }
        }
    }

    #[inline]
    fn memtable(&self) -> &MemTableRef {
        match self {
            MemTableForWrite::Sampling(v) => &v.mem,
            MemTableForWrite::Normal(v) => &v.mem,
        }
    }

    #[cfg(test)]
    pub fn as_sampling(&self) -> &SamplingMemTable {
        match self {
            MemTableForWrite::Sampling(v) => v,
            MemTableForWrite::Normal(_) => panic!(),
        }
    }

    #[cfg(test)]
    pub fn as_normal(&self) -> &MemTableState {
        match self {
            MemTableForWrite::Sampling(_) => panic!(),
            MemTableForWrite::Normal(v) => v,
        }
    }
}

#[derive(Debug, Default)]
pub struct FlushableMemTables {
    pub sampling_mem: Option<SamplingMemTable>,
    pub memtables: MemTableVec,
}

impl FlushableMemTables {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.sampling_mem.is_none() && self.memtables.is_empty()
    }

    pub fn ids(&self) -> Vec<MemTableId> {
        let mut memtable_ids = Vec::with_capacity(self.memtables.len() + 1);
        if let Some(v) = &self.sampling_mem {
            memtable_ids.push(v.id);
        }
        for mem in &self.memtables {
            memtable_ids.push(mem.id);
        }

        memtable_ids
    }

    pub fn len(&self) -> usize {
        self.sampling_mem.as_ref().map_or(0, |_| 1) + self.memtables.len()
    }
}

/// Vec to store memtables
pub type MemTableVec = Vec<MemTableState>;

/// MemTableView holds all memtables of the table
#[derive(Debug)]
struct MemTableView {
    /// The memtable for sampling timestamp to suggest segment duration.
    ///
    /// This memtable is special and may contains data in differnt segment, so
    /// can not be moved into immutable memtable set.
    sampling_mem: Option<SamplingMemTable>,
    /// Mutable memtables arranged by its time range.
    mutables: MutableMemTableSet,
    /// Immutable memtables set, lookup by memtable id is fast.
    immutables: ImmutableMemTableSet,
}

impl MemTableView {
    fn new() -> Self {
        Self {
            sampling_mem: None,
            mutables: MutableMemTableSet::new(),
            immutables: ImmutableMemTableSet(BTreeMap::new()),
        }
    }

    /// Get the memory usage of mutable memtables.
    fn mutable_memory_usage(&self) -> usize {
        self.mutables.memory_usage()
            + self
                .sampling_mem
                .as_ref()
                .map(|v| v.memory_usage())
                .unwrap_or(0)
    }

    /// Get the total memory usage of mutable and immutable memtables.
    fn total_memory_usage(&self) -> usize {
        let mutable_usage = self.mutable_memory_usage();
        let immutable_usage = self.immutables.memory_usage();

        mutable_usage + immutable_usage
    }

    /// Instead of replace the old memtable by a new memtable, we just move the
    /// old memtable to immutable memtables and left mutable memtables
    /// empty. New mutable memtable will be constructed via put request.
    fn switch_memtables(&mut self) -> Option<SequenceNumber> {
        self.mutables.move_to_inmem(&mut self.immutables)
    }

    /// Sample the segment duration.
    ///
    /// If the sampling memtable is still active, return the suggested segment
    /// duration or move all mutable memtables into immutable memtables if
    /// the sampling memtable is freezed and returns None.
    fn suggest_duration(&mut self) -> Option<Duration> {
        if let Some(v) = &mut self.sampling_mem {
            if !v.freezed {
                // Other memtable should be empty during sampling phase.
                assert!(self.mutables.is_empty());
                assert!(self.immutables.is_empty());

                // The sampling memtable is still active, we need to compute the
                // segment duration and then freeze the memtable.
                let segment_duration = v.suggest_segment_duration();

                // But we cannot freeze the sampling memtable now, because the
                // segment duration may not yet been persisted.
                return Some(segment_duration);
            }
        }

        None
    }

    fn freeze_sampling_memtable(&mut self) -> Option<SequenceNumber> {
        if let Some(v) = &mut self.sampling_mem {
            v.freezed = true;
            return Some(v.mem.last_sequence());
        }
        None
    }

    /// Returns memtables need to be flushed. Only sampling memtable and
    /// immutables will be considered. And only memtables which `last_sequence`
    /// less or equal to the given [SequenceNumber] will be picked.
    ///
    /// This method assumes that one sequence number will not exist in multiple
    /// memtables.
    fn pick_memtables_to_flush(&self, last_sequence: SequenceNumber) -> FlushableMemTables {
        let mut mems = FlushableMemTables::default();

        if let Some(v) = &self.sampling_mem {
            if v.last_sequence() <= last_sequence {
                mems.sampling_mem = Some(v.clone());
            }
        }

        for mem in self.immutables.0.values() {
            if mem.last_sequence() <= last_sequence {
                mems.memtables.push(mem.clone());
            }
        }

        mems
    }

    /// Remove memtable from immutables or sampling memtable.
    #[inline]
    fn remove_immutable_or_sampling(&mut self, id: MemTableId) {
        if let Some(v) = &self.sampling_mem {
            if v.id == id {
                self.sampling_mem = None;
                return;
            }
        }

        self.immutables.0.remove(&id);
    }

    /// Collect memtables itersect with `time_range`
    fn memtables_for_read(
        &self,
        time_range: TimeRange,
        mems: &mut MemTableVec,
        sampling_mem: &mut Option<SamplingMemTable>,
    ) {
        self.mutables.memtables_for_read(time_range, mems);

        self.immutables.memtables_for_read(time_range, mems);

        *sampling_mem = self.sampling_mem.clone();
    }
}

/// Mutable memtables
///
/// All mutable memtables ordered by their end time (exclusive), their time
/// range may overlaps if `alter segment duration` is supported
///
/// We choose end time so we can use BTreeMap::range to find the first range
/// that may contains a given timestamp (end >= timestamp)
#[derive(Debug)]
struct MutableMemTableSet(BTreeMap<Timestamp, MemTableState>);

impl MutableMemTableSet {
    fn new() -> Self {
        Self(BTreeMap::new())
    }

    /// Get memtale by timestamp for write
    fn memtable_for_write(&self, timestamp: Timestamp) -> Option<&MemTableState> {
        // Find the first memtable whose end time (exclusive) > timestamp
        if let Some((_, memtable)) = self
            .0
            .range((Bound::Excluded(timestamp), Bound::Unbounded))
            .next()
        {
            if memtable.time_range.contains(timestamp) {
                return Some(memtable);
            }
        }

        None
    }

    /// Insert memtable, the caller should guarantee the key of memtable is not
    /// present.
    fn insert(&mut self, memtable: MemTableState) -> Option<MemTableState> {
        // Use end time of time range as key
        let end = memtable.time_range.exclusive_end();
        self.0.insert(end, memtable)
    }

    fn memory_usage(&self) -> usize {
        self.0
            .values()
            .map(|m| m.mem.approximate_memory_usage())
            .sum()
    }

    /// Move all mutable memtables to immutable memtables.
    fn move_to_inmem(&mut self, immem: &mut ImmutableMemTableSet) -> Option<SequenceNumber> {
        let last_seq = self
            .0
            .values()
            .map(|m| {
                let last_sequence = m.mem.last_sequence();
                immem.0.insert(m.id, m.clone());

                last_sequence
            })
            .max();

        self.0.clear();
        last_seq
    }

    fn memtables_for_read(&self, time_range: TimeRange, mems: &mut MemTableVec) {
        // Seek to first memtable whose end time (exclusive) > time_range.start
        let inclusive_start = time_range.inclusive_start();
        let iter = self
            .0
            .range((Bound::Excluded(inclusive_start), Bound::Unbounded));
        for (_end_ts, mem) in iter {
            // We need to iterate all candidate memtables as their start time is unspecific
            if mem.time_range.intersect_with(time_range) {
                mems.push(mem.clone());
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Immutable memtables set
///
/// MemTables are ordered by memtable id, so lookup by memtable id is fast
#[derive(Debug)]
struct ImmutableMemTableSet(BTreeMap<MemTableId, MemTableState>);

impl ImmutableMemTableSet {
    /// Memory used by all immutable memtables
    fn memory_usage(&self) -> usize {
        self.0
            .values()
            .map(|m| m.mem.approximate_memory_usage())
            .sum()
    }

    fn memtables_for_read(&self, time_range: TimeRange, mems: &mut MemTableVec) {
        for mem in self.0.values() {
            if mem.time_range.intersect_with(time_range) {
                mems.push(mem.clone());
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

pub type LeveledFiles = Vec<Vec<FileHandle>>;

/// Memtable/sst to read for given time range.
pub struct ReadView {
    pub sampling_mem: Option<SamplingMemTable>,
    pub memtables: MemTableVec,
    /// Ssts to read in each level.
    ///
    /// The `ReadView` MUST ensure the length of `leveled_ssts` >= MAX_LEVEL.
    pub leveled_ssts: LeveledFiles,
}

impl Default for ReadView {
    fn default() -> Self {
        Self {
            sampling_mem: None,
            memtables: Vec::new(),
            leveled_ssts: vec![Vec::new(); SST_LEVEL_NUM],
        }
    }
}

impl ReadView {
    pub fn contains_sampling(&self) -> bool {
        self.sampling_mem.is_some()
    }
}

/// Data of TableVersion
struct TableVersionInner {
    /// All memtables
    memtable_view: MemTableView,
    /// All ssts
    levels_controller: LevelsController,

    /// The earliest sequence number of the entries already flushed (inclusive).
    /// All log entry with sequence <= `flushed_sequence` can be deleted
    flushed_sequence: SequenceNumber,
    /// Max id of the sst file.
    ///
    /// The id is allocated by step, so there are some still unused ids smaller
    /// than the max one. And this field is only a mem state for Manifest,
    /// it can only be updated during recover or by Manifest.
    max_file_id: FileId,
}

impl TableVersionInner {
    fn memtable_for_write(&self, timestamp: Timestamp) -> Option<MemTableForWrite> {
        if let Some(mem) = self.memtable_view.sampling_mem.clone() {
            if !mem.freezed {
                // If sampling memtable is not freezed.
                return Some(MemTableForWrite::Sampling(mem));
            }
        }

        self.memtable_view
            .mutables
            .memtable_for_write(timestamp)
            .cloned()
            .map(MemTableForWrite::Normal)
    }
}

// TODO(yingwen): How to support snapshot?
/// Table version
///
/// Holds memtables and sst meta data of a table
///
/// Switching memtable, memtable to level 0 file, addition/deletion to files
/// should be done atomically.
pub struct TableVersion {
    inner: RwLock<TableVersionInner>,
}

impl TableVersion {
    /// Create an empty table version
    pub fn new(purge_queue: FilePurgeQueue) -> Self {
        Self {
            inner: RwLock::new(TableVersionInner {
                memtable_view: MemTableView::new(),
                levels_controller: LevelsController::new(purge_queue),
                flushed_sequence: 0,
                max_file_id: 0,
            }),
        }
    }

    /// See [MemTableView::mutable_memory_usage]
    pub fn mutable_memory_usage(&self) -> usize {
        self.inner
            .read()
            .unwrap()
            .memtable_view
            .mutable_memory_usage()
    }

    /// See [MemTableView::total_memory_usage]
    pub fn total_memory_usage(&self) -> usize {
        self.inner
            .read()
            .unwrap()
            .memtable_view
            .total_memory_usage()
    }

    /// Return the suggested segment duration if sampling memtable is still
    /// active.
    pub fn suggest_duration(&self) -> Option<Duration> {
        self.inner.write().unwrap().memtable_view.suggest_duration()
    }

    /// Switch all mutable memtables
    ///
    /// Returns the maxium `SequenceNumber` in the mutable memtables needs to be
    /// freezed.
    pub fn switch_memtables(&self) -> Option<SequenceNumber> {
        self.inner.write().unwrap().memtable_view.switch_memtables()
    }

    /// Stop timestamp sampling and freezed the sampling memtable.
    ///
    /// REQUIRE: Do in write worker
    pub fn freeze_sampling_memtable(&self) -> Option<SequenceNumber> {
        self.inner
            .write()
            .unwrap()
            .memtable_view
            .freeze_sampling_memtable()
    }

    /// See [MemTableView::pick_memtables_to_flush]
    pub fn pick_memtables_to_flush(&self, last_sequence: SequenceNumber) -> FlushableMemTables {
        self.inner
            .read()
            .unwrap()
            .memtable_view
            .pick_memtables_to_flush(last_sequence)
    }

    /// Get memtable by timestamp for write.
    ///
    /// The returned schema is guaranteed to have schema with same version as
    /// `schema_version`. Return None if the schema of existing memtable has
    /// different schema.
    pub fn memtable_for_write(
        &self,
        timestamp: Timestamp,
        schema_version: schema::Version,
    ) -> Result<Option<MemTableForWrite>> {
        // Find memtable by timestamp
        let memtable = self.inner.read().unwrap().memtable_for_write(timestamp);
        let mutable = match memtable {
            Some(v) => v,
            None => return Ok(None),
        };

        // We consider the schemas are same if they have the same version.
        ensure!(
            mutable.memtable().schema().version() == schema_version,
            SchemaMismatch {
                memtable_version: mutable.memtable().schema().version(),
                given: schema_version,
            }
        );

        Ok(Some(mutable))
    }

    /// Insert memtable into mutable memtable set.
    pub fn insert_mutable(&self, mem_state: MemTableState) {
        let mut inner = self.inner.write().unwrap();
        let old_memtable = inner.memtable_view.mutables.insert(mem_state.clone());
        assert!(
            old_memtable.is_none(),
            "Find a duplicate memtable, new_memtable:{:?}, old_memtable:{:?}, memtable_view:{:#?}",
            mem_state,
            old_memtable,
            inner.memtable_view
        );
    }

    /// Set sampling memtable.
    ///
    /// Panic if the sampling memtable of this version is not None.
    pub fn set_sampling(&self, sampling_mem: SamplingMemTable) {
        let mut inner = self.inner.write().unwrap();
        assert!(inner.memtable_view.sampling_mem.is_none());
        inner.memtable_view.sampling_mem = Some(sampling_mem);
    }

    /// Atomically apply the edit to the version.
    pub fn apply_edit(&self, edit: VersionEdit) {
        let mut inner = self.inner.write().unwrap();

        // TODO(yingwen): else, log warning
        inner.flushed_sequence = cmp::max(inner.flushed_sequence, edit.flushed_sequence);

        inner.max_file_id = cmp::max(inner.max_file_id, edit.max_file_id);

        // Add sst files to level first.
        for add_file in edit.files_to_add {
            inner
                .levels_controller
                .add_sst_to_level(add_file.level, add_file.file);
        }

        // Remove ssts from level.
        for delete_file in edit.files_to_delete {
            inner
                .levels_controller
                .remove_ssts_from_level(delete_file.level, &[delete_file.file_id]);
        }

        // Remove immutable memtables.
        for mem_id in edit.mems_to_remove {
            inner.memtable_view.remove_immutable_or_sampling(mem_id);
        }
    }

    /// Atomically apply the meta to the version, useful in recover.
    pub fn apply_meta(&self, meta: TableVersionMeta) {
        let mut inner = self.inner.write().unwrap();

        inner.flushed_sequence = cmp::max(inner.flushed_sequence, meta.flushed_sequence);

        inner.max_file_id = cmp::max(inner.max_file_id, meta.max_file_id);

        for add_file in meta.files.into_values() {
            inner
                .levels_controller
                .add_sst_to_level(add_file.level, add_file.file);
        }
    }

    pub fn pick_read_view(&self, time_range: TimeRange) -> ReadView {
        let mut sampling_mem = None;
        let mut memtables = MemTableVec::new();
        let mut leveled_ssts = vec![Vec::new(); SST_LEVEL_NUM];

        {
            // Pick memtables for read.
            let inner = self.inner.read().unwrap();

            inner
                .memtable_view
                .memtables_for_read(time_range, &mut memtables, &mut sampling_mem);

            // Pick ssts for read.
            inner
                .levels_controller
                .pick_ssts(time_range, |level, ssts| {
                    leveled_ssts[level.as_usize()].extend_from_slice(ssts)
                });
        }

        ReadView {
            sampling_mem,
            memtables,
            leveled_ssts,
        }
    }

    /// Pick ssts for compaction using given `picker`.
    pub fn pick_for_compaction(
        &self,
        picker_ctx: PickerContext,
        picker: &CompactionPickerRef,
    ) -> picker::Result<CompactionTask> {
        let mut inner = self.inner.write().unwrap();

        picker.pick_compaction(picker_ctx, &mut inner.levels_controller)
    }

    pub fn has_expired_sst(&self, expire_time: Option<Timestamp>) -> bool {
        let inner = self.inner.read().unwrap();

        inner.levels_controller.has_expired_sst(expire_time)
    }

    pub fn expired_ssts(&self, expire_time: Option<Timestamp>) -> Vec<ExpiredFiles> {
        let inner = self.inner.read().unwrap();

        inner.levels_controller.expired_ssts(expire_time)
    }

    pub fn flushed_sequence(&self) -> SequenceNumber {
        let inner = self.inner.read().unwrap();

        inner.flushed_sequence
    }

    pub fn snapshot(&self) -> TableVersionSnapshot {
        let inner = self.inner.read().unwrap();
        let controller = &inner.levels_controller;
        let files = controller
            .levels()
            .flat_map(|level| {
                let ssts = controller.iter_ssts_at_level(level);
                ssts.map(move |file| {
                    let add_file = AddFile {
                        level,
                        file: file.meta(),
                    };
                    (file.id(), add_file)
                })
            })
            .collect();

        TableVersionSnapshot {
            flushed_sequence: inner.flushed_sequence,
            files,
            max_file_id: inner.max_file_id,
        }
    }
}

pub struct TableVersionSnapshot {
    pub flushed_sequence: SequenceNumber,
    pub files: HashMap<FileId, AddFile>,
    pub max_file_id: FileId,
}

/// During recovery, we apply all version edit to [TableVersionMeta] first, then
/// apply the version meta to the table, so we can avoid adding removed ssts to
/// the version.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TableVersionMeta {
    pub flushed_sequence: SequenceNumber,
    pub files: HashMap<FileId, AddFile>,
    pub max_file_id: FileId,
}

impl TableVersionMeta {
    pub fn apply_edit(&mut self, edit: VersionEdit) {
        self.flushed_sequence = cmp::max(self.flushed_sequence, edit.flushed_sequence);

        for add_file in edit.files_to_add {
            self.max_file_id = cmp::max(self.max_file_id, add_file.file.id);

            self.files.insert(add_file.file.id, add_file);
        }

        self.max_file_id = cmp::max(self.max_file_id, edit.max_file_id);

        // aligned max file id.
        self.max_file_id =
            (self.max_file_id + DEFAULT_ALLOC_STEP - 1) / DEFAULT_ALLOC_STEP * DEFAULT_ALLOC_STEP;

        for delete_file in edit.files_to_delete {
            self.files.remove(&delete_file.file_id);
        }
    }

    /// Returns the max file id in the files to add.
    pub fn max_file_id_to_add(&self) -> FileId {
        self.max_file_id
    }

    pub fn ordered_files(&self) -> Vec<AddFile> {
        let mut files_vec: Vec<_> = self.files.values().cloned().collect();
        files_vec.sort_unstable_by_key(|file| file.file.id);

        files_vec
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        sst::file::tests::FilePurgerMocker,
        table::{data::tests::MemTableMocker, version_edit::tests::AddFileMocker},
        table_options,
        tests::table,
    };

    fn new_table_version() -> TableVersion {
        let purger = FilePurgerMocker::mock();
        let queue = purger.create_purge_queue(1, table::new_table_id(2, 2));
        TableVersion::new(queue)
    }

    #[test]
    fn test_empty_table_version() {
        let version = new_table_version();

        let ts = Timestamp::now();
        assert!(!version.has_expired_sst(None));
        assert!(!version.has_expired_sst(Some(ts)));

        assert_eq!(0, version.mutable_memory_usage());
        assert_eq!(0, version.total_memory_usage());

        {
            let inner = version.inner.read().unwrap();
            let memtable_view = &inner.memtable_view;
            assert!(memtable_view.sampling_mem.is_none());
            assert!(memtable_view.mutables.is_empty());
            assert!(memtable_view.immutables.is_empty());
        }

        let last_sequence = 1000;
        let flushable_mems = version.pick_memtables_to_flush(last_sequence);
        assert!(flushable_mems.is_empty());

        let read_view = version.pick_read_view(TimeRange::min_to_max());
        assert!(!read_view.contains_sampling());

        assert!(read_view.sampling_mem.is_none());
        assert!(read_view.memtables.is_empty());
        for ssts in read_view.leveled_ssts {
            assert!(ssts.is_empty());
        }

        let now = Timestamp::now();
        let mutable = version.memtable_for_write(now, 1).unwrap();
        assert!(mutable.is_none());

        // Nothing to switch.
        assert!(version.suggest_duration().is_none());
        assert!(version.switch_memtables().is_none());
    }

    fn check_flushable_mem_with_sampling(
        flushable_mems: &FlushableMemTables,
        memtable_id: MemTableId,
    ) {
        assert!(!flushable_mems.is_empty());
        assert_eq!(
            memtable_id,
            flushable_mems.sampling_mem.as_ref().unwrap().id
        );
        assert!(flushable_mems.memtables.is_empty());
    }

    #[test]
    fn test_table_version_sampling() {
        let memtable = MemTableMocker::default().build();
        test_table_version_sampling_with_memtable(memtable);
        let memtable = MemTableMocker::default().build_columnar();
        test_table_version_sampling_with_memtable(memtable);
    }

    fn test_table_version_sampling_with_memtable(memtable: MemTableRef) {
        let version = new_table_version();

        let schema = memtable.schema().clone();

        let memtable_id = 1;
        let sampling_mem = SamplingMemTable::new(memtable, memtable_id);

        version.set_sampling(sampling_mem);

        // Should write to sampling memtable.
        let now = Timestamp::now();
        let mutable = version
            .memtable_for_write(now, schema.version())
            .unwrap()
            .unwrap();
        let actual_memtable = mutable.as_sampling();
        assert_eq!(memtable_id, actual_memtable.id);

        let mutable = version
            .memtable_for_write(Timestamp::new(1234), schema.version())
            .unwrap()
            .unwrap();
        let actual_memtable = mutable.as_sampling();
        assert_eq!(memtable_id, actual_memtable.id);

        // Sampling memtable should always be read.
        let read_view = version.pick_read_view(TimeRange::new(0.into(), 123.into()).unwrap());
        assert!(read_view.contains_sampling());
        assert_eq!(memtable_id, read_view.sampling_mem.unwrap().id);

        let last_sequence = 1000;
        let flushable_mems = version.pick_memtables_to_flush(last_sequence);
        check_flushable_mem_with_sampling(&flushable_mems, memtable_id);
    }

    #[test]
    fn test_table_version_sampling_switch() {
        let memtable = MemTableMocker::default().build();
        test_table_version_sampling_switch_with_memtable(memtable);
        let memtable = MemTableMocker::default().build_columnar();
        test_table_version_sampling_switch_with_memtable(memtable);
    }

    fn test_table_version_sampling_switch_with_memtable(memtable: MemTableRef) {
        let version = new_table_version();

        let schema = memtable.schema().clone();

        let memtable_id = 1;
        let last_sequence = 1000;
        let sampling_mem = SamplingMemTable::new(memtable, memtable_id);

        version.set_sampling(sampling_mem);

        let duration = version.suggest_duration().unwrap();
        assert_eq!(table_options::DEFAULT_SEGMENT_DURATION, duration);
        assert!(version.switch_memtables().is_none());

        // Flushable memtables only contains sampling memtable.
        let flushable_mems = version.pick_memtables_to_flush(last_sequence);
        check_flushable_mem_with_sampling(&flushable_mems, memtable_id);

        // Write to memtable after switch and before freezed.
        let now = Timestamp::now();
        let mutable = version
            .memtable_for_write(now, schema.version())
            .unwrap()
            .unwrap();
        // Still write to sampling memtable.
        let actual_memtable = mutable.as_sampling();
        assert_eq!(memtable_id, actual_memtable.id);

        // Switch still return duration before freezed.
        let duration = version.suggest_duration().unwrap();
        assert_eq!(table_options::DEFAULT_SEGMENT_DURATION, duration);
        assert!(version.switch_memtables().is_none());

        version.switch_memtables();
        // Flushable memtables only contains sampling memtable before sampling
        // memtable is freezed.
        let flushable_mems = version.pick_memtables_to_flush(last_sequence);
        check_flushable_mem_with_sampling(&flushable_mems, memtable_id);
    }

    // TODO: test columnar memtable
    #[test]
    fn test_table_version_sampling_freeze() {
        let version = new_table_version();

        let memtable = MemTableMocker::default().build();
        let schema = memtable.schema().clone();

        let memtable_id1 = 1;
        let last_sequence = 1000;
        let sampling_mem = SamplingMemTable::new(memtable, memtable_id1);

        version.set_sampling(sampling_mem);
        assert_eq!(
            table_options::DEFAULT_SEGMENT_DURATION,
            version.suggest_duration().unwrap()
        );
        assert!(version.switch_memtables().is_none());
        // Freeze the sampling memtable.
        version.freeze_sampling_memtable();

        // No memtable after switch and freezed.
        let now = Timestamp::now();
        assert!(version
            .memtable_for_write(now, schema.version())
            .unwrap()
            .is_none());

        // Still flushable after freezed.
        let flushable_mems = version.pick_memtables_to_flush(last_sequence);
        assert!(flushable_mems.sampling_mem.unwrap().freezed);

        let time_range =
            TimeRange::bucket_of(now, table_options::DEFAULT_SEGMENT_DURATION).unwrap();

        // Sampling memtable still readable after freezed.
        let read_view = version.pick_read_view(time_range);
        assert!(read_view.contains_sampling());
        assert_eq!(memtable_id1, read_view.sampling_mem.as_ref().unwrap().id);
        assert!(read_view.sampling_mem.unwrap().freezed);

        let memtable = MemTableMocker::default().build();
        let memtable_id2 = 2;
        let mem_state = MemTableState {
            mem: memtable,
            time_range,
            id: memtable_id2,
        };
        // Insert a mutable memtable.
        version.insert_mutable(mem_state);

        // Write to mutable memtable.
        let mutable = version
            .memtable_for_write(now, schema.version())
            .unwrap()
            .unwrap();
        let mutable = mutable.as_normal();
        assert_eq!(time_range, mutable.time_range);
        assert_eq!(memtable_id2, mutable.id);

        // Need to read sampling memtable and mutable memtable.
        let read_view = version.pick_read_view(time_range);
        assert_eq!(memtable_id1, read_view.sampling_mem.as_ref().unwrap().id);
        assert_eq!(1, read_view.memtables.len());
        assert_eq!(memtable_id2, read_view.memtables[0].id);

        // Switch mutable memtable.
        assert!(version.suggest_duration().is_none());
        assert!(version.switch_memtables().is_some());
        // No memtable after switch.
        let now = Timestamp::now();
        assert!(version
            .memtable_for_write(now, schema.version())
            .unwrap()
            .is_none());

        // Two memtables to flush.
        let flushable_mems = version.pick_memtables_to_flush(last_sequence);
        assert!(flushable_mems.sampling_mem.unwrap().freezed);
        assert_eq!(1, flushable_mems.memtables.len());
        assert_eq!(memtable_id2, flushable_mems.memtables[0].id);
    }

    // TODO: test columnar memtable
    #[test]
    fn test_table_version_sampling_apply_edit() {
        let version = new_table_version();

        let memtable = MemTableMocker::default().build();

        let memtable_id1 = 1;
        let sampling_mem = SamplingMemTable::new(memtable, memtable_id1);

        // Prepare sampling memtable.
        version.set_sampling(sampling_mem);
        version.freeze_sampling_memtable();

        let now = Timestamp::now();
        let time_range =
            TimeRange::bucket_of(now, table_options::DEFAULT_SEGMENT_DURATION).unwrap();

        // Prepare mutable memtable.
        let memtable = MemTableMocker::default().build();
        let memtable_id2 = 2;
        let mem_state = MemTableState {
            mem: memtable,
            time_range,
            id: memtable_id2,
        };
        // Insert a mutable memtable.
        version.insert_mutable(mem_state);

        // Switch memtable.
        assert!(version.suggest_duration().is_none());
        assert!(version.switch_memtables().is_some());
        let max_sequence = 120;
        let file_id = 13;
        let add_file = AddFileMocker::new(file_id)
            .time_range(time_range)
            .max_seq(max_sequence)
            .build();
        let edit = VersionEdit {
            flushed_sequence: max_sequence,
            mems_to_remove: vec![memtable_id1, memtable_id2],
            files_to_add: vec![add_file],
            files_to_delete: vec![],
            max_file_id: 0,
        };
        version.apply_edit(edit);

        // Only pick ssts after flushed.
        let read_view = version.pick_read_view(time_range);
        assert!(!read_view.contains_sampling());
        assert!(read_view.sampling_mem.is_none());
        assert!(read_view.memtables.is_empty());
        assert_eq!(1, read_view.leveled_ssts[0].len());
        assert_eq!(file_id, read_view.leveled_ssts[0][0].id());
    }
}
