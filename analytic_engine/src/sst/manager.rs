// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Multi-level SST management

use common_types::time::{TimeRange, Timestamp};

use crate::{
    compaction::ExpiredFiles,
    sst::file::{FileHandle, FileMeta, FilePurgeQueue, Iter, Level, LevelHandler},
};

/// Id for a sst file
pub type FileId = u64;
/// We use two level merge tree, the max level should less than u16::MAX
pub const MAX_LEVEL: usize = 2;

/// A table level manager that manages all the sst files of the table
pub struct LevelsController {
    levels: Vec<LevelHandler>,
    purge_queue: FilePurgeQueue,
}

impl Drop for LevelsController {
    fn drop(&mut self) {
        // Close the purge queue to avoid files being deleted.
        self.purge_queue.close();
    }
}

impl LevelsController {
    /// Create an empty LevelsController
    pub fn new(purge_queue: FilePurgeQueue) -> Self {
        let mut levels = Vec::with_capacity(MAX_LEVEL);
        for level in 0..MAX_LEVEL {
            levels.push(LevelHandler::new(level as Level));
        }

        Self {
            levels,
            purge_queue,
        }
    }

    /// Add sst file to level
    ///
    /// Panic: If the level is greater than the max level
    pub fn add_sst_to_level(&mut self, level: Level, file_meta: FileMeta) {
        let level_handler = &mut self.levels[usize::from(level)];
        let file = FileHandle::new(file_meta, self.purge_queue.clone());

        level_handler.insert(file);
    }

    pub fn latest_sst(&self, level: Level) -> Option<FileHandle> {
        self.levels[usize::from(level)].latest_sst()
    }

    /// Pick the ssts and collect it by `append_sst`.
    pub fn pick_ssts(
        &self,
        time_range: TimeRange,
        mut append_sst: impl FnMut(Level, &[FileHandle]),
    ) {
        for level_handler in self.levels.iter() {
            let ssts = level_handler.pick_ssts(time_range);
            append_sst(level_handler.level, &ssts);
        }
    }

    /// Remove sst files from level.
    ///
    /// Panic: If the level is greater than the max level
    pub fn remove_ssts_from_level(&mut self, level: Level, file_ids: &[FileId]) {
        let level_handler = &mut self.levels[usize::from(level)];
        level_handler.remove_ssts(file_ids);
    }

    /// Total number of levels.
    pub fn num_levels(&self) -> Level {
        self.levels.len() as Level
    }

    /// Iter ssts at given `level`.
    ///
    /// Panic if level is out of bound.
    pub fn iter_ssts_at_level(&self, level: Level) -> Iter {
        let level_handler = &self.levels[usize::from(level)];
        level_handler.iter_ssts()
    }

    pub fn collect_expired_at_level(
        &self,
        level: Level,
        expire_time: Option<Timestamp>,
    ) -> Vec<FileHandle> {
        let level_handler = &self.levels[usize::from(level)];
        let mut expired = Vec::new();
        level_handler.collect_expired(expire_time, &mut expired);

        expired
    }

    pub fn has_expired_sst(&self, expire_time: Option<Timestamp>) -> bool {
        self.levels
            .iter()
            .any(|level_handler| level_handler.has_expired_sst(expire_time))
    }

    pub fn expired_ssts(&self, expire_time: Option<Timestamp>) -> Vec<ExpiredFiles> {
        let mut expired = Vec::new();
        let num_levels = self.num_levels();
        for level in 0..num_levels {
            let files = self.collect_expired_at_level(level, expire_time);
            expired.push(ExpiredFiles { level, files });
        }

        expired
    }
}

#[cfg(test)]
pub mod tests {
    use table_engine::table::TableId;
    use tokio::sync::mpsc;

    use crate::{
        sst::{
            file::{FileMeta, FilePurgeQueue},
            manager::{FileId, LevelsController},
            meta_data::SstMetaData,
        },
        table_options::StorageFormat,
    };

    #[must_use]
    #[derive(Default)]
    pub struct LevelsControllerMockBuilder {
        sst_meta_vec: Vec<SstMetaData>,
    }

    impl LevelsControllerMockBuilder {
        pub fn add_sst(mut self, mut sst_meta: Vec<SstMetaData>) -> Self {
            self.sst_meta_vec.append(&mut sst_meta);
            self
        }

        pub fn build(self) -> LevelsController {
            let (tx, _rx) = mpsc::unbounded_channel();
            let file_purge_queue = FilePurgeQueue::new(100, TableId::from(101), tx);
            let mut levels_controller = LevelsController::new(file_purge_queue);
            for (id, sst_meta) in self.sst_meta_vec.into_iter().enumerate() {
                levels_controller.add_sst_to_level(
                    0,
                    FileMeta {
                        id: id as FileId,
                        size: 0,
                        row_num: 0,
                        time_range: sst_meta.time_range(),
                        max_seq: sst_meta.max_sequence(),
                        storage_format: StorageFormat::Columnar,
                    },
                );
            }
            levels_controller
        }
    }
}
