// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Version edits

use std::convert::{TryFrom, TryInto};

use common_types::{bytes::Bytes, schema::Schema, time::TimeRange, SequenceNumber};
use common_util::define_result;
use proto::meta_update as meta_pb;
use snafu::{Backtrace, ResultExt, Snafu};

use crate::{
    sst::{
        file::{FileMeta, SstMetaData},
        manager::FileId,
    },
    table::data::MemTableId,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid level:{}, err:{}.\nBacktrace:\n{}", level, source, backtrace))]
    InvalidLevel {
        level: u32,
        source: std::num::TryFromIntError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to convert time range, err:{}", source))]
    ConvertTimeRange { source: common_types::time::Error },

    #[snafu(display("Fail to convert table schema, err:{}", source))]
    ConvertTableSchema { source: common_types::schema::Error },
}

define_result!(Error);

/// Meta data of a new file.
#[derive(Debug, Clone, PartialEq)]
pub struct AddFile {
    /// The level of the file intended to add.
    pub level: u16,
    /// Meta data of the file to add.
    pub file: FileMeta,
}

impl AddFile {
    /// Convert into protobuf struct
    pub fn into_pb(self) -> meta_pb::AddFileMeta {
        let mut target = meta_pb::AddFileMeta::new();
        target.set_level(self.level.into());
        target.set_file_id(self.file.id);
        target.set_min_key(self.file.meta.min_key.to_vec());
        target.set_max_key(self.file.meta.max_key.to_vec());
        target.set_time_range(self.file.meta.time_range.into());
        target.set_max_seq(self.file.meta.max_sequence);
        target.set_schema(self.file.meta.schema.into());
        target.set_size(self.file.meta.size);
        target.set_row_num(self.file.meta.row_num);

        target
    }
}

impl TryFrom<meta_pb::AddFileMeta> for AddFile {
    type Error = Error;

    fn try_from(mut src: meta_pb::AddFileMeta) -> Result<Self> {
        let time_range = TimeRange::try_from(src.take_time_range()).context(ConvertTimeRange)?;
        let schema = Schema::try_from(src.take_schema()).context(ConvertTableSchema)?;
        Ok(Self {
            level: src
                .level
                .try_into()
                .context(InvalidLevel { level: src.level })?,
            file: FileMeta {
                id: src.file_id,
                meta: SstMetaData {
                    min_key: Bytes::from(src.min_key),
                    max_key: Bytes::from(src.max_key),
                    time_range,
                    max_sequence: src.max_seq,
                    schema,
                    size: src.size,
                    row_num: src.row_num,
                    storage_format: src.storage_format.into(),
                },
            },
        })
    }
}

/// Meta data of the file to delete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteFile {
    /// The level of the file intended to delete.
    pub level: u16,
    /// Id of the file to delete.
    pub file_id: FileId,
}

impl DeleteFile {
    /// Convert into protobuf struct
    pub fn into_pb(self) -> meta_pb::DeleteFileMeta {
        let mut target = meta_pb::DeleteFileMeta::new();
        target.set_level(self.level.into());
        target.set_file_id(self.file_id);

        target
    }
}

impl TryFrom<meta_pb::DeleteFileMeta> for DeleteFile {
    type Error = Error;

    fn try_from(src: meta_pb::DeleteFileMeta) -> Result<Self> {
        let level = src
            .level
            .try_into()
            .context(InvalidLevel { level: src.level })?;

        Ok(Self {
            level,
            file_id: src.file_id,
        })
    }
}

/// Edit to the [TableVersion], which should be done atomically
#[derive(Debug)]
pub struct VersionEdit {
    /// The last sequence already flushed. This field is not guaranteed to be
    /// set if the version edit is created by a non-flush operation (such as
    /// compaction).
    pub flushed_sequence: SequenceNumber,
    /// Id of memtables to remove from immutable memtable lists.
    pub mems_to_remove: Vec<MemTableId>,
    /// Sst files to add.
    pub files_to_add: Vec<AddFile>,
    /// Sst files to delete.
    pub files_to_delete: Vec<DeleteFile>,
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[must_use]
    pub struct AddFileMocker {
        file_id: FileId,
        sst_meta: SstMetaData,
    }

    impl AddFileMocker {
        pub fn new(sst_meta: SstMetaData) -> Self {
            Self {
                file_id: 1,
                sst_meta,
            }
        }

        pub fn file_id(mut self, file_id: FileId) -> Self {
            self.file_id = file_id;
            self
        }

        pub fn build(&self) -> AddFile {
            AddFile {
                level: 0,
                file: FileMeta {
                    id: self.file_id,
                    meta: self.sst_meta.clone(),
                },
            }
        }
    }
}
