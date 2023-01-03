// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Version edits

use std::convert::{TryFrom, TryInto};

use common_types::{bytes::Bytes, schema::Schema, time::TimeRange, SequenceNumber};
use common_util::define_result;
use proto::{analytic_common as analytic_common_pb, common as common_pb, meta_update as meta_pb};
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};

use crate::{
    sst::{
        file::{FileMeta, SstMetaData},
        manager::FileId,
    },
    table::data::MemTableId,
    table_options::StorageFormatOptions,
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

    #[snafu(display("Time range is not found.\nBacktrace:\n{}", backtrace))]
    TimeRangeNotFound { backtrace: Backtrace },

    #[snafu(display("Table schema is not found.\nBacktrace:\n{}", backtrace))]
    TableSchemaNotFound { backtrace: Backtrace },
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

impl From<AddFile> for meta_pb::AddFileMeta {
    /// Convert into protobuf struct
    fn from(v: AddFile) -> meta_pb::AddFileMeta {
        meta_pb::AddFileMeta {
            level: v.level as u32,
            file_id: v.file.id,
            min_key: v.file.meta.min_key.to_vec(),
            max_key: v.file.meta.max_key.to_vec(),
            time_range: Some(v.file.meta.time_range.into()),
            max_seq: v.file.meta.max_sequence,
            schema: Some(common_pb::TableSchema::from(&v.file.meta.schema)),
            size: v.file.size,
            row_num: v.file.row_num,
            storage_format: analytic_common_pb::StorageFormat::from(v.file.meta.storage_format())
                as i32,
        }
    }
}

impl TryFrom<meta_pb::AddFileMeta> for AddFile {
    type Error = Error;

    fn try_from(src: meta_pb::AddFileMeta) -> Result<Self> {
        let storage_format = src.storage_format();
        let time_range = {
            let time_range = src.time_range.context(TimeRangeNotFound)?;
            TimeRange::try_from(time_range).context(ConvertTimeRange)?
        };
        let schema = {
            let schema = src.schema.context(TableSchemaNotFound)?;
            Schema::try_from(schema).context(ConvertTableSchema)?
        };

        let target = Self {
            level: src
                .level
                .try_into()
                .context(InvalidLevel { level: src.level })?,
            file: FileMeta {
                id: src.file_id,
                size: src.size,
                row_num: src.row_num,
                meta: SstMetaData {
                    min_key: Bytes::from(src.min_key),
                    max_key: Bytes::from(src.max_key),
                    time_range,
                    max_sequence: src.max_seq,
                    schema,
                    storage_format_opts: StorageFormatOptions::new(storage_format.into()),
                    bloom_filter: Default::default(),
                },
            },
        };

        Ok(target)
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

impl From<DeleteFile> for meta_pb::DeleteFileMeta {
    fn from(v: DeleteFile) -> Self {
        meta_pb::DeleteFileMeta {
            level: v.level as u32,
            file_id: v.file_id,
        }
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
                    size: 0,
                    row_num: 0,
                    meta: self.sst_meta.clone(),
                },
            }
        }
    }
}
