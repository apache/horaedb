// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Update to meta

use std::convert::TryFrom;

use bytes::{Buf, BufMut};
use common_types::{
    schema::{Schema, Version},
    table::Location,
    SequenceNumber,
};
use common_util::define_result;
use prost::Message;
use proto::{analytic_common, common as common_pb, meta_update as meta_pb};
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use table_engine::table::TableId;
use wal::log_batch::{Payload, PayloadDecoder};

use crate::{
    space::SpaceId,
    table::version_edit::{AddFile, DeleteFile, VersionEdit},
    TableOptions,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to encode payload, err:{}.\nBacktrace:\n{}", source, backtrace))]
    EncodePayloadPb {
        source: prost::EncodeError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to convert schema, err:{}", source))]
    ConvertSchema { source: common_types::schema::Error },

    #[snafu(display("Empty table schema.\nBacktrace:\n{}", backtrace))]
    EmptyTableSchema { backtrace: Backtrace },

    #[snafu(display("Empty table options.\nBacktrace:\n{}", backtrace))]
    EmptyTableOptions { backtrace: Backtrace },

    #[snafu(display("Empty log entry of meta update.\nBacktrace:\n{}", backtrace))]
    EmptyMetaUpdateLogEntry { backtrace: Backtrace },

    #[snafu(display("Empty meta update.\nBacktrace:\n{}", backtrace))]
    EmptyMetaUpdate { backtrace: Backtrace },

    #[snafu(display("Failed to decode payload, err:{}.\nBacktrace:\n{}", source, backtrace))]
    DecodePayloadPb {
        source: prost::DecodeError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to convert version edit, err:{}", source))]
    ConvertVersionEdit {
        source: crate::table::version_edit::Error,
    },
}

define_result!(Error);

/// Wrapper for the meta update written into the Wal.
#[derive(Debug, Clone, PartialEq)]
pub enum MetaUpdateLogEntry {
    Normal(MetaUpdate),
    Snapshot {
        sequence: SequenceNumber,
        meta_update: MetaUpdate,
    },
    SnapshotStart(SequenceNumber),
    SnapshotEnd(SequenceNumber),
}

impl From<MetaUpdateLogEntry> for meta_pb::MetaUpdateLogEntry {
    fn from(v: MetaUpdateLogEntry) -> Self {
        let entry = match v {
            MetaUpdateLogEntry::Normal(v) => {
                meta_pb::meta_update_log_entry::Entry::Normal(v.into())
            }
            MetaUpdateLogEntry::Snapshot {
                sequence,
                meta_update,
            } => {
                let snapshot_log_entry = meta_pb::SnapshotLogEntry {
                    sequence,
                    meta_update: Some(meta_update.into()),
                };
                meta_pb::meta_update_log_entry::Entry::Snapshot(snapshot_log_entry)
            }
            MetaUpdateLogEntry::SnapshotStart(sequence) => {
                meta_pb::meta_update_log_entry::Entry::SnapshotStart(
                    meta_pb::SnapshotFlagLogEntry { sequence },
                )
            }
            MetaUpdateLogEntry::SnapshotEnd(sequence) => {
                meta_pb::meta_update_log_entry::Entry::SnapshotEnd(meta_pb::SnapshotFlagLogEntry {
                    sequence,
                })
            }
        };

        meta_pb::MetaUpdateLogEntry { entry: Some(entry) }
    }
}

impl TryFrom<meta_pb::MetaUpdateLogEntry> for MetaUpdateLogEntry {
    type Error = Error;

    fn try_from(src: meta_pb::MetaUpdateLogEntry) -> Result<Self> {
        let entry = match src.entry.context(EmptyMetaUpdateLogEntry)? {
            meta_pb::meta_update_log_entry::Entry::Normal(v) => {
                MetaUpdateLogEntry::Normal(MetaUpdate::try_from(v)?)
            }
            meta_pb::meta_update_log_entry::Entry::Snapshot(v) => MetaUpdateLogEntry::Snapshot {
                sequence: v.sequence,
                meta_update: MetaUpdate::try_from(v.meta_update.context(EmptyMetaUpdate)?)?,
            },
            meta_pb::meta_update_log_entry::Entry::SnapshotStart(v) => {
                MetaUpdateLogEntry::SnapshotStart(v.sequence)
            }
            meta_pb::meta_update_log_entry::Entry::SnapshotEnd(v) => {
                MetaUpdateLogEntry::SnapshotEnd(v.sequence)
            }
        };

        Ok(entry)
    }
}

/// Modifications to meta data in meta
#[derive(Debug, Clone, PartialEq)]
pub enum MetaUpdate {
    AddTable(AddTableMeta),
    DropTable(DropTableMeta),
    VersionEdit(VersionEditMeta),
    AlterSchema(AlterSchemaMeta),
    AlterOptions(AlterOptionsMeta),
}

impl From<MetaUpdate> for meta_pb::MetaUpdate {
    fn from(update: MetaUpdate) -> Self {
        let meta = match update {
            MetaUpdate::AddTable(v) => meta_pb::meta_update::Meta::AddTable(v.into()),
            MetaUpdate::VersionEdit(v) => meta_pb::meta_update::Meta::VersionEdit(v.into()),
            MetaUpdate::AlterSchema(v) => meta_pb::meta_update::Meta::AlterSchema(v.into()),
            MetaUpdate::AlterOptions(v) => meta_pb::meta_update::Meta::AlterOptions(v.into()),
            MetaUpdate::DropTable(v) => meta_pb::meta_update::Meta::DropTable(v.into()),
        };

        meta_pb::MetaUpdate { meta: Some(meta) }
    }
}

impl MetaUpdate {
    pub fn table_id(&self) -> TableId {
        match self {
            MetaUpdate::AddTable(v) => v.table_id,
            MetaUpdate::VersionEdit(v) => v.table_id,
            MetaUpdate::AlterSchema(v) => v.table_id,
            MetaUpdate::AlterOptions(v) => v.table_id,
            MetaUpdate::DropTable(v) => v.table_id,
        }
    }
}

impl TryFrom<meta_pb::MetaUpdate> for MetaUpdate {
    type Error = Error;

    fn try_from(src: meta_pb::MetaUpdate) -> Result<Self> {
        let meta_update = match src.meta.context(EmptyMetaUpdate)? {
            meta_pb::meta_update::Meta::AddTable(v) => {
                let add_table = AddTableMeta::try_from(v)?;
                MetaUpdate::AddTable(add_table)
            }
            meta_pb::meta_update::Meta::VersionEdit(v) => {
                let version_edit = VersionEditMeta::try_from(v)?;
                MetaUpdate::VersionEdit(version_edit)
            }
            meta_pb::meta_update::Meta::AlterSchema(v) => {
                let alter_schema = AlterSchemaMeta::try_from(v)?;
                MetaUpdate::AlterSchema(alter_schema)
            }
            meta_pb::meta_update::Meta::AlterOptions(v) => {
                let alter_options = AlterOptionsMeta::try_from(v)?;
                MetaUpdate::AlterOptions(alter_options)
            }
            meta_pb::meta_update::Meta::DropTable(v) => {
                let drop_table = DropTableMeta::from(v);
                MetaUpdate::DropTable(drop_table)
            }
        };

        Ok(meta_update)
    }
}

/// Meta data for a new table
#[derive(Debug, Clone, PartialEq)]
pub struct AddTableMeta {
    /// Space id of the table
    pub space_id: SpaceId,
    pub table_id: TableId,
    pub table_name: String,
    /// Schema of the table
    pub schema: Schema,
    // Options needed to persist
    pub opts: TableOptions,
}

impl From<AddTableMeta> for meta_pb::AddTableMeta {
    fn from(v: AddTableMeta) -> Self {
        meta_pb::AddTableMeta {
            space_id: v.space_id,
            table_id: v.table_id.as_u64(),
            table_name: v.table_name,
            schema: Some(common_pb::TableSchema::from(&v.schema)),
            options: Some(analytic_common::TableOptions::from(v.opts)),
        }
    }
}

impl TryFrom<meta_pb::AddTableMeta> for AddTableMeta {
    type Error = Error;

    fn try_from(src: meta_pb::AddTableMeta) -> Result<Self> {
        let table_schema = src.schema.context(EmptyTableSchema)?;
        let opts = src.options.context(EmptyTableOptions)?;
a
        Ok(Self {
            space_id: src.space_id,
            table_id: TableId::from(src.table_id),
            table_name: src.table_name,
            schema: Schema::try_from(table_schema).context(ConvertSchema)?,
            opts: TableOptions::from(opts),
        })
    }
}

/// Meta data for dropping a table
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropTableMeta {
    /// Space id of the table
    pub space_id: SpaceId,
    pub table_id: TableId,
    pub table_name: String,
}

impl From<DropTableMeta> for meta_pb::DropTableMeta {
    fn from(v: DropTableMeta) -> Self {
        meta_pb::DropTableMeta {
            space_id: v.space_id,
            table_id: v.table_id.as_u64(),
            table_name: v.table_name,
        }
    }
}

impl From<meta_pb::DropTableMeta> for DropTableMeta {
    fn from(src: meta_pb::DropTableMeta) -> Self {
        Self {
            space_id: src.space_id,
            table_id: TableId::from(src.table_id),
            table_name: src.table_name,
        }
    }
}

/// Meta data of version edit to table
#[derive(Debug, Clone, PartialEq)]
pub struct VersionEditMeta {
    pub space_id: SpaceId,
    pub table_id: TableId,
    /// Sequence number of the flushed data. Set to 0 if this edit is not
    /// created by a flush request.
    pub flushed_sequence: SequenceNumber,
    pub files_to_add: Vec<AddFile>,
    pub files_to_delete: Vec<DeleteFile>,
}

impl VersionEditMeta {
    /// Convert into [crate::table::version_edit::VersionEdit]. The
    /// `mems_to_remove` field is left empty.
    pub fn into_version_edit(self) -> VersionEdit {
        VersionEdit {
            mems_to_remove: Vec::new(),
            flushed_sequence: self.flushed_sequence,
            files_to_add: self.files_to_add,
            files_to_delete: self.files_to_delete,
        }
    }
}

impl From<VersionEditMeta> for meta_pb::VersionEditMeta {
    fn from(v: VersionEditMeta) -> Self {
        let files_to_add = v.files_to_add.into_iter().map(|file| file.into()).collect();
        let files_to_delete = v
            .files_to_delete
            .into_iter()
            .map(|file| file.into())
            .collect();
        meta_pb::VersionEditMeta {
            space_id: v.space_id,
            table_id: v.table_id.as_u64(),
            flushed_sequence: v.flushed_sequence,
            files_to_add,
            files_to_delete,
        }
    }
}

impl TryFrom<meta_pb::VersionEditMeta> for VersionEditMeta {
    type Error = Error;

    fn try_from(src: meta_pb::VersionEditMeta) -> Result<Self> {
        let mut files_to_add = Vec::with_capacity(src.files_to_add.len());
        for file_meta in src.files_to_add {
            files_to_add.push(AddFile::try_from(file_meta).context(ConvertVersionEdit)?);
        }

        let mut files_to_delete = Vec::with_capacity(src.files_to_delete.len());
        for file_meta in src.files_to_delete {
            files_to_delete.push(DeleteFile::try_from(file_meta).context(ConvertVersionEdit)?);
        }

        Ok(Self {
            space_id: src.space_id,
            table_id: TableId::from(src.table_id),
            flushed_sequence: src.flushed_sequence,
            files_to_add,
            files_to_delete,
        })
    }
}

/// Meta data of schema update.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterSchemaMeta {
    pub space_id: SpaceId,
    pub table_id: TableId,
    pub schema: Schema,
    pub pre_schema_version: Version,
}

impl From<AlterSchemaMeta> for meta_pb::AlterSchemaMeta {
    fn from(v: AlterSchemaMeta) -> Self {
        meta_pb::AlterSchemaMeta {
            space_id: v.space_id,
            table_id: v.table_id.as_u64(),
            schema: Some(common_pb::TableSchema::from(&v.schema)),
            pre_schema_version: v.pre_schema_version,
        }
    }
}

impl TryFrom<meta_pb::AlterSchemaMeta> for AlterSchemaMeta {
    type Error = Error;

    fn try_from(src: meta_pb::AlterSchemaMeta) -> Result<Self> {
        let table_schema = src.schema.context(EmptyTableSchema)?;

        Ok(Self {
            space_id: src.space_id,
            table_id: TableId::from(src.table_id),
            schema: Schema::try_from(table_schema).context(ConvertSchema)?,
            pre_schema_version: src.pre_schema_version,
        })
    }
}

/// Meta data of options update.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterOptionsMeta {
    pub space_id: SpaceId,
    pub table_id: TableId,
    pub options: TableOptions,
}

impl From<AlterOptionsMeta> for meta_pb::AlterOptionsMeta {
    fn from(v: AlterOptionsMeta) -> Self {
        meta_pb::AlterOptionsMeta {
            space_id: v.space_id,
            table_id: v.table_id.as_u64(),
            options: Some(analytic_common::TableOptions::from(v.options)),
        }
    }
}

impl TryFrom<meta_pb::AlterOptionsMeta> for AlterOptionsMeta {
    type Error = Error;

    fn try_from(src: meta_pb::AlterOptionsMeta) -> Result<Self> {
        let table_options = src.options.context(EmptyTableOptions)?;

        Ok(Self {
            space_id: src.space_id,
            table_id: TableId::from(src.table_id),
            options: TableOptions::from(table_options),
        })
    }
}

/// An adapter to implement [wal::log_batch::Payload] for
/// [proto::meta_update::MetaUpdate]
#[derive(Debug)]
pub struct MetaUpdatePayload(meta_pb::MetaUpdateLogEntry);

impl From<MetaUpdateLogEntry> for MetaUpdatePayload {
    fn from(src: MetaUpdateLogEntry) -> Self {
        Self(src.into())
    }
}

impl From<&MetaUpdateLogEntry> for MetaUpdatePayload {
    fn from(src: &MetaUpdateLogEntry) -> Self {
        Self::from(src.clone())
    }
}

impl Payload for MetaUpdatePayload {
    type Error = Error;

    fn encode_size(&self) -> usize {
        self.0.encoded_len()
    }

    fn encode_to<B: BufMut>(&self, buf: &mut B) -> Result<()> {
        self.0.encode(buf).context(EncodePayloadPb)
    }
}

/// Decoder to decode MetaUpdate from log entry
pub struct MetaUpdateDecoder;

impl PayloadDecoder for MetaUpdateDecoder {
    type Error = Error;
    type Target = MetaUpdateLogEntry;

    fn decode<B: Buf>(&self, buf: &mut B) -> Result<Self::Target> {
        let log_entry_pb =
            meta_pb::MetaUpdateLogEntry::decode(buf.chunk()).context(DecodePayloadPb)?;

        let log_entry = MetaUpdateLogEntry::try_from(log_entry_pb)?;

        Ok(log_entry)
    }
}

#[derive(Debug, Clone)]
pub struct MetaUpdateRequest {
    pub location: Location,
    pub meta_update: MetaUpdate,
}

impl MetaUpdateRequest {
    pub fn new(location: Location, meta_update: MetaUpdate) -> Self {
        Self {
            location,
            meta_update,
        }
    }
}
