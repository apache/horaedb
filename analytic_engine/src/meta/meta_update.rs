// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Update to meta

use std::convert::{TryFrom, TryInto};

use common_types::{
    bytes::{MemBuf, MemBufMut, Writer},
    schema::{Schema, Version},
    SequenceNumber,
};
use common_util::define_result;
use proto::{analytic_common, common as common_pb, meta_update as meta_pb};
use protobuf::Message;
use snafu::{Backtrace, ResultExt, Snafu};
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
        source: protobuf::error::ProtobufError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to convert schema, err:{}", source))]
    ConvertSchema { source: common_types::schema::Error },

    #[snafu(display("Empty log entry of meta update.\nBacktrace:\n{}", backtrace))]
    EmptyMetaUpdateLogEntry { backtrace: Backtrace },

    #[snafu(display("Empty meta update.\nBacktrace:\n{}", backtrace))]
    EmptyMetaUpdate { backtrace: Backtrace },

    #[snafu(display("Failed to decode payload, err:{}.\nBacktrace:\n{}", source, backtrace))]
    DecodePayloadPb {
        source: protobuf::error::ProtobufError,
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

impl MetaUpdateLogEntry {
    pub fn into_pb(self) -> meta_pb::MetaUpdateLogEntry {
        let mut log_entry = meta_pb::MetaUpdateLogEntry::new();

        match self {
            MetaUpdateLogEntry::Normal(v) => {
                log_entry.set_normal(v.into_pb());
            }
            MetaUpdateLogEntry::Snapshot {
                sequence,
                meta_update,
            } => {
                let mut snapshot_entry = meta_pb::SnapshotLogEntry::new();
                snapshot_entry.set_sequence(sequence);
                snapshot_entry.set_meta_update(meta_update.into_pb());
                log_entry.set_snapshot(snapshot_entry);
            }
            MetaUpdateLogEntry::SnapshotStart(v) => {
                let mut snapshot_flag_entry = meta_pb::SnapshotFlagLogEntry::new();
                snapshot_flag_entry.set_sequence(v);
                log_entry.set_snapshot_start(snapshot_flag_entry);
            }
            MetaUpdateLogEntry::SnapshotEnd(v) => {
                let mut snapshot_flag_entry = meta_pb::SnapshotFlagLogEntry::new();
                snapshot_flag_entry.set_sequence(v);
                log_entry.set_snapshot_end(snapshot_flag_entry);
            }
        }

        log_entry
    }
}

impl TryFrom<meta_pb::MetaUpdateLogEntry> for MetaUpdateLogEntry {
    type Error = Error;

    fn try_from(src: meta_pb::MetaUpdateLogEntry) -> Result<Self> {
        let entry = match src.entry {
            Some(meta_pb::MetaUpdateLogEntry_oneof_entry::normal(v)) => {
                MetaUpdateLogEntry::Normal(MetaUpdate::try_from(v)?)
            }
            Some(meta_pb::MetaUpdateLogEntry_oneof_entry::snapshot(mut v)) => {
                MetaUpdateLogEntry::Snapshot {
                    sequence: v.sequence,
                    meta_update: MetaUpdate::try_from(v.take_meta_update())?,
                }
            }
            Some(meta_pb::MetaUpdateLogEntry_oneof_entry::snapshot_start(v)) => {
                MetaUpdateLogEntry::SnapshotStart(v.sequence)
            }
            Some(meta_pb::MetaUpdateLogEntry_oneof_entry::snapshot_end(v)) => {
                MetaUpdateLogEntry::SnapshotEnd(v.sequence)
            }
            None => return EmptyMetaUpdateLogEntry.fail(),
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

impl MetaUpdate {
    pub fn into_pb(self) -> meta_pb::MetaUpdate {
        let mut meta_update = meta_pb::MetaUpdate::new();

        match self {
            MetaUpdate::AddTable(v) => {
                meta_update.set_add_table(v.into_pb());
            }
            MetaUpdate::VersionEdit(v) => {
                meta_update.set_version_edit(v.into_pb());
            }
            MetaUpdate::AlterSchema(v) => {
                meta_update.set_alter_schema(v.into_pb());
            }
            MetaUpdate::AlterOptions(v) => {
                meta_update.set_alter_options(v.into_pb());
            }
            MetaUpdate::DropTable(v) => {
                meta_update.set_drop_table(v.into_pb());
            }
        }

        meta_update
    }

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
        let meta_update = match src.meta {
            Some(meta_pb::MetaUpdate_oneof_meta::add_table(v)) => {
                let add_table = AddTableMeta::try_from(v)?;
                MetaUpdate::AddTable(add_table)
            }
            Some(meta_pb::MetaUpdate_oneof_meta::version_edit(v)) => {
                let version_edit = VersionEditMeta::try_from(v)?;
                MetaUpdate::VersionEdit(version_edit)
            }
            Some(meta_pb::MetaUpdate_oneof_meta::alter_schema(v)) => {
                let alter_schema = AlterSchemaMeta::try_from(v)?;
                MetaUpdate::AlterSchema(alter_schema)
            }
            Some(meta_pb::MetaUpdate_oneof_meta::alter_options(v)) => {
                let alter_options = AlterOptionsMeta::from(v);
                MetaUpdate::AlterOptions(alter_options)
            }
            Some(meta_pb::MetaUpdate_oneof_meta::drop_table(v)) => {
                let drop_table = DropTableMeta::from(v);
                MetaUpdate::DropTable(drop_table)
            }
            None => {
                // Meta update should not be empty.
                return EmptyMetaUpdate.fail();
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

impl AddTableMeta {
    fn into_pb(self) -> meta_pb::AddTableMeta {
        let mut target = meta_pb::AddTableMeta::new();
        target.set_space_id(self.space_id);
        target.set_table_id(self.table_id.as_u64());
        target.set_table_name(self.table_name);
        target.set_schema(common_pb::TableSchema::from(self.schema));
        target.set_options(analytic_common::TableOptions::from(self.opts));

        target
    }
}

impl TryFrom<meta_pb::AddTableMeta> for AddTableMeta {
    type Error = Error;

    fn try_from(mut src: meta_pb::AddTableMeta) -> Result<Self> {
        let table_schema = src.take_schema();
        let opts = src.take_options();

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
#[derive(Debug, Clone, PartialEq)]
pub struct DropTableMeta {
    /// Space id of the table
    pub space_id: SpaceId,
    pub table_id: TableId,
    pub table_name: String,
}

impl DropTableMeta {
    fn into_pb(self) -> meta_pb::DropTableMeta {
        let mut target = meta_pb::DropTableMeta::new();
        target.set_space_id(self.space_id);
        target.set_table_id(self.table_id.as_u64());
        target.set_table_name(self.table_name);

        target
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
    fn into_pb(self) -> meta_pb::VersionEditMeta {
        let mut target = meta_pb::VersionEditMeta::new();
        target.set_space_id(self.space_id);
        target.set_table_id(self.table_id.as_u64());
        target.set_flushed_sequence(self.flushed_sequence);

        let mut files_to_add = Vec::with_capacity(self.files_to_add.len());
        for file in self.files_to_add {
            files_to_add.push(file.into_pb());
        }
        target.files_to_add = files_to_add.into();

        let mut files_to_delete = Vec::with_capacity(self.files_to_delete.len());
        for file in self.files_to_delete {
            files_to_delete.push(file.into_pb());
        }
        target.files_to_delete = files_to_delete.into();

        target
    }

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

impl AlterSchemaMeta {
    fn into_pb(self) -> meta_pb::AlterSchemaMeta {
        let mut target = meta_pb::AlterSchemaMeta::new();
        target.set_space_id(self.space_id);
        target.set_table_id(self.table_id.as_u64());
        target.set_schema(common_pb::TableSchema::from(self.schema));
        target.set_pre_schema_version(self.pre_schema_version);

        target
    }
}

impl TryFrom<meta_pb::AlterSchemaMeta> for AlterSchemaMeta {
    type Error = Error;

    fn try_from(mut src: meta_pb::AlterSchemaMeta) -> Result<Self> {
        let table_schema = src.take_schema();

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

impl AlterOptionsMeta {
    fn into_pb(self) -> meta_pb::AlterOptionsMeta {
        let mut target = meta_pb::AlterOptionsMeta::new();
        target.set_space_id(self.space_id);
        target.set_table_id(self.table_id.as_u64());
        target.set_options(analytic_common::TableOptions::from(self.options));

        target
    }
}

impl From<meta_pb::AlterOptionsMeta> for AlterOptionsMeta {
    fn from(mut src: meta_pb::AlterOptionsMeta) -> Self {
        let table_options = src.take_options();

        Self {
            space_id: src.space_id,
            table_id: TableId::from(src.table_id),
            options: TableOptions::from(table_options),
        }
    }
}

/// An adapter to implement [wal::log_batch::Payload] for
/// [proto::meta_update::MetaUpdate]
#[derive(Debug)]
pub struct MetaUpdatePayload(meta_pb::MetaUpdateLogEntry);

impl From<MetaUpdateLogEntry> for MetaUpdatePayload {
    fn from(src: MetaUpdateLogEntry) -> Self {
        Self(src.into_pb())
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
        self.0.compute_size().try_into().unwrap_or(0)
    }

    fn encode_to<B: MemBufMut>(&self, buf: &mut B) -> Result<()> {
        let mut writer = Writer::new(buf);
        self.0.write_to_writer(&mut writer).context(EncodePayloadPb)
    }
}

/// Decoder to decode MetaUpdate from log entry
pub struct MetaUpdateDecoder;

impl PayloadDecoder for MetaUpdateDecoder {
    type Error = Error;
    type Target = MetaUpdateLogEntry;

    fn decode<B: MemBuf>(&self, buf: &mut B) -> Result<Self::Target> {
        let log_entry_pb = meta_pb::MetaUpdateLogEntry::parse_from_bytes(buf.remaining_slice())
            .context(DecodePayloadPb)?;

        let log_entry = MetaUpdateLogEntry::try_from(log_entry_pb)?;

        Ok(log_entry)
    }
}
