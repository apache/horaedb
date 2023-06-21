// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Update to meta

use std::convert::TryFrom;

use bytes::{Buf, BufMut};
use ceresdbproto::{manifest as manifest_pb, schema as schema_pb};
use common_types::{
    schema::{Schema, Version},
    SequenceNumber,
};
use common_util::define_result;
use prost::Message;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use table_engine::table::TableId;
use wal::log_batch::{Payload, PayloadDecoder};

use crate::{
    manifest::meta_snapshot::MetaSnapshot,
    space::SpaceId,
    table::{
        data::{MemTableId, TableShardInfo},
        version::TableVersionMeta,
        version_edit::{AddFile, DeleteFile, VersionEdit},
    },
    table_options, TableOptions,
};
use crate::sst::manager::FileId;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to encode payload, err:{}.\nBacktrace:\n{}", source, backtrace))]
    EncodePayloadPb {
        source: prost::EncodeError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to decode payload, err:{}.\nBacktrace:\n{}", source, backtrace))]
    DecodePayloadPb {
        source: prost::DecodeError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to convert schema, err:{}", source))]
    ConvertSchema { source: common_types::schema::Error },

    #[snafu(display("Empty table schema.\nBacktrace:\n{}", backtrace))]
    EmptyTableSchema { backtrace: Backtrace },

    #[snafu(display("Empty table options.\nBacktrace:\n{}", backtrace))]
    EmptyTableOptions { backtrace: Backtrace },

    #[snafu(display("Failed to convert table options, err:{}", source))]
    ConvertTableOptions { source: table_options::Error },

    #[snafu(display("Empty meta update.\nBacktrace:\n{}", backtrace))]
    EmptyMetaUpdate { backtrace: Backtrace },

    #[snafu(display("Failed to convert version edit, err:{}", source))]
    ConvertVersionEdit {
        source: crate::table::version_edit::Error,
    },

    #[snafu(display("Failed to convert meta edit, msg:{}.\nBacktrace:\n{}", msg, backtrace))]
    ConvertMetaEdit { msg: String, backtrace: Backtrace },
}

define_result!(Error);

/// Modifications to meta data in meta
#[derive(Debug, Clone)]
pub enum MetaUpdate {
    AddTable(AddTableMeta),
    DropTable(DropTableMeta),
    VersionEdit(VersionEditMeta),
    AlterSchema(AlterSchemaMeta),
    AlterOptions(AlterOptionsMeta),
    AlterSstId(AlterSstIdMeta),
}

impl From<MetaUpdate> for manifest_pb::MetaUpdate {
    fn from(update: MetaUpdate) -> Self {
        let meta = match update {
            MetaUpdate::AddTable(v) => manifest_pb::meta_update::Meta::AddTable(v.into()),
            MetaUpdate::VersionEdit(v) => manifest_pb::meta_update::Meta::VersionEdit(v.into()),
            MetaUpdate::AlterSchema(v) => manifest_pb::meta_update::Meta::AlterSchema(v.into()),
            MetaUpdate::AlterOptions(v) => manifest_pb::meta_update::Meta::AlterOptions(v.into()),
            MetaUpdate::DropTable(v) => manifest_pb::meta_update::Meta::DropTable(v.into()),
            MetaUpdate::AlterSstId(v) => manifest_pb::meta_update::Meta::AlterSstId(v.into()),
        };

        manifest_pb::MetaUpdate { meta: Some(meta) }
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
            MetaUpdate::AlterSstId(v) => v.table_id,
        }
    }

    pub fn space_id(&self) -> SpaceId {
        match self {
            MetaUpdate::AddTable(v) => v.space_id,
            MetaUpdate::VersionEdit(v) => v.space_id,
            MetaUpdate::AlterSchema(v) => v.space_id,
            MetaUpdate::AlterOptions(v) => v.space_id,
            MetaUpdate::DropTable(v) => v.space_id,
            MetaUpdate::AlterSstId(v) => v.space_id,
        }
    }
}

impl TryFrom<manifest_pb::MetaUpdate> for MetaUpdate {
    type Error = Error;

    fn try_from(src: manifest_pb::MetaUpdate) -> Result<Self> {
        let meta_update = match src.meta.context(EmptyMetaUpdate)? {
            manifest_pb::meta_update::Meta::AddTable(v) => {
                let add_table = AddTableMeta::try_from(v)?;
                MetaUpdate::AddTable(add_table)
            }
            manifest_pb::meta_update::Meta::VersionEdit(v) => {
                let version_edit = VersionEditMeta::try_from(v)?;
                MetaUpdate::VersionEdit(version_edit)
            }
            manifest_pb::meta_update::Meta::AlterSchema(v) => {
                let alter_schema = AlterSchemaMeta::try_from(v)?;
                MetaUpdate::AlterSchema(alter_schema)
            }
            manifest_pb::meta_update::Meta::AlterOptions(v) => {
                let alter_options = AlterOptionsMeta::try_from(v)?;
                MetaUpdate::AlterOptions(alter_options)
            }
            manifest_pb::meta_update::Meta::DropTable(v) => {
                let drop_table = DropTableMeta::from(v);
                MetaUpdate::DropTable(drop_table)
            }
            manifest_pb::meta_update::Meta::AlterSstId(v) => {
                let alter_sst_id = AlterSstIdMeta::try_from(v)?;
                MetaUpdate::AlterSstId(alter_sst_id)
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

impl From<AddTableMeta> for manifest_pb::AddTableMeta {
    fn from(v: AddTableMeta) -> Self {
        manifest_pb::AddTableMeta {
            space_id: v.space_id,
            table_id: v.table_id.as_u64(),
            table_name: v.table_name,
            schema: Some(schema_pb::TableSchema::from(&v.schema)),
            options: Some(manifest_pb::TableOptions::from(v.opts)),
            // Deprecated.
            partition_info: None,
        }
    }
}

impl TryFrom<manifest_pb::AddTableMeta> for AddTableMeta {
    type Error = Error;

    fn try_from(src: manifest_pb::AddTableMeta) -> Result<Self> {
        let table_schema = src.schema.context(EmptyTableSchema)?;
        let opts = src.options.context(EmptyTableOptions)?;

        Ok(Self {
            space_id: src.space_id,
            table_id: TableId::from(src.table_id),
            table_name: src.table_name,
            schema: Schema::try_from(table_schema).context(ConvertSchema)?,
            opts: TableOptions::try_from(opts).context(ConvertTableOptions)?,
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

impl From<DropTableMeta> for manifest_pb::DropTableMeta {
    fn from(v: DropTableMeta) -> Self {
        manifest_pb::DropTableMeta {
            space_id: v.space_id,
            table_id: v.table_id.as_u64(),
            table_name: v.table_name,
        }
    }
}

impl From<manifest_pb::DropTableMeta> for DropTableMeta {
    fn from(src: manifest_pb::DropTableMeta) -> Self {
        Self {
            space_id: src.space_id,
            table_id: TableId::from(src.table_id),
            table_name: src.table_name,
        }
    }
}

/// Meta data of version edit to table
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionEditMeta {
    pub space_id: SpaceId,
    pub table_id: TableId,
    /// Sequence number of the flushed data. Set to 0 if this edit is not
    /// created by a flush request.
    pub flushed_sequence: SequenceNumber,
    pub files_to_add: Vec<AddFile>,
    pub files_to_delete: Vec<DeleteFile>,
    /// Id of memtables to remove from immutable memtable lists.
    /// No need to persist.
    pub mems_to_remove: Vec<MemTableId>,
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

impl From<VersionEditMeta> for manifest_pb::VersionEditMeta {
    fn from(v: VersionEditMeta) -> Self {
        let files_to_add = v.files_to_add.into_iter().map(|file| file.into()).collect();
        let files_to_delete = v
            .files_to_delete
            .into_iter()
            .map(|file| file.into())
            .collect();
        manifest_pb::VersionEditMeta {
            space_id: v.space_id,
            table_id: v.table_id.as_u64(),
            flushed_sequence: v.flushed_sequence,
            files_to_add,
            files_to_delete,
        }
    }
}

impl TryFrom<manifest_pb::VersionEditMeta> for VersionEditMeta {
    type Error = Error;

    fn try_from(src: manifest_pb::VersionEditMeta) -> Result<Self> {
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
            mems_to_remove: Vec::default(),
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

impl From<AlterSchemaMeta> for manifest_pb::AlterSchemaMeta {
    fn from(v: AlterSchemaMeta) -> Self {
        manifest_pb::AlterSchemaMeta {
            space_id: v.space_id,
            table_id: v.table_id.as_u64(),
            schema: Some(schema_pb::TableSchema::from(&v.schema)),
            pre_schema_version: v.pre_schema_version,
        }
    }
}

impl TryFrom<manifest_pb::AlterSchemaMeta> for AlterSchemaMeta {
    type Error = Error;

    fn try_from(src: manifest_pb::AlterSchemaMeta) -> Result<Self> {
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

impl From<AlterOptionsMeta> for manifest_pb::AlterOptionsMeta {
    fn from(v: AlterOptionsMeta) -> Self {
        manifest_pb::AlterOptionsMeta {
            space_id: v.space_id,
            table_id: v.table_id.as_u64(),
            options: Some(manifest_pb::TableOptions::from(v.options)),
        }
    }
}

impl TryFrom<manifest_pb::AlterOptionsMeta> for AlterOptionsMeta {
    type Error = Error;

    fn try_from(src: manifest_pb::AlterOptionsMeta) -> Result<Self> {
        let table_options = src.options.context(EmptyTableOptions)?;

        Ok(Self {
            space_id: src.space_id,
            table_id: TableId::from(src.table_id),
            options: TableOptions::try_from(table_options).context(ConvertTableOptions)?,
        })
    }
}

/// Meta data of sst id update.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterSstIdMeta {
    pub space_id: SpaceId,
    pub table_id: TableId,
    pub last_file_id: FileId,
}

impl From<AlterSstIdMeta> for manifest_pb::AlterSstIdMeta {
    fn from(v: AlterSstIdMeta) -> Self {
        manifest_pb::AlterSstIdMeta {
            space_id: v.space_id,
            table_id: v.table_id.as_u64(),
            last_file_id: v.last_file_id,
        }
    }
}

impl TryFrom<manifest_pb::AlterSstIdMeta> for AlterSstIdMeta {
    type Error = Error;

    fn try_from(src: manifest_pb::AlterSstIdMeta) -> Result<Self> {
        Ok(Self {
            space_id: src.space_id,
            table_id: TableId::from(src.table_id),
            last_file_id: src.last_file_id,
        })
    }
}

/// An adapter to implement [wal::log_batch::Payload] for
/// [proto::meta_update::MetaUpdate]
#[derive(Debug)]
pub struct MetaUpdatePayload(manifest_pb::MetaUpdate);

impl From<MetaUpdate> for MetaUpdatePayload {
    fn from(src: MetaUpdate) -> Self {
        Self(src.into())
    }
}

impl From<&MetaUpdate> for MetaUpdatePayload {
    fn from(src: &MetaUpdate) -> Self {
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
    type Target = MetaUpdate;

    fn decode<B: Buf>(&self, buf: &mut B) -> Result<Self::Target> {
        let meta_update_pb =
            manifest_pb::MetaUpdate::decode(buf.chunk()).context(DecodePayloadPb)?;
        MetaUpdate::try_from(meta_update_pb)
    }
}

/// The snapshot for the current logs.
#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    /// The end sequence of the logs that this snapshot covers.
    /// Basically it is the latest sequence number of the logs when creating a
    /// new snapshot.
    pub end_seq: SequenceNumber,
    /// The data of the snapshot.
    /// None means the table not exists(maybe dropped or not created yet).
    pub data: Option<MetaSnapshot>,
}

impl TryFrom<manifest_pb::Snapshot> for Snapshot {
    type Error = Error;

    fn try_from(src: manifest_pb::Snapshot) -> Result<Self> {
        let meta = src.meta.map(AddTableMeta::try_from).transpose()?;

        let version_edit = src
            .version_edit
            .map(VersionEditMeta::try_from)
            .transpose()?;

        let version_meta = version_edit.map(|v| {
            let mut version_meta = TableVersionMeta::default();
            version_meta.apply_edit(v.into_version_edit());
            version_meta
        });

        let table_manifest_data = meta.map(|v| MetaSnapshot {
            table_meta: v,
            version_meta,
        });
        Ok(Self {
            end_seq: src.end_seq,
            data: table_manifest_data,
        })
    }
}

impl From<Snapshot> for manifest_pb::Snapshot {
    fn from(src: Snapshot) -> Self {
        if let Some((meta, version_edit)) = src.data.map(|v| {
            let space_id = v.table_meta.space_id;
            let table_id = v.table_meta.table_id;
            let table_meta = manifest_pb::AddTableMeta::from(v.table_meta);
            let version_edit = v.version_meta.map(|version_meta| VersionEditMeta {
                space_id,
                table_id,
                flushed_sequence: version_meta.flushed_sequence,
                files_to_add: version_meta.ordered_files(),
                files_to_delete: vec![],
                mems_to_remove: vec![],
            });
            (
                table_meta,
                version_edit.map(manifest_pb::VersionEditMeta::from),
            )
        }) {
            Self {
                end_seq: src.end_seq,
                meta: Some(meta),
                version_edit,
            }
        } else {
            Self {
                end_seq: src.end_seq,
                meta: None,
                version_edit: None,
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum MetaEdit {
    Update(MetaUpdate),
    Snapshot(MetaSnapshot),
}

impl TryFrom<MetaEdit> for MetaUpdate {
    type Error = Error;

    fn try_from(value: MetaEdit) -> std::result::Result<Self, Self::Error> {
        if let MetaEdit::Update(update) = value {
            Ok(update)
        } else {
            ConvertMetaEdit {
                msg: "it is not the update type meta edit",
            }
            .fail()
        }
    }
}

impl TryFrom<MetaEdit> for MetaSnapshot {
    type Error = Error;

    fn try_from(value: MetaEdit) -> std::result::Result<Self, Self::Error> {
        if let MetaEdit::Snapshot(table_manifest_data) = value {
            Ok(table_manifest_data)
        } else {
            ConvertMetaEdit {
                msg: "it is not the snapshot type meta edit",
            }
            .fail()
        }
    }
}

#[derive(Debug, Clone)]
pub struct MetaEditRequest {
    pub shard_info: TableShardInfo,
    pub meta_edit: MetaEdit,
}
