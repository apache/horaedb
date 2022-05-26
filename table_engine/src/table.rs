// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Table abstraction

use std::{
    collections::HashMap,
    convert::TryFrom,
    fmt,
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use common_types::{
    column_schema::ColumnSchema,
    datum::Datum,
    projected_schema::ProjectedSchema,
    request_id::RequestId,
    row::{Row, RowGroup},
    schema::{RecordSchemaWithKey, Schema, Version},
    time::Timestamp,
};
use proto::sys_catalog::{TableEntry, TableState as TableStatePb};
use serde_derive::Deserialize;
use snafu::{Backtrace, Snafu};

use crate::{
    engine::{TableRequestType, TableState},
    predicate::PredicateRef,
    stream::{PartitionedStreams, SendableRecordBatchStream},
};

/// Contains common error variant, implementation specific error should
/// be cast into Box<dyn Error>
#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(display(
        "Unsupported table method, table:{}, method:{}.\nBacktrace:\n{}",
        table,
        method,
        backtrace
    ))]
    UnsupportedMethod {
        table: String,
        method: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Get Invalid primary key, expected schema:{:?}, given_primary_keys:{:?}.\nBacktrace:\n{}",
        schema,
        primary_key_columns,
        backtrace
    ))]
    GetInvalidPrimaryKey {
        schema: RecordSchemaWithKey,
        primary_key_columns: Vec<ColumnSchema>,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Get null primary key, expected schema:{:?}, given_primary_keys:{:?}.\nBacktrace:\n{}",
        schema,
        primary_key_columns,
        backtrace
    ))]
    GetNullPrimaryKey {
        schema: RecordSchemaWithKey,
        primary_key_columns: Vec<ColumnSchema>,
        backtrace: Backtrace,
    },

    #[snafu(display("Unexpected error, err:{}", source))]
    Unexpected {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Invalid arguments, err:{}", source))]
    InvalidArguments {
        table: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to write table, table:{}, err:{}", table, source))]
    Write {
        table: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to scan table, table:{}, err:{}", table, source))]
    Scan {
        table: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to get table, table:{}, err:{}", table, source))]
    Get {
        table: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to alter schema, table:{}, err:{}", table, source))]
    AlterSchema {
        table: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to alter options, table:{}, err:{}", table, source))]
    AlterOptions {
        table: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to flush table, table:{}, err:{}", table, source))]
    Flush {
        table: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to compact table, table:{}, err:{}", table, source))]
    Compact {
        table: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

define_result!(Error);

/// Default partition num to scan in parallelism.
pub const DEFAULT_READ_PARALLELISM: usize = 8;

/// Schema id (24 bits)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaId(u32);

impl SchemaId {
    /// Bits of schema id.
    const BITS: u32 = 24;
    /// 24 bits mask (0xffffff)
    const MASK: u32 = (1 << Self::BITS) - 1;
    /// Max schema id.
    pub const MAX: SchemaId = SchemaId(Self::MASK);
    /// Min schema id.
    pub const MIN: SchemaId = SchemaId(0);

    /// Create a new schema id from u32, return None if `id` is invalid.
    pub fn new(id: u32) -> Option<Self> {
        // Only need to check max as min is 0.
        if id <= SchemaId::MAX.0 {
            Some(Self(id))
        } else {
            None
        }
    }

    // It is safe to convert u16 into schema id.
    pub const fn from_u16(id: u16) -> Self {
        Self(id as u32)
    }

    /// Convert the schema id into u32.
    #[inline]
    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

impl PartialEq<u32> for SchemaId {
    fn eq(&self, other: &u32) -> bool {
        self.0 == *other
    }
}

impl From<u16> for SchemaId {
    fn from(id: u16) -> SchemaId {
        SchemaId::from_u16(id)
    }
}

/// Sequence of a table under a schema (40 bits).
#[derive(Debug, Clone, Copy)]
pub struct TableSeq(u64);

impl TableSeq {
    /// Bits of schema id.
    const BITS: u64 = 40;
    /// 40 bits mask (0xffffffffff).
    const MASK: u64 = (1 << Self::BITS) - 1;
    /// Max sequence of table in a schema.
    pub const MAX: TableSeq = TableSeq(Self::MASK);
    /// Min sequence of table in a schema.
    pub const MIN: TableSeq = TableSeq(0);

    /// Create a new table sequence from u64, return None if `seq` is invalid.
    pub const fn new(seq: u64) -> Option<Self> {
        // Only need to check max as min is 0.
        if seq <= TableSeq::MAX.0 {
            Some(Self(seq))
        } else {
            None
        }
    }

    // It is safe to convert u32 into table seq.
    pub const fn from_u32(id: u32) -> Self {
        Self(id as u64)
    }

    /// Convert the table sequence into u64.
    #[inline]
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl From<u32> for TableSeq {
    fn from(id: u32) -> TableSeq {
        TableSeq::from_u32(id)
    }
}

/// Table Id (64 bits)
///
/// Table id is constructed via schema id (24 bits) and a table sequence (40
/// bits).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize)]
pub struct TableId(u64);

impl TableId {
    /// Min table id.
    pub const MIN: TableId = TableId(0);

    /// Create a new table id from `schema_id` and `table_seq`.
    pub const fn new(schema_id: SchemaId, table_seq: TableSeq) -> Self {
        let schema_id_data = schema_id.0 as u64;
        let schema_id_part = schema_id_data << TableSeq::BITS;
        let table_id_data = schema_id_part | table_seq.0;

        Self(table_id_data)
    }

    /// Get the schema id part of the table id.
    #[inline]
    pub fn schema_id(&self) -> SchemaId {
        let schema_id_part = self.0 >> TableSeq::BITS;

        SchemaId(schema_id_part as u32)
    }

    /// Get the sequence part of the table id.
    #[inline]
    pub fn table_seq(&self) -> TableSeq {
        let seq_part = self.0 & TableSeq::MASK;

        TableSeq(seq_part)
    }

    /// Convert table id into u64.
    #[inline]
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl From<u64> for TableId {
    fn from(id: u64) -> TableId {
        TableId(id)
    }
}

impl fmt::Debug for TableId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TableId({}, {}, {})",
            self.0,
            self.schema_id().as_u32(),
            self.table_seq().as_u64()
        )
    }
}

impl fmt::Display for TableId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// TODO(yingwen): Support DELETE/UPDATE... , a mutation type is needed.
#[derive(Debug)]
pub struct WriteRequest {
    /// rows to write
    pub row_group: RowGroup,
}

#[derive(Debug)]
pub struct ReadOptions {
    pub batch_size: usize,
    /// Suggested read parallelism, the actual returned stream should equal to
    /// `read_parallelism`.
    pub read_parallelism: usize,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            batch_size: 10000,
            read_parallelism: DEFAULT_READ_PARALLELISM,
        }
    }
}

#[derive(Debug)]
pub struct GetRequest {
    /// Query request id.
    pub request_id: RequestId,
    /// The schema and projection for get, the output data should match this
    /// schema.
    pub projected_schema: ProjectedSchema,
    /// The primary key of the row to get.
    pub primary_key: Vec<Datum>,
}

#[derive(Copy, Clone, Debug)]
pub enum ReadOrder {
    /// No order requirements from the read request.
    None,
    Asc,
    Desc,
}

impl ReadOrder {
    pub fn from_is_asc(is_asc: Option<bool>) -> Self {
        match is_asc {
            Some(true) => ReadOrder::Asc,
            Some(false) => ReadOrder::Desc,
            None => ReadOrder::None,
        }
    }

    #[inline]
    pub fn is_out_of_order(&self) -> bool {
        matches!(self, ReadOrder::None)
    }

    #[inline]
    pub fn is_in_order(&self) -> bool {
        !self.is_out_of_order()
    }

    #[inline]
    pub fn is_in_desc_order(&self) -> bool {
        matches!(self, ReadOrder::Desc)
    }
}

#[derive(Debug)]
pub struct ReadRequest {
    /// Read request id.
    pub request_id: RequestId,
    /// Read options.
    pub opts: ReadOptions,
    /// The schema and projection for read, the output data should match this
    /// schema.
    pub projected_schema: ProjectedSchema,
    /// Predicate of the query.
    pub predicate: PredicateRef,
    /// Read the rows in reverse order.
    pub order: ReadOrder,
}

#[derive(Debug)]
pub struct AlterSchemaRequest {
    /// The new schema.
    pub schema: Schema,
    /// Previous schema version before alteration.
    pub pre_schema_version: Version,
}

#[derive(Debug)]
pub struct FlushRequest {
    /// Trigger a compaction after flush, default is true.
    pub compact_after_flush: bool,
    /// Whether to wait flush task finishes, default is true.
    pub sync: bool,
}

impl Default for FlushRequest {
    fn default() -> Self {
        Self {
            compact_after_flush: true,
            sync: true,
        }
    }
}

/// Table abstraction
///
/// We do not let Table trait extends datafusion's TableProvider, since
/// that will tie out abstraction with datafusion. However, we still use
/// datafusion's RecordBatchStream trait.
#[async_trait]
pub trait Table: std::fmt::Debug {
    /// Returns table name.
    fn name(&self) -> &str;

    /// Returns the id of this table.
    fn id(&self) -> TableId;

    /// Schema of this table.
    fn schema(&self) -> Schema;

    /// Options of this table.
    fn options(&self) -> HashMap<String, String>;

    /// Engine type of this table.
    fn engine_type(&self) -> &str;

    /// Get table's statistics.
    fn stats(&self) -> TableStats;

    /// Write to table.
    async fn write(&self, request: WriteRequest) -> Result<usize>;

    /// Read from table.
    async fn read(&self, request: ReadRequest) -> Result<SendableRecordBatchStream>;

    /// Get the specific row according to the primary key.
    /// TODO(xikai): object-safety is not ensured by now if the default
    ///  implementation is provided. Actually it is better to use the read
    ///  method to implement the get method.
    async fn get(&self, request: GetRequest) -> Result<Option<Row>>;

    /// Read multiple partition of the table in parallel.
    async fn partitioned_read(&self, request: ReadRequest) -> Result<PartitionedStreams>;

    /// Alter table schema to the schema specific in [AlterSchemaRequest] if
    /// the `pre_schema_version` is equal to current schema version.
    ///
    /// Returns the affected rows (always 1).
    async fn alter_schema(&self, request: AlterSchemaRequest) -> Result<usize>;

    /// Alter table options.
    ///
    /// Returns the affected rows (always 1).
    async fn alter_options(&self, options: HashMap<String, String>) -> Result<usize>;

    /// Flush this table.
    async fn flush(&self, request: FlushRequest) -> Result<()>;

    /// Compact this table and wait until compaction completes.
    async fn compact(&self) -> Result<()>;
}

/// Basic statistics of table.
#[derive(Debug, Clone, Copy, Default)]
pub struct TableStats {
    /// Total write request
    pub num_write: u64,
    /// Total read request
    pub num_read: u64,
    /// Total flush request
    pub num_flush: u64,
}

/// A reference-counted pointer to Table
pub type TableRef = Arc<dyn Table + Send + Sync>;

/// Helper to generate a schema id.
pub struct SchemaIdGenerator {
    last_schema_id: AtomicU32,
}

impl SchemaIdGenerator {
    pub fn last_schema_id_u32(&self) -> u32 {
        self.last_schema_id.load(Ordering::Relaxed)
    }

    pub fn set_last_schema_id(&self, last_schema_id: SchemaId) {
        self.last_schema_id
            .store(last_schema_id.as_u32(), Ordering::Relaxed);
    }

    pub fn alloc_schema_id(&self) -> Option<SchemaId> {
        let last = self.last_schema_id.fetch_add(1, Ordering::Relaxed);

        SchemaId::new(last + 1)
    }
}

impl Default for SchemaIdGenerator {
    fn default() -> Self {
        Self {
            last_schema_id: AtomicU32::new(SchemaId::MIN.as_u32()),
        }
    }
}

/// Helper to generate a table sequence.
pub struct TableSeqGenerator {
    last_table_seq: AtomicU64,
}

impl TableSeqGenerator {
    pub fn last_table_seq_u64(&self) -> u64 {
        self.last_table_seq.load(Ordering::Relaxed)
    }

    pub fn set_last_table_seq(&self, last_table_seq: TableSeq) {
        self.last_table_seq
            .store(last_table_seq.as_u64(), Ordering::Relaxed);
    }

    pub fn alloc_table_seq(&self) -> Option<TableSeq> {
        let last = self.last_table_seq.fetch_add(1, Ordering::Relaxed);

        TableSeq::new(last + 1)
    }
}

impl Default for TableSeqGenerator {
    fn default() -> Self {
        Self {
            last_table_seq: AtomicU64::new(TableSeq::MIN.as_u64()),
        }
    }
}

/// Create table request in catalog
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// Catalog name
    pub catalog_name: String,
    /// Schema name
    pub schema_name: String,
    /// Table id
    pub table_id: TableId,
    /// Table name
    pub table_name: String,
    /// Table engine type
    pub engine: String,
    /// Tells state of the table
    pub state: TableState,
}

#[derive(Debug, Snafu)]
pub struct TryFromTableEntryError(common_types::schema::Error);

impl TryFrom<TableEntry> for TableInfo {
    type Error = TryFromTableEntryError;

    fn try_from(entry: TableEntry) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            catalog_name: entry.catalog_name,
            schema_name: entry.schema_name,
            table_id: entry.table_id.into(),
            table_name: entry.table_name,
            engine: entry.engine,
            state: TableState::from(entry.state),
        })
    }
}

impl From<TableInfo> for TableEntry {
    fn from(table_info: TableInfo) -> Self {
        let mut entry = TableEntry::new();
        entry.set_catalog_name(table_info.catalog_name);
        entry.set_schema_name(table_info.schema_name);
        entry.set_table_id(table_info.table_id.as_u64());
        entry.set_table_name(table_info.table_name);
        entry.set_engine(table_info.engine);
        entry.set_state(TableStatePb::from(table_info.state));

        entry
    }
}

impl TableInfo {
    // TODO(chunshao.rcs): refactor
    pub fn into_pb(self, typ: TableRequestType) -> TableEntry {
        let mut table_entry: TableEntry = self.into();
        match typ {
            TableRequestType::Create => table_entry.set_created_time(Timestamp::now().as_i64()),
            TableRequestType::Drop => table_entry.set_modified_time(Timestamp::now().as_i64()),
        }
        table_entry
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_id() {
        assert_eq!(0, SchemaId::MIN.as_u32());
        assert_eq!(0xffffff, SchemaId::MAX.as_u32());
    }

    #[test]
    fn test_table_seq() {
        assert_eq!(0, TableSeq::MIN.as_u64());
        assert_eq!(0xffffffffff, TableSeq::MAX.as_u64());
    }
}
