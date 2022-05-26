// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Table space
//!
//! A table space acts like a namespace of a bunch of tables, tables under
//! different space can use same table name

use std::{
    fmt,
    sync::{Arc, RwLock},
};

use arena::CollectorRef;
use common_util::define_result;
use log::info;
use snafu::{Backtrace, ResultExt, Snafu};
use table_engine::{engine::CreateTableRequest, table::TableId};
use tokio::sync::Mutex;

use crate::{
    instance::{mem_collector::MemUsageCollector, write_worker::WriteGroup},
    meta::{
        meta_update::{AddTableMeta, MetaUpdate},
        Manifest,
    },
    sst::file::FilePurger,
    table::data::{TableData, TableDataRef, TableDataSet},
    TableOptions,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Table already exists, table:{}.\nBacktrace:\n{}", table, backtrace))]
    TableExists { table: String, backtrace: Backtrace },

    #[snafu(display("Failed to create table data, table:{}, err:{}", table, source))]
    CreateTableData {
        table: String,
        source: crate::table::data::Error,
    },

    #[snafu(display("Failed to store meta data, err:{}", source))]
    WriteMeta {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

define_result!(Error);

impl From<Error> for table_engine::engine::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::TableExists { table, backtrace } => Self::TableExists { table, backtrace },
            Error::CreateTableData { ref table, .. } => Self::InvalidArguments {
                table: table.clone(),
                source: Box::new(err),
            },
            Error::WriteMeta { .. } => Self::WriteMeta {
                source: Box::new(err),
            },
        }
    }
}

/// Holds references to the table data and its space
///
/// REQUIRE: The table must belongs to the space
#[derive(Clone)]
pub struct SpaceAndTable {
    /// The space of the table
    space: SpaceRef,
    /// Data of the table
    table_data: TableDataRef,
}

impl SpaceAndTable {
    /// Create SpaceAndTable
    ///
    /// REQUIRE: The table must belongs to the space
    pub fn new(space: SpaceRef, table_data: TableDataRef) -> Self {
        // Checks table is in space
        debug_assert!(space
            .table_datas
            .read()
            .unwrap()
            .find_table(&table_data.name)
            .is_some());

        Self { space, table_data }
    }

    /// Get space info
    #[inline]
    pub fn space(&self) -> &SpaceRef {
        &self.space
    }

    /// Get table data
    #[inline]
    pub fn table_data(&self) -> &TableDataRef {
        &self.table_data
    }
}

impl fmt::Debug for SpaceAndTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpaceAndTable")
            .field("space_id", &self.space.id)
            .field("space_name", &self.space.name)
            .field("table_id", &self.table_data.id)
            .field("table_name", &self.table_data.name)
            .finish()
    }
}

/// Name type of space
// TODO(yingwen): Or use binary string?
pub type SpaceName = String;
/// Reference of space name
pub type SpaceNameRef<'a> = &'a str;
/// Space id
// TODO(yingwen): Or just use something like uuid as space id?
pub type SpaceId = u32;

/// A space can hold mulitple tables
pub struct Space {
    /// Space id
    pub id: SpaceId,
    /// Space name
    pub name: SpaceName,
    /// Data of tables in this space
    ///
    /// Adding table into it should acquire the space lock first, then the write
    /// lock
    table_datas: RwLock<TableDataSet>,
    /// Space lock
    ///
    /// Persisting meta update of this space is protected by this lock
    mutex: Mutex<()>,

    /// Write workers
    pub write_group: WriteGroup,
    /// Space memtable memory usage collector
    pub mem_usage_collector: Arc<MemUsageCollector>,
    /// The maximum write buffer size used for single space.
    pub write_buffer_size: usize,
}

impl Space {
    pub fn new(
        id: SpaceId,
        name: SpaceName,
        write_buffer_size: usize,
        write_group: WriteGroup,
        engine_mem_collector: CollectorRef,
    ) -> Self {
        Self {
            id,
            name,
            table_datas: RwLock::new(TableDataSet::new()),
            mutex: Mutex::new(()),
            write_group,
            mem_usage_collector: Arc::new(MemUsageCollector::with_parent(engine_mem_collector)),
            write_buffer_size,
        }
    }

    /// Returns true when space total memtable memory usage reaches
    /// space_write_buffer_size limit.
    #[inline]
    pub fn should_flush_space(&self) -> bool {
        self.write_buffer_size > 0 && self.memtable_memory_usage() >= self.write_buffer_size
    }

    /// Find the table in space which it's memtable consumes maximum memory.
    #[inline]
    pub fn find_maximum_memory_usage_table(&self) -> Option<TableDataRef> {
        self.table_datas
            .read()
            .unwrap()
            .find_maximum_memory_usage_table()
    }

    #[inline]
    pub fn memtable_memory_usage(&self) -> usize {
        self.mem_usage_collector.total_memory_allocated()
    }

    pub async fn close(&self) -> Result<()> {
        // Stop the write group.
        self.write_group.stop().await;

        Ok(())
    }

    /// Create a table under this space
    ///
    /// Returns error if the table already exists
    pub async fn create_table<Meta: Manifest>(
        &self,
        request: CreateTableRequest,
        manifest: &Meta,
        table_opts: &TableOptions,
        purger: &FilePurger,
    ) -> Result<TableDataRef> {
        info!(
            "Space create table, space_id:{}, space_name:{}, request:{:?}",
            self.id, self.name, request
        );

        // Checks whether the table is exists
        if self.find_table(&request.table_name).is_some() {
            return TableExists {
                table: request.table_name,
            }
            .fail();
        }

        // Choose a write worker for this table
        let write_handle = self.write_group.choose_worker(request.table_id);

        let _lock = self.mutex.lock().await;

        // Double check for table existence under space lock
        if self.find_table(&request.table_name).is_some() {
            return TableExists {
                table: request.table_name,
            }
            .fail();
        }

        // Store table info into meta
        let update = MetaUpdate::AddTable(AddTableMeta {
            space_id: self.id,
            table_id: request.table_id,
            table_name: request.table_name.clone(),
            schema: request.table_schema.clone(),
            opts: table_opts.clone(),
        });
        manifest
            .store_update(update)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(WriteMeta)?;

        // Update memory state
        let table_name = request.table_name.clone();
        let table_data = Arc::new(
            TableData::new(
                self.id,
                request,
                write_handle,
                table_opts.clone(),
                purger,
                self.mem_usage_collector.clone(),
            )
            .context(CreateTableData { table: &table_name })?,
        );

        self.insert_table(table_data.clone());

        Ok(table_data)
    }

    /// Insert table data into space memory state if the table is
    /// absent. For internal use only
    ///
    /// Panic if the table is already exists
    pub(crate) fn insert_table(&self, table_data: TableDataRef) {
        let success = self
            .table_datas
            .write()
            .unwrap()
            .insert_if_absent(table_data);
        assert!(success);
    }

    /// Find table under this space by table name
    pub fn find_table(&self, table_name: &str) -> Option<TableDataRef> {
        self.table_datas.read().unwrap().find_table(table_name)
    }

    /// Find table under this space by its id
    pub fn find_table_by_id(&self, table_id: TableId) -> Option<TableDataRef> {
        self.table_datas.read().unwrap().find_table_by_id(table_id)
    }

    /// Remove table under this space by table name
    pub fn remove_table(&self, table_name: &str) -> Option<TableDataRef> {
        self.table_datas.write().unwrap().remove_table(table_name)
    }

    /// Returns the total table num in this space
    pub fn table_num(&self) -> usize {
        self.table_datas.read().unwrap().table_num()
    }

    /// List all tables of this space to `tables`
    pub fn list_all_tables(&self, tables: &mut Vec<TableDataRef>) {
        self.table_datas.read().unwrap().list_all_tables(tables)
    }
}

/// A reference to space
pub type SpaceRef = Arc<Space>;
