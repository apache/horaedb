// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Table space
//!
//! A table space acts like a namespace of a bunch of tables, tables under
//! different space can use same table name

use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, RwLock},
};

use arena::CollectorRef;
use table_engine::table::TableId;

use crate::{
    instance::mem_collector::MemUsageCollector,
    table::data::{TableDataRef, TableDataSet},
};

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
            .field("table_id", &self.table_data.id)
            .field("table_name", &self.table_data.name)
            .finish()
    }
}

/// Space id
// TODO(yingwen): Or just use something like uuid as space id?
pub type SpaceId = u32;

#[derive(Debug)]
pub struct SpaceContext {
    /// Catalog name
    pub catalog_name: String,
    /// Schema name
    pub schema_name: String,
}

/// A space can hold multiple tables
pub struct Space {
    /// Space id
    pub id: SpaceId,
    /// Space context
    pub context: SpaceContext,

    /// Data of tables in this space
    ///
    /// Adding table into it should acquire the space lock first, then the write
    /// lock
    table_datas: RwLock<TableDataSet>,

    /// If table open failed, request of this table is not allowed, otherwise
    /// schema may become inconsistent.
    // TODO: engine should provide a repair method to fix those failed tables.
    open_failed_tables: RwLock<Vec<String>>,

    /// Space memtable memory usage collector
    pub mem_usage_collector: Arc<MemUsageCollector>,
    /// The maximum write buffer size used for single space.
    pub write_buffer_size: usize,
}

impl Space {
    pub fn new(
        id: SpaceId,
        context: SpaceContext,
        write_buffer_size: usize,
        engine_mem_collector: CollectorRef,
    ) -> Self {
        Self {
            id,
            context,
            table_datas: Default::default(),
            open_failed_tables: Default::default(),
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

    /// Find the table whose memtable consumes the most memory in the space by
    /// specifying Worker.
    #[inline]
    pub fn find_maximum_memory_usage_table(&self) -> Option<TableDataRef> {
        self.table_datas
            .read()
            .unwrap()
            .find_maximum_memory_usage_table()
    }

    /// Find the table whose mutable memtable consumes the most memory in the
    /// space by specifying Worker.
    #[inline]
    pub fn find_maximum_mutable_memory_usage_table(&self) -> Option<TableDataRef> {
        self.table_datas
            .read()
            .unwrap()
            .find_maximum_mutable_memory_usage_table()
    }

    #[inline]
    pub fn memtable_memory_usage(&self) -> usize {
        self.mem_usage_collector.total_memory_allocated()
    }

    /// Insert table data into space memory state if the table is
    /// absent. For internal use only
    ///
    /// Panic if the table has already existed.
    pub(crate) fn insert_table(&self, table_data: TableDataRef) {
        let success = self
            .table_datas
            .write()
            .unwrap()
            .insert_if_absent(table_data);
        assert!(success);
    }

    pub(crate) fn insert_open_failed_table(&self, table_name: String) {
        self.open_failed_tables.write().unwrap().push(table_name)
    }

    pub(crate) fn is_open_failed_table(&self, table_name: &String) -> bool {
        self.open_failed_tables.read().unwrap().contains(table_name)
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

    pub fn space_id(&self) -> SpaceId {
        self.id
    }
}

/// A reference to space
pub type SpaceRef = Arc<Space>;

impl fmt::Debug for Space {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Space")
            .field("id", &self.id)
            .field("context", &self.context)
            .field("write_buffer_size", &self.write_buffer_size)
            .finish()
    }
}

/// Spaces states
#[derive(Default)]
pub(crate) struct Spaces {
    /// Id to space
    id_to_space: HashMap<SpaceId, SpaceRef>,
}

impl Spaces {
    /// Insert space by name, and also insert id to space mapping
    pub fn insert(&mut self, space: SpaceRef) {
        let space_id = space.id;
        self.id_to_space.insert(space_id, space);
    }

    pub fn get_by_id(&self, id: SpaceId) -> Option<&SpaceRef> {
        self.id_to_space.get(&id)
    }

    /// List all tables of all spaces
    pub fn list_all_tables(&self, tables: &mut Vec<TableDataRef>) {
        let total_tables = self.id_to_space.values().map(|s| s.table_num()).sum();
        tables.reserve(total_tables);
        for space in self.id_to_space.values() {
            space.list_all_tables(tables);
        }
    }

    pub fn list_all_spaces(&self) -> Vec<SpaceRef> {
        self.id_to_space.values().cloned().collect()
    }
}

pub(crate) type SpacesRef = Arc<RwLock<Spaces>>;
