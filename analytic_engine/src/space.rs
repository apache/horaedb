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
use table_engine::table::TableId;

use crate::{
    instance::{mem_collector::MemUsageCollector, write_worker::WriteGroup},
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

/// A space can hold multiple tables
pub struct Space {
    /// Space id
    pub id: SpaceId,
    /// Data of tables in this space
    ///
    /// Adding table into it should acquire the space lock first, then the write
    /// lock
    table_datas: RwLock<TableDataSet>,

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
        write_buffer_size: usize,
        write_group: WriteGroup,
        engine_mem_collector: CollectorRef,
    ) -> Self {
        Self {
            id,
            table_datas: RwLock::new(TableDataSet::new()),
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

    /// Find the table whose memtable consumes the most memory in the space by
    /// specifying Worker.
    #[inline]
    pub fn find_maximum_memory_usage_table(&self, worker_index: usize) -> Option<TableDataRef> {
        let worker_num = self.write_group.worker_num();
        self.table_datas
            .read()
            .unwrap()
            .find_maximum_memory_usage_table(worker_num, worker_index)
    }

    #[inline]
    pub fn memtable_memory_usage(&self) -> usize {
        self.mem_usage_collector.total_memory_allocated()
    }

    pub async fn close(&self) {
        // Stop the write group.
        self.write_group.stop().await;
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
