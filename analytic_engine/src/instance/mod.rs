// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! A table engine instance
//!
//! The root mod only contains common functions of instance, other logics are
//! divided into the sub crates

mod alter;
mod drop;
mod engine;
pub mod flush_compaction;
pub(crate) mod mem_collector;
pub mod open;
mod read;
mod write;
pub mod write_worker;

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use common_util::{define_result, runtime::Runtime};
use log::info;
use mem_collector::MemUsageCollector;
use object_store::ObjectStore;
use parquet::{DataCacheRef, MetaCacheRef};
use snafu::{ResultExt, Snafu};
use table_engine::engine::EngineRuntimes;
use tokio::sync::Mutex;
use wal::manager::WalManager;

use crate::{
    compaction::scheduler::CompactionSchedulerRef,
    meta::Manifest,
    space::{SpaceId, SpaceName, SpaceNameRef, SpaceRef},
    sst::file::FilePurger,
    table::data::TableDataRef,
    TableOptions,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to stop file purger, err:{}", source))]
    StopFilePurger { source: crate::sst::file::Error },

    #[snafu(display("Failed to stop compaction scheduler, err:{}", source))]
    StopScheduler {
        source: crate::compaction::scheduler::Error,
    },

    #[snafu(display("Failed to close space, name:{}, err:{}", name, source))]
    CloseSpace {
        name: String,
        source: crate::space::Error,
    },
}

define_result!(Error);

/// Meta state
#[derive(Debug)]
struct MetaState {
    /// Id of the last space
    last_space_id: SpaceId,
}

impl MetaState {
    /// Create a new state
    fn new() -> Self {
        Self { last_space_id: 1 }
    }

    /// Acquire next id for a new space
    fn alloc_space_id(&mut self) -> SpaceId {
        self.last_space_id += 1;
        self.last_space_id
    }
}

impl Default for MetaState {
    fn default() -> Self {
        Self::new()
    }
}

/// Spaces states
#[derive(Default)]
struct Spaces {
    /// Name to space
    name_to_space: HashMap<SpaceName, SpaceRef>,
    /// Id to space
    id_to_space: HashMap<SpaceId, SpaceRef>,
}

impl Spaces {
    /// Insert space by name, and also insert id to space mapping
    fn insert(&mut self, space_name: SpaceName, space: SpaceRef) {
        let space_id = space.id;
        self.name_to_space.insert(space_name, space.clone());
        self.id_to_space.insert(space_id, space);
    }

    fn get_by_name(&self, name: SpaceNameRef) -> Option<&SpaceRef> {
        self.name_to_space.get(name)
    }

    /// List all tables of all spaces
    fn list_all_tables(&self, tables: &mut Vec<TableDataRef>) {
        let total_tables = self.id_to_space.values().map(|s| s.table_num()).sum();
        tables.reserve(total_tables);
        for space in self.id_to_space.values() {
            space.list_all_tables(tables);
        }
    }

    fn list_all_spaces(&self) -> Vec<SpaceRef> {
        self.id_to_space.values().cloned().collect()
    }
}

pub struct SpaceStore<Wal, Meta, Store, Fa> {
    /// All spaces of the engine.
    spaces: RwLock<Spaces>,
    /// Manifest (or meta) stores meta data of the engine instance.
    manifest: Meta,
    /// Wal of all tables
    wal_manager: Wal,
    /// Sst storage.
    store: Arc<Store>,
    /// Meta lock protects mutation to meta data of the instance. This lock
    /// should be held when persisting mutation of the instance level meta data
    /// to the manifest.
    /// - add a space
    /// - delete a space
    ///
    /// Mutation to space's meta, like add/delete a table, is protected by
    /// space's lock instead of this lock.
    meta_state: Mutex<MetaState>,
    /// Sst factory.
    sst_factory: Fa,

    meta_cache: Option<MetaCacheRef>,
    data_cache: Option<DataCacheRef>,
}

impl<Wal, Meta, Store, Fa> Drop for SpaceStore<Wal, Meta, Store, Fa> {
    fn drop(&mut self) {
        info!("SpaceStore dropped");
    }
}

impl<Wal, Meta, Store, Fa> SpaceStore<Wal, Meta, Store, Fa> {
    async fn close(&self) -> Result<()> {
        let spaces = self.spaces.read().unwrap().list_all_spaces();
        for space in spaces {
            // Close all spaces.
            space
                .close()
                .await
                .context(CloseSpace { name: &space.name })?;
        }

        Ok(())
    }
}

impl<Wal, Meta, Store, Fa> SpaceStore<Wal, Meta, Store, Fa> {
    fn store_ref(&self) -> &Store {
        &*self.store
    }

    /// List all tables of all spaces
    pub fn list_all_tables(&self, tables: &mut Vec<TableDataRef>) {
        let spaces = self.spaces.read().unwrap();
        spaces.list_all_tables(tables);
    }

    /// Find the space which it's all memtables consumes maximum memory.
    #[inline]
    fn find_maximum_memory_usage_space(&self) -> Option<SpaceRef> {
        let spaces = self.spaces.read().unwrap().list_all_spaces();
        spaces.into_iter().max_by_key(|t| t.memtable_memory_usage())
    }
}

/// Table engine instance
///
/// Manages all spaces, also contains needed resources shared across all table
// TODO(yingwen): Track memory usage of all tables (or tables of space)
pub struct Instance<Wal, Meta, Store, Fa> {
    /// Space storage
    space_store: Arc<SpaceStore<Wal, Meta, Store, Fa>>,
    /// Runtime to execute async tasks.
    runtimes: Arc<EngineRuntimes>,
    /// Global table options, overwrite mutable options in each table's
    /// TableOptions.
    table_opts: TableOptions,

    // Write group options:
    write_group_worker_num: usize,
    write_group_command_channel_cap: usize,
    // End of write group options.
    compaction_scheduler: CompactionSchedulerRef,
    file_purger: FilePurger,

    meta_cache: Option<MetaCacheRef>,
    data_cache: Option<DataCacheRef>,
    /// Engine memtable memory usage collector
    mem_usage_collector: Arc<MemUsageCollector>,
    /// Engine write buffer size
    pub(crate) db_write_buffer_size: usize,
    /// Space write buffer size
    pub(crate) space_write_buffer_size: usize,
}

impl<Wal, Meta, Store, Fa> Instance<Wal, Meta, Store, Fa> {
    /// Close the instance gracefully.
    pub async fn close(&self) -> Result<()> {
        self.file_purger.stop().await.context(StopFilePurger)?;

        self.space_store.close().await?;

        self.compaction_scheduler
            .stop_scheduler()
            .await
            .context(StopScheduler)
    }
}

// TODO(yingwen): Instance builder
impl<Wal: WalManager + Send + Sync, Meta: Manifest, Store: ObjectStore, Fa>
    Instance<Wal, Meta, Store, Fa>
{
    /// Find space using read lock
    fn get_space_by_read_lock(&self, space: SpaceNameRef) -> Option<SpaceRef> {
        let spaces = self.space_store.spaces.read().unwrap();
        spaces.get_by_name(space).cloned()
    }

    /// Returns options to create a write group for given space
    fn write_group_options(&self, space_id: SpaceId) -> write_worker::Options {
        write_worker::Options {
            space_id,
            worker_num: self.write_group_worker_num,
            runtime: self.write_runtime().clone(),
            command_channel_capacity: self.write_group_command_channel_cap,
        }
    }

    /// Returns true when engine instance's total memtable memory usage reaches
    /// db_write_buffer_size limit.
    #[inline]
    fn should_flush_instance(&self) -> bool {
        self.db_write_buffer_size > 0
            && self.mem_usage_collector.total_memory_allocated() >= self.db_write_buffer_size
    }

    #[inline]
    fn read_runtime(&self) -> &Arc<Runtime> {
        &self.runtimes.read_runtime
    }

    #[inline]
    fn write_runtime(&self) -> &Arc<Runtime> {
        &self.runtimes.write_runtime
    }
}

/// Instance reference
pub type InstanceRef<Wal, Meta, Store, Fa> = Arc<Instance<Wal, Meta, Store, Fa>>;
