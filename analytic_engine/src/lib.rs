// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Analytic table engine implementations

mod compaction;
mod context;
mod engine;
mod instance;
pub mod memtable;
mod meta;
mod payload;
mod role_table;
pub mod row_iter;
mod sampler;
pub mod setup;
pub mod space;
pub mod sst;
mod storage_options;
pub mod table;
pub mod table_options;
mod wal_synchronizer;

#[cfg(any(test, feature = "test"))]
pub mod tests;

use message_queue::kafka::config::Config as KafkaConfig;
use meta::details::Options as ManifestOptions;
use serde_derive::Deserialize;
use storage_options::{LocalOptions, StorageOptions};
use table_kv::config::ObkvConfig;
use wal::{
    message_queue_impl::config::Config as MessageQueueWalConfig,
    table_kv_impl::model::NamespaceConfig,
};

pub use crate::{compaction::scheduler::SchedulerConfig, table_options::TableOptions};

/// Config of analytic engine.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Storage options of the engine.
    pub storage: StorageOptions,

    /// WAL path of the engine.
    pub wal_path: String,

    /// Batch size to read records from wal to replay.
    pub replay_batch_size: usize,
    /// Batch size to replay tables.
    pub max_replay_tables_per_batch: usize,
    // Write group options:
    pub write_group_worker_num: usize,
    pub write_group_command_channel_cap: usize,
    // End of write group options.
    /// Default options for table.
    pub table_opts: TableOptions,

    pub compaction_config: SchedulerConfig,

    /// sst meta cache capacity.
    pub sst_meta_cache_cap: Option<usize>,
    /// sst data cache capacity.
    pub sst_data_cache_cap: Option<usize>,

    /// Manifest options.
    pub manifest: ManifestOptions,

    // Global write buffer options:
    /// The maximum write buffer size used for single space.
    pub space_write_buffer_size: usize,
    /// The maximum size of all Write Buffers across all spaces.
    pub db_write_buffer_size: usize,
    // End of global write buffer options.

    // Batch size for scan sst
    pub scan_batch_size: usize,

    /// Wal storage config
    ///
    /// Now, following three storages are supported:
    /// + RocksDB
    /// + OBKV
    /// + Kafka
    pub wal_storage: WalStorageConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            storage: StorageOptions::Local(LocalOptions {
                data_path: String::from("/tmp/ceresdb"),
            }),
            wal_path: String::from("/tmp/ceresdb"),
            replay_batch_size: 500,
            max_replay_tables_per_batch: 64,
            write_group_worker_num: 8,
            write_group_command_channel_cap: 128,
            table_opts: TableOptions::default(),
            compaction_config: SchedulerConfig::default(),
            sst_meta_cache_cap: Some(1000),
            sst_data_cache_cap: Some(1000),
            manifest: ManifestOptions::default(),
            /// Zero means disabling this param, give a positive value to enable
            /// it.
            space_write_buffer_size: 0,
            /// Zero means disabling this param, give a positive value to enable
            /// it.
            db_write_buffer_size: 0,
            scan_batch_size: 500,
            wal_storage: WalStorageConfig::RocksDB,
        }
    }
}

/// Config of wal based on obkv.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ObkvWalConfig {
    /// Obkv client config.
    pub obkv: ObkvConfig,
    /// Wal (stores data) namespace config.
    pub wal: NamespaceConfig,
    /// Manifest (stores meta data) namespace config.
    pub manifest: NamespaceConfig,
}

impl Default for ObkvWalConfig {
    fn default() -> Self {
        Self {
            obkv: ObkvConfig::default(),
            wal: NamespaceConfig::default(),
            manifest: NamespaceConfig {
                // Manifest has no ttl.
                ttl: None,
                ..Default::default()
            },
        }
    }
}

/// Config of wal based on obkv.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default)]
pub struct KafkaWalConfig {
    /// Kafka client config
    pub kafka_config: KafkaConfig,

    /// Wal config
    pub wal_config: MessageQueueWalConfig,
}

/// Options for storage backend
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub enum WalStorageConfig {
    RocksDB,
    Obkv(Box<ObkvWalConfig>),
    Kafka(Box<KafkaWalConfig>),
}
