// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Setup the analytic engine

use std::{num::NonZeroUsize, path::Path, pin::Pin, sync::Arc};

use async_trait::async_trait;
use common_util::define_result;
use futures::Future;
use message_queue::kafka::kafka_impl::KafkaImpl;
use object_store::{
    aliyun,
    disk_cache::DiskCacheStore,
    mem_cache::{MemCache, MemCacheStore},
    metrics::StoreWithMetrics,
    prefix::StoreWithPrefix,
    LocalFileSystem, ObjectStoreRef,
};
use remote_engine_client::RemoteEngineImpl;
use router::RouterRef;
use snafu::{Backtrace, ResultExt, Snafu};
use table_engine::{
    engine::{EngineRuntimes, TableEngineRef},
    remote::RemoteEngineRef,
};
use table_kv::{memory::MemoryImpl, obkv::ObkvImpl, TableKv};
use wal::{
    manager::{self, WalManagerRef},
    message_queue_impl::wal::MessageQueueImpl,
    rocks_impl::manager::Builder as RocksWalBuilder,
    table_kv_impl::{wal::WalNamespaceImpl, WalRuntimes},
};

use crate::{
    context::OpenContext,
    engine::TableEngineImpl,
    instance::{Instance, InstanceRef},
    manifest::{details::ManifestImpl, ManifestRef},
    sst::{
        factory::{FactoryImpl, ObjectStorePicker, ObjectStorePickerRef, ReadFrequency},
        meta_data::cache::{MetaCache, MetaCacheRef},
    },
    storage_options::{ObjectStoreOptions, StorageOptions},
    Config, ObkvWalConfig, WalStorageConfig,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to open engine instance, err:{}", source))]
    OpenInstance {
        source: crate::instance::engine::Error,
    },

    #[snafu(display("Failed to open wal, err:{}", source))]
    OpenWal { source: manager::error::Error },

    #[snafu(display(
        "Failed to open with the invalid config, msg:{}.\nBacktrace:\n{}",
        msg,
        backtrace
    ))]
    InvalidWalConfig { msg: String, backtrace: Backtrace },

    #[snafu(display("Failed to open wal for manifest, err:{}", source))]
    OpenManifestWal { source: manager::error::Error },

    #[snafu(display("Failed to open manifest, err:{}", source))]
    OpenManifest {
        source: crate::manifest::details::Error,
    },

    #[snafu(display("Failed to open obkv, err:{}", source))]
    OpenObkv { source: table_kv::obkv::Error },

    #[snafu(display("Failed to execute in runtime, err:{}", source))]
    RuntimeExec { source: common_util::runtime::Error },

    #[snafu(display("Failed to open object store, err:{}", source))]
    OpenObjectStore {
        source: object_store::ObjectStoreError,
    },

    #[snafu(display("Failed to create dir for {}, err:{}", path, source))]
    CreateDir {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Failed to open kafka, err:{}", source))]
    OpenKafka {
        source: message_queue::kafka::kafka_impl::Error,
    },

    #[snafu(display("Failed to create mem cache, err:{}", source))]
    OpenMemCache {
        source: object_store::mem_cache::Error,
    },
}

define_result!(Error);

const WAL_DIR_NAME: &str = "wal";
const MANIFEST_DIR_NAME: &str = "manifest";
const STORE_DIR_NAME: &str = "store";
const DISK_CACHE_DIR_NAME: &str = "sst_cache";

/// Builder for [TableEngine].
///
/// [TableEngine]: table_engine::engine::TableEngine
#[derive(Clone)]
pub struct EngineBuilder<'a> {
    pub config: &'a Config,
    pub router: Option<RouterRef>,
    pub engine_runtimes: Arc<EngineRuntimes>,
    pub opened_wals: OpenedWals,
}

impl<'a> EngineBuilder<'a> {
    pub async fn build(self) -> Result<TableEngineRef> {
        let opened_storages = open_storage(self.config.storage.clone()).await?;
        let manifest = ManifestImpl::open(
            self.config.manifest.clone(),
            self.opened_wals.manifest_wal.clone(),
            opened_storages.default_store().clone(),
        )
        .await
        .context(OpenManifest)?;

        let instance = open_instance(
            self.config.clone(),
            self.engine_runtimes,
            self.opened_wals.data_wal,
            Arc::new(manifest),
            Arc::new(opened_storages),
            self.router,
        )
        .await?;
        Ok(Arc::new(TableEngineImpl::new(instance)))
    }
}

#[derive(Debug, Clone)]
pub struct OpenedWals {
    pub data_wal: WalManagerRef,
    pub manifest_wal: WalManagerRef,
}

/// Analytic engine builder.
#[async_trait]
pub trait WalsOpener: Send + Sync + Default {
    async fn open_wals(
        &self,
        config: &WalStorageConfig,
        engine_runtimes: Arc<EngineRuntimes>,
    ) -> Result<OpenedWals>;
}

/// [RocksEngine] builder.
#[derive(Default)]
pub struct RocksDBWalsOpener;

#[async_trait]
impl WalsOpener for RocksDBWalsOpener {
    async fn open_wals(
        &self,
        config: &WalStorageConfig,
        engine_runtimes: Arc<EngineRuntimes>,
    ) -> Result<OpenedWals> {
        let rocksdb_wal_config = match &config {
            WalStorageConfig::RocksDB(config) => config.clone(),
            _ => {
                return InvalidWalConfig {
                    msg: format!(
                        "invalid wal storage config while opening rocksDB wal, config:{config:?}"
                    ),
                }
                .fail();
            }
        };

        let write_runtime = engine_runtimes.write_runtime.clone();
        let data_path = Path::new(&rocksdb_wal_config.data_dir);
        let wal_path = data_path.join(WAL_DIR_NAME);
        let data_wal =
            RocksWalBuilder::with_default_rocksdb_config(wal_path, write_runtime.clone())
                .build()
                .context(OpenWal)?;

        let manifest_path = data_path.join(MANIFEST_DIR_NAME);
        let manifest_wal =
            RocksWalBuilder::with_default_rocksdb_config(manifest_path, write_runtime)
                .build()
                .context(OpenManifestWal)?;
        let opened_wals = OpenedWals {
            data_wal: Arc::new(data_wal),
            manifest_wal: Arc::new(manifest_wal),
        };
        Ok(opened_wals)
    }
}

/// [ReplicatedEngine] builder.
#[derive(Default)]
pub struct ObkvWalsOpener;

#[async_trait]
impl WalsOpener for ObkvWalsOpener {
    async fn open_wals(
        &self,
        config: &WalStorageConfig,
        engine_runtimes: Arc<EngineRuntimes>,
    ) -> Result<OpenedWals> {
        let obkv_wal_config = match config {
            WalStorageConfig::Obkv(config) => config.clone(),
            _ => {
                return InvalidWalConfig {
                    msg: format!(
                        "invalid wal storage config while opening obkv wal, config:{config:?}"
                    ),
                }
                .fail();
            }
        };

        // Notice the creation of obkv client may block current thread.
        let obkv_config = obkv_wal_config.obkv.clone();
        let obkv = engine_runtimes
            .write_runtime
            .spawn_blocking(move || ObkvImpl::new(obkv_config).context(OpenObkv))
            .await
            .context(RuntimeExec)??;

        open_wal_and_manifest_with_table_kv(*obkv_wal_config, engine_runtimes, obkv).await
    }
}

/// [MemWalEngine] builder.
///
/// All engine built by this builder share same [MemoryImpl] instance, so the
/// data wrote by the engine still remains after the engine dropped.
#[derive(Default)]
pub struct MemWalsOpener {
    table_kv: MemoryImpl,
}

#[async_trait]
impl WalsOpener for MemWalsOpener {
    async fn open_wals(
        &self,
        config: &WalStorageConfig,
        engine_runtimes: Arc<EngineRuntimes>,
    ) -> Result<OpenedWals> {
        let obkv_wal_config = match config {
            WalStorageConfig::Obkv(config) => config.clone(),
            _ => {
                return InvalidWalConfig {
                    msg: format!(
                        "invalid wal storage config while opening memory wal, config:{config:?}"
                    ),
                }
                .fail();
            }
        };

        open_wal_and_manifest_with_table_kv(
            *obkv_wal_config,
            engine_runtimes,
            self.table_kv.clone(),
        )
        .await
    }
}

#[derive(Default)]
pub struct KafkaWalsOpener;

#[async_trait]
impl WalsOpener for KafkaWalsOpener {
    async fn open_wals(
        &self,
        config: &WalStorageConfig,
        engine_runtimes: Arc<EngineRuntimes>,
    ) -> Result<OpenedWals> {
        let kafka_wal_config = match config {
            WalStorageConfig::Kafka(config) => config.clone(),
            _ => {
                return InvalidWalConfig {
                    msg: format!(
                        "invalid wal storage config while opening kafka wal, config:{config:?}"
                    ),
                }
                .fail();
            }
        };

        let bg_runtime = &engine_runtimes.bg_runtime;

        let kafka = KafkaImpl::new(kafka_wal_config.kafka.clone())
            .await
            .context(OpenKafka)?;
        let data_wal = MessageQueueImpl::new(
            WAL_DIR_NAME.to_string(),
            kafka.clone(),
            bg_runtime.clone(),
            kafka_wal_config.data_namespace,
        );

        let manifest_wal = MessageQueueImpl::new(
            MANIFEST_DIR_NAME.to_string(),
            kafka,
            bg_runtime.clone(),
            kafka_wal_config.meta_namespace,
        );

        Ok(OpenedWals {
            data_wal: Arc::new(data_wal),
            manifest_wal: Arc::new(manifest_wal),
        })
    }
}

async fn open_wal_and_manifest_with_table_kv<T: TableKv>(
    config: ObkvWalConfig,
    engine_runtimes: Arc<EngineRuntimes>,
    table_kv: T,
) -> Result<OpenedWals> {
    let runtimes = WalRuntimes {
        read_runtime: engine_runtimes.read_runtime.clone(),
        write_runtime: engine_runtimes.write_runtime.clone(),
        bg_runtime: engine_runtimes.bg_runtime.clone(),
    };

    let data_wal = WalNamespaceImpl::open(
        table_kv.clone(),
        runtimes.clone(),
        WAL_DIR_NAME,
        config.data_namespace.clone().into(),
    )
    .await
    .context(OpenWal)?;

    let manifest_wal = WalNamespaceImpl::open(
        table_kv,
        runtimes,
        MANIFEST_DIR_NAME,
        config.meta_namespace.clone().into(),
    )
    .await
    .context(OpenManifestWal)?;

    Ok(OpenedWals {
        data_wal: Arc::new(data_wal),
        manifest_wal: Arc::new(manifest_wal),
    })
}

async fn open_instance(
    config: Config,
    engine_runtimes: Arc<EngineRuntimes>,
    wal_manager: WalManagerRef,
    manifest: ManifestRef,
    store_picker: ObjectStorePickerRef,
    router: Option<RouterRef>,
) -> Result<InstanceRef> {
    let remote_engine_ref: Option<RemoteEngineRef> = if let Some(v) = router {
        Some(Arc::new(RemoteEngineImpl::new(
            config.remote_engine_client.clone(),
            v,
        )))
    } else {
        None
    };

    let meta_cache: Option<MetaCacheRef> = config
        .sst_meta_cache_cap
        .map(|cap| Arc::new(MetaCache::new(cap)));

    let open_ctx = OpenContext {
        config,
        runtimes: engine_runtimes,
        meta_cache,
    };

    let instance = Instance::open(
        open_ctx,
        manifest,
        wal_manager,
        store_picker,
        Arc::new(FactoryImpl::default()),
        remote_engine_ref,
    )
    .await
    .context(OpenInstance)?;
    Ok(instance)
}

#[derive(Debug)]
struct OpenedStorages {
    default_store: ObjectStoreRef,
    store_with_readonly_cache: ObjectStoreRef,
}

impl ObjectStorePicker for OpenedStorages {
    fn default_store(&self) -> &ObjectStoreRef {
        &self.default_store
    }

    fn pick_by_freq(&self, freq: ReadFrequency) -> &ObjectStoreRef {
        match freq {
            ReadFrequency::Once => &self.store_with_readonly_cache,
            ReadFrequency::Frequent => &self.default_store,
        }
    }
}

// Build store in multiple layer, access speed decrease in turn.
// MemCacheStore           → DiskCacheStore → real ObjectStore(OSS/S3...)
// MemCacheStore(ReadOnly) ↑
// ```plaintext
// +-------------------------------+
// |    MemCacheStore              |
// |       +-----------------------+
// |       |    DiskCacheStore     |
// |       |      +----------------+
// |       |      |                |
// |       |      |    OSS/S3....  |
// +-------+------+----------------+
// ```
fn open_storage(
    opts: StorageOptions,
) -> Pin<Box<dyn Future<Output = Result<OpenedStorages>> + Send>> {
    Box::pin(async move {
        let mut store = match opts.object_store {
            ObjectStoreOptions::Local(local_opts) => {
                let data_path = Path::new(&local_opts.data_dir);
                let sst_path = data_path.join(STORE_DIR_NAME);
                tokio::fs::create_dir_all(&sst_path)
                    .await
                    .context(CreateDir {
                        path: sst_path.to_string_lossy().into_owned(),
                    })?;
                let store = LocalFileSystem::new_with_prefix(sst_path).context(OpenObjectStore)?;
                Arc::new(store) as _
            }
            ObjectStoreOptions::Aliyun(aliyun_opts) => {
                let oss: ObjectStoreRef = Arc::new(
                    aliyun::try_new(
                        aliyun_opts.key_id,
                        aliyun_opts.key_secret,
                        aliyun_opts.endpoint,
                        aliyun_opts.bucket,
                        aliyun_opts.pool_max_idle_per_host,
                        aliyun_opts.timeout.0,
                    )
                    .context(OpenObjectStore)?,
                );
                let oss_with_metrics = Arc::new(StoreWithMetrics::new(oss));
                Arc::new(
                    StoreWithPrefix::new(aliyun_opts.prefix, oss_with_metrics)
                        .context(OpenObjectStore)?,
                ) as _
            }
        };

        if opts.disk_cache_capacity.as_byte() > 0 {
            let path = Path::new(&opts.disk_cache_dir).join(DISK_CACHE_DIR_NAME);
            tokio::fs::create_dir_all(&path).await.context(CreateDir {
                path: path.to_string_lossy().into_owned(),
            })?;

            store = Arc::new(
                DiskCacheStore::try_new(
                    path.to_string_lossy().into_owned(),
                    opts.disk_cache_capacity.as_byte() as usize,
                    opts.disk_cache_page_size.as_byte() as usize,
                    store,
                )
                .await
                .context(OpenObjectStore)?,
            ) as _;
        }

        if opts.mem_cache_capacity.as_byte() > 0 {
            let mem_cache = Arc::new(
                MemCache::try_new(
                    opts.mem_cache_partition_bits,
                    NonZeroUsize::new(opts.mem_cache_capacity.as_byte() as usize).unwrap(),
                )
                .context(OpenMemCache)?,
            );
            let default_store = Arc::new(MemCacheStore::new(mem_cache.clone(), store.clone())) as _;
            let store_with_readonly_cache =
                Arc::new(MemCacheStore::new_with_readonly_cache(mem_cache, store)) as _;
            Ok(OpenedStorages {
                default_store,
                store_with_readonly_cache,
            })
        } else {
            let store_with_readonly_cache = store.clone();
            Ok(OpenedStorages {
                default_store: store,
                store_with_readonly_cache,
            })
        }
    })
}
