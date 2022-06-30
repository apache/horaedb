// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Setup the analytic engine

use std::{path::Path, sync::Arc};

use async_trait::async_trait;
use common_util::define_result;
use object_store::{aliyun::AliyunOSS, LocalFileSystem, ObjectStore};
use parquet::{
    cache::{LruDataCache, LruMetaCache},
    DataCacheRef, MetaCacheRef,
};
use snafu::{ResultExt, Snafu};
use table_engine::engine::{EngineRuntimes, TableEngineRef};
use table_kv::{memory::MemoryImpl, obkv::ObkvImpl, TableKv};
use wal::{
    manager::{self, WalManager},
    rocks_impl::manager::Builder as WalBuilder,
    table_kv_impl::{wal::WalNamespaceImpl, WalRuntimes},
};

use crate::{
    context::OpenContext,
    engine::{ReplicatedInstanceRef, RocksInstanceRef, TableEngineImpl},
    instance::{Instance, InstanceRef},
    meta::{details::ManifestImpl, Manifest},
    sst::factory::FactoryImpl,
    storage_options::{AliyunOptions, LocalOptions},
    Config,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to open engine instance, err:{}", source))]
    OpenInstance {
        source: crate::instance::engine::Error,
    },

    #[snafu(display("Failed to open wal, err:{}", source))]
    OpenWal { source: manager::error::Error },

    #[snafu(display("Failed to open wal for manifest, err:{}", source))]
    OpenManifestWal { source: manager::error::Error },

    #[snafu(display("Failed to open manifest, err:{}", source))]
    OpenManifest { source: crate::meta::details::Error },

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
}

define_result!(Error);

const WAL_DIR_NAME: &str = "wal";
const MANIFEST_DIR_NAME: &str = "manifest";
const STORE_DIR_NAME: &str = "store";

type InstanceRefOnTableKv<T, Store> =
    InstanceRef<WalNamespaceImpl<T>, ManifestImpl<WalNamespaceImpl<T>>, Store, FactoryImpl>;

/// Analytic engine builder.
#[async_trait]
pub trait EngineBuilder: Default {
    /// Build the analytic engine from `config` and `engine_runtimes`.
    async fn build(
        &self,
        config: Config,
        engine_runtimes: Arc<EngineRuntimes>,
    ) -> Result<TableEngineRef>;
}

/// [RocksEngine] builder.
#[derive(Default)]
pub struct RocksEngineBuilder;

#[async_trait]
impl EngineBuilder for RocksEngineBuilder {
    async fn build(
        &self,
        config: Config,
        engine_runtimes: Arc<EngineRuntimes>,
    ) -> Result<TableEngineRef> {
        assert!(!config.obkv_wal.enable);

        match config.storage {
            crate::storage_options::StorageOptions::Local(ref opts) => {
                let store = open_storage_local(opts.clone()).await?;
                let instance = open_rocks_instance(config.clone(), engine_runtimes, store).await?;
                Ok(Arc::new(TableEngineImpl::new(instance)))
            }
            crate::storage_options::StorageOptions::Aliyun(ref opts) => {
                let store = open_storage_aliyun(opts.clone()).await?;
                let instance = open_rocks_instance(config.clone(), engine_runtimes, store).await?;
                Ok(Arc::new(TableEngineImpl::new(instance)))
            }
        }
    }
}

/// [ReplicatedEngine] builder.
#[derive(Default)]
pub struct ReplicatedEngineBuilder;

#[async_trait]
impl EngineBuilder for ReplicatedEngineBuilder {
    async fn build(
        &self,
        config: Config,
        engine_runtimes: Arc<EngineRuntimes>,
    ) -> Result<TableEngineRef> {
        assert!(config.obkv_wal.enable);

        match config.storage {
            crate::storage_options::StorageOptions::Local(ref opts) => {
                let store = open_storage_local(opts.clone()).await?;
                let instance =
                    open_replicated_instance(config.clone(), engine_runtimes, store).await?;
                Ok(Arc::new(TableEngineImpl::new(instance)))
            }
            crate::storage_options::StorageOptions::Aliyun(ref opts) => {
                let store = open_storage_aliyun(opts.clone()).await?;
                let instance =
                    open_replicated_instance(config.clone(), engine_runtimes, store).await?;
                Ok(Arc::new(TableEngineImpl::new(instance)))
            }
        }
    }
}

/// [MemWalEngine] builder.
///
/// All engine built by this builder share same [MemoryImpl] instance, so the
/// data wrote by the engine still remains after the engine dropped.
#[derive(Default)]
pub struct MemWalEngineBuilder {
    table_kv: MemoryImpl,
}

#[async_trait]
impl EngineBuilder for MemWalEngineBuilder {
    async fn build(
        &self,
        config: Config,
        engine_runtimes: Arc<EngineRuntimes>,
    ) -> Result<TableEngineRef> {
        match config.storage {
            crate::storage_options::StorageOptions::Local(ref opts) => {
                let store = open_storage_local(opts.clone()).await?;
                let instance = open_instance_with_table_kv(
                    config.clone(),
                    engine_runtimes,
                    self.table_kv.clone(),
                    store,
                )
                .await?;
                Ok(Arc::new(TableEngineImpl::new(instance)))
            }
            crate::storage_options::StorageOptions::Aliyun(ref opts) => {
                let store = open_storage_aliyun(opts.clone()).await?;
                let instance = open_instance_with_table_kv(
                    config.clone(),
                    engine_runtimes,
                    self.table_kv.clone(),
                    store,
                )
                .await?;
                Ok(Arc::new(TableEngineImpl::new(instance)))
            }
        }
    }
}

async fn open_rocks_instance<Store>(
    config: Config,
    engine_runtimes: Arc<EngineRuntimes>,
    store: Store,
) -> Result<RocksInstanceRef<Store>>
where
    Store: ObjectStore,
{
    let write_runtime = engine_runtimes.write_runtime.clone();
    let data_path = Path::new(&config.wal_path);
    let wal_path = data_path.join(WAL_DIR_NAME);
    let wal_manager = WalBuilder::with_default_rocksdb_config(wal_path, write_runtime.clone())
        .build()
        .context(OpenWal)?;

    let manifest_path = data_path.join(MANIFEST_DIR_NAME);
    let manifest_wal = WalBuilder::with_default_rocksdb_config(manifest_path, write_runtime)
        .build()
        .context(OpenManifestWal)?;

    let manifest = ManifestImpl::open(manifest_wal, config.manifest.clone())
        .await
        .context(OpenManifest)?;

    let instance =
        open_with_wal_manifest(config, engine_runtimes, wal_manager, manifest, store).await?;

    Ok(instance)
}

async fn open_replicated_instance<Store>(
    config: Config,
    engine_runtimes: Arc<EngineRuntimes>,
    store: Store,
) -> Result<ReplicatedInstanceRef<Store>>
where
    Store: ObjectStore,
{
    assert!(config.obkv_wal.enable);

    // Notice the creation of obkv client may block current thread.
    let obkv_config = config.obkv_wal.obkv.clone();
    let obkv = engine_runtimes
        .write_runtime
        .spawn_blocking(move || ObkvImpl::new(obkv_config).context(OpenObkv))
        .await
        .context(RuntimeExec)??;

    open_instance_with_table_kv(config, engine_runtimes, obkv, store).await
}

async fn open_instance_with_table_kv<T: TableKv, Store>(
    config: Config,
    engine_runtimes: Arc<EngineRuntimes>,
    table_kv: T,
    store: Store,
) -> Result<InstanceRefOnTableKv<T, Store>>
where
    Store: ObjectStore,
{
    let runtimes = WalRuntimes {
        read_runtime: engine_runtimes.read_runtime.clone(),
        write_runtime: engine_runtimes.write_runtime.clone(),
        bg_runtime: engine_runtimes.bg_runtime.clone(),
    };

    let wal_manager = WalNamespaceImpl::open(
        table_kv.clone(),
        runtimes.clone(),
        WAL_DIR_NAME,
        config.obkv_wal.wal.clone(),
    )
    .await
    .context(OpenWal)?;

    let manifest_wal = WalNamespaceImpl::open(
        table_kv,
        runtimes.clone(),
        MANIFEST_DIR_NAME,
        config.obkv_wal.manifest.clone(),
    )
    .await
    .context(OpenManifestWal)?;
    let manifest = ManifestImpl::open(manifest_wal, config.manifest.clone())
        .await
        .context(OpenManifest)?;

    let instance =
        open_with_wal_manifest(config, engine_runtimes, wal_manager, manifest, store).await?;
    Ok(instance)
}

async fn open_with_wal_manifest<Wal, Meta, Store>(
    config: Config,
    engine_runtimes: Arc<EngineRuntimes>,
    wal_manager: Wal,
    manifest: Meta,
    store: Store,
) -> Result<InstanceRef<Wal, Meta, Store, FactoryImpl>>
where
    Wal: WalManager + Send + Sync + 'static,
    Meta: Manifest + Send + Sync + 'static,
    Store: ObjectStore,
{
    let meta_cache: Option<MetaCacheRef> =
        if let Some(sst_meta_cache_cap) = &config.sst_meta_cache_cap {
            Some(Arc::new(LruMetaCache::new(*sst_meta_cache_cap)))
        } else {
            None
        };

    let data_cache: Option<DataCacheRef> =
        if let Some(sst_data_cache_cap) = &config.sst_data_cache_cap {
            Some(Arc::new(LruDataCache::new(*sst_data_cache_cap)))
        } else {
            None
        };

    let open_ctx = OpenContext {
        config,
        runtimes: engine_runtimes,
        meta_cache,
        data_cache,
    };

    let instance = Instance::open(open_ctx, manifest, wal_manager, store, FactoryImpl)
        .await
        .context(OpenInstance)?;
    Ok(instance)
}

async fn open_storage_local(opts: LocalOptions) -> Result<LocalFileSystem> {
    let data_path = Path::new(&opts.data_path);
    let sst_path = data_path.join(STORE_DIR_NAME);
    tokio::fs::create_dir_all(&sst_path)
        .await
        .context(CreateDir {
            path: sst_path.to_string_lossy().into_owned(),
        })?;
    LocalFileSystem::new_with_prefix(sst_path).context(OpenObjectStore)
}

async fn open_storage_aliyun(opts: AliyunOptions) -> Result<impl ObjectStore> {
    Ok(AliyunOSS::new(
        opts.key_id,
        opts.key_secret,
        opts.endpoint,
        opts.bucket,
    ))
}
