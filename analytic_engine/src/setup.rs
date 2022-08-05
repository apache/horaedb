// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Setup the analytic engine

use std::{path::Path, pin::Pin, sync::Arc};

use async_trait::async_trait;
use common_util::define_result;
use futures::Future;
use object_store::{aliyun::AliyunOSS, cache::CachedStore, LocalFileSystem, ObjectStoreRef};
use parquet::{
    cache::{LruDataCache, LruMetaCache},
    DataCacheRef, MetaCacheRef,
};
use snafu::{ResultExt, Snafu};
use table_engine::engine::{EngineRuntimes, TableEngineRef};
use table_kv::{memory::MemoryImpl, obkv::ObkvImpl, TableKv};
use wal::{
    manager::{self, WalManagerRef},
    rocks_impl::manager::Builder as WalBuilder,
    table_kv_impl::{wal::WalNamespaceImpl, WalRuntimes},
};

use crate::{
    context::OpenContext,
    engine::TableEngineImpl,
    instance::{Instance, InstanceRef},
    meta::{details::ManifestImpl, ManifestRef},
    sst::factory::FactoryImpl,
    storage_options::StorageOptions,
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

/// Analytic engine builder.
#[async_trait]
pub trait EngineBuilder: Send + Sync + Default {
    /// Build the analytic engine from `config` and `engine_runtimes`.
    async fn build(
        &self,
        config: Config,
        engine_runtimes: Arc<EngineRuntimes>,
    ) -> Result<TableEngineRef> {
        let (wal, manifest) = self
            .open_wal_and_manifest(config.clone(), engine_runtimes.clone())
            .await?;
        let store = open_storage(config.storage.clone()).await?;
        let instance = open_instance(config.clone(), engine_runtimes, wal, manifest, store).await?;
        Ok(Arc::new(TableEngineImpl::new(instance)))
    }

    async fn open_wal_and_manifest(
        &self,
        config: Config,
        engine_runtimes: Arc<EngineRuntimes>,
    ) -> Result<(WalManagerRef, ManifestRef)>;
}

/// [RocksEngine] builder.
#[derive(Default)]
pub struct RocksEngineBuilder;

#[async_trait]
impl EngineBuilder for RocksEngineBuilder {
    async fn open_wal_and_manifest(
        &self,
        config: Config,
        engine_runtimes: Arc<EngineRuntimes>,
    ) -> Result<(WalManagerRef, ManifestRef)> {
        assert!(!config.obkv_wal.enable);

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

        let manifest = ManifestImpl::open(Arc::new(manifest_wal), config.manifest.clone())
            .await
            .context(OpenManifest)?;

        Ok((Arc::new(wal_manager), Arc::new(manifest)))
    }
}

/// [ReplicatedEngine] builder.
#[derive(Default)]
pub struct ReplicatedEngineBuilder;

#[async_trait]
impl EngineBuilder for ReplicatedEngineBuilder {
    async fn open_wal_and_manifest(
        &self,
        config: Config,
        engine_runtimes: Arc<EngineRuntimes>,
    ) -> Result<(WalManagerRef, ManifestRef)> {
        assert!(config.obkv_wal.enable);

        // Notice the creation of obkv client may block current thread.
        let obkv_config = config.obkv_wal.obkv.clone();
        let obkv = engine_runtimes
            .write_runtime
            .spawn_blocking(move || ObkvImpl::new(obkv_config).context(OpenObkv))
            .await
            .context(RuntimeExec)??;

        open_wal_and_manifest_with_table_kv(config, engine_runtimes, obkv).await
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
    async fn open_wal_and_manifest(
        &self,
        config: Config,
        engine_runtimes: Arc<EngineRuntimes>,
    ) -> Result<(WalManagerRef, ManifestRef)> {
        open_wal_and_manifest_with_table_kv(config, engine_runtimes, self.table_kv.clone()).await
    }
}

async fn open_wal_and_manifest_with_table_kv<T: TableKv>(
    config: Config,
    engine_runtimes: Arc<EngineRuntimes>,
    table_kv: T,
) -> Result<(WalManagerRef, ManifestRef)> {
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
        runtimes,
        MANIFEST_DIR_NAME,
        config.obkv_wal.manifest.clone(),
    )
    .await
    .context(OpenManifestWal)?;
    let manifest = ManifestImpl::open(Arc::new(manifest_wal), config.manifest.clone())
        .await
        .context(OpenManifest)?;

    Ok((Arc::new(wal_manager), Arc::new(manifest)))
}

async fn open_instance(
    config: Config,
    engine_runtimes: Arc<EngineRuntimes>,
    wal_manager: WalManagerRef,
    manifest: ManifestRef,
    store: ObjectStoreRef,
) -> Result<InstanceRef> {
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

    let instance = Instance::open(
        open_ctx,
        manifest,
        wal_manager,
        store,
        Arc::new(FactoryImpl::default()),
    )
    .await
    .context(OpenInstance)?;
    Ok(instance)
}

fn open_storage(
    opts: StorageOptions,
) -> Pin<Box<dyn Future<Output = Result<ObjectStoreRef>> + Send>> {
    Box::pin(async move {
        match opts {
            StorageOptions::Local(local_opts) => {
                let data_path = Path::new(&local_opts.data_path);
                let sst_path = data_path.join(STORE_DIR_NAME);
                tokio::fs::create_dir_all(&sst_path)
                    .await
                    .context(CreateDir {
                        path: sst_path.to_string_lossy().into_owned(),
                    })?;
                let store = LocalFileSystem::new_with_prefix(sst_path).context(OpenObjectStore)?;
                Ok(Arc::new(store) as _)
            }
            StorageOptions::Aliyun(aliyun_opts) => Ok(Arc::new(AliyunOSS::new(
                aliyun_opts.key_id,
                aliyun_opts.key_secret,
                aliyun_opts.endpoint,
                aliyun_opts.bucket,
            )) as _),
            StorageOptions::Cache(cache_opts) => {
                let local_store = open_storage(*cache_opts.local_store).await?;
                let remote_store = open_storage(*cache_opts.remote_store).await?;
                let store = CachedStore::init(local_store, remote_store, cache_opts.cache_opts)
                    .await
                    .context(OpenObjectStore)?;
                Ok(Arc::new(store) as _)
            }
        }
    })
}
