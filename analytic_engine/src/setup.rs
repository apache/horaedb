// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Setup the analytic engine

use std::{path::Path, sync::Arc};

use common_util::{define_result, runtime::Runtime};
use object_store::{aliyun::AliyunOSS, LocalFileSystem, ObjectStore};
use parquet::{
    cache::{LruDataCache, LruMetaCache},
    DataCacheRef, MetaCacheRef,
};
use snafu::{ResultExt, Snafu};
use table_engine::engine::{EngineRuntimes, TableEngineRef};
use wal::{
    manager::{self, WalManager},
    rocks_impl::manager::Builder as WalBuilder,
};

use crate::{
    context::OpenContext,
    engine::TableEngineImpl,
    instance::{Instance, InstanceRef},
    meta::{details::ManifestImpl, Manifest},
    sst::factory::{Factory, FactoryImpl},
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

/// Open an [AnalyticTableEngine] instance
pub async fn open_analytic_table_engine(
    config: Config,
    engine_runtimes: Arc<EngineRuntimes>,
) -> Result<TableEngineRef> {
    let wal = open_wal(
        config.clone(),
        engine_runtimes.write_runtime.clone(),
        WAL_DIR_NAME,
    )
    .await?;
    let manifest_wal = open_wal(
        config.clone(),
        engine_runtimes.write_runtime.clone(),
        MANIFEST_DIR_NAME,
    )
    .await?;
    let manifest = open_manifest(config.clone(), manifest_wal).await?;

    match config.storage {
        crate::storage_options::StorageOptions::Local(ref opts) => {
            let storage = open_storage_local(opts.clone()).await?;
            let instance =
                open_instance(config, wal, manifest, storage, FactoryImpl, engine_runtimes).await?;
            Ok(Arc::new(TableEngineImpl::new(instance)))
        }
        crate::storage_options::StorageOptions::Aliyun(ref opts) => {
            let storage = open_storage_aliyun(opts.clone()).await?;
            let instance =
                open_instance(config, wal, manifest, storage, FactoryImpl, engine_runtimes).await?;
            Ok(Arc::new(TableEngineImpl::new(instance)))
        }
    }
}

async fn open_instance<Wal, M, Store, Fa>(
    config: Config,
    wal: Wal,
    manifest: M,
    storage: Store,
    factory: Fa,
    engine_runtimes: Arc<EngineRuntimes>,
) -> Result<InstanceRef<Wal, M, Store, Fa>>
where
    Wal: WalManager + Send + Sync + 'static,
    M: Manifest + Send + Sync + 'static,
    Store: ObjectStore,
    Fa: Factory + Send + Sync + 'static,
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

    let instance = Instance::open(open_ctx, manifest, wal, storage, factory)
        .await
        .context(OpenInstance)?;

    Ok(instance)
}

async fn open_wal(
    config: Config,
    runtime: Arc<Runtime>,
    sub_path: &str,
) -> Result<impl WalManager + Send + Sync + 'static> {
    let data_path = Path::new(&config.wal_path);
    let wal_path = data_path.join(sub_path);
    WalBuilder::with_default_rocksdb_config(wal_path, runtime)
        .build()
        .context(OpenWal)
}

async fn open_manifest<WAL>(
    config: Config,
    wal: WAL,
) -> Result<impl Manifest + Send + Sync + 'static>
where
    WAL: WalManager + Send + Sync + 'static,
{
    ManifestImpl::open(wal, config.manifest.clone())
        .await
        .context(OpenManifest)
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
