// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Setup the analytic engine

use std::{path::Path, sync::Arc};

use common_util::define_result;
use object_store::LocalFileSystem;
use parquet::{
    cache::{LruDataCache, LruMetaCache},
    DataCacheRef, MetaCacheRef,
};
use snafu::{ResultExt, Snafu};
use table_engine::engine::EngineRuntimes;
use wal::{manager, rocks_impl::manager::Builder as WalBuilder};

use crate::{
    context::OpenContext, engine::TableEngineImpl, instance::Instance, meta::details::ManifestImpl,
    sst::factory::FactoryImpl, AnalyticTableEngine, Config, EngineInstance,
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
) -> Result<AnalyticTableEngine> {
    let instance = open_instance(config.clone(), engine_runtimes).await?;

    Ok(TableEngineImpl::new(instance))
}

async fn open_instance(
    config: Config,
    engine_runtimes: Arc<EngineRuntimes>,
) -> Result<EngineInstance> {
    let write_runtime = engine_runtimes.write_runtime.clone();
    let data_path = Path::new(&config.data_path);
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

    let sst_path = data_path.join(STORE_DIR_NAME);
    tokio::fs::create_dir_all(&sst_path)
        .await
        .context(CreateDir {
            path: sst_path.to_string_lossy().into_owned(),
        })?;
    let store = LocalFileSystem::new_with_prefix(sst_path).context(OpenObjectStore)?;
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
