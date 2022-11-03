// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use analytic_engine::sst::{file::SstMetaData, parquet::reader};
use object_store::{ObjectStoreRef, Path};
use parquet_ext::{DataCacheRef, MetaCacheRef};

pub async fn meta_from_sst(
    store: &ObjectStoreRef,
    sst_path: &Path,
    meta_cache: &Option<MetaCacheRef>,
    data_cache: &Option<DataCacheRef>,
) -> SstMetaData {
    let (_, sst_meta) = reader::read_sst_meta(store, sst_path, meta_cache, data_cache)
        .await
        .unwrap();

    sst_meta
}
