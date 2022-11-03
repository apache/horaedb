// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use analytic_engine::sst::{file::SstMetaData, parquet::reader};
use object_store::{ObjectStoreRef, Path};
use parquet::file::footer;

/// Extract the meta data from the sst file.
pub async fn meta_from_sst(store: &ObjectStoreRef, sst_path: &Path) -> SstMetaData {
    let chunk_reader = reader::make_sst_chunk_reader(store, sst_path)
        .await
        .unwrap();
    let meta_data = footer::parse_metadata(&chunk_reader).unwrap();
    reader::read_sst_meta(&meta_data).unwrap()
}
