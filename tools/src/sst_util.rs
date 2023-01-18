// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use analytic_engine::sst::{parquet::encoding, writer::MetaData};
use object_store::{ObjectStoreRef, Path};
use parquet::file::footer;

/// Extract the meta data from the sst file.
pub async fn meta_from_sst(store: &ObjectStoreRef, sst_path: &Path) -> MetaData {
    let get_result = store.get(sst_path).await.unwrap();
    let chunk_reader = get_result.bytes().await.unwrap();
    let metadata = footer::parse_metadata(&chunk_reader).unwrap();
    let kv_metas = metadata.file_metadata().key_value_metadata().unwrap();

    let parquet_meta_data = encoding::decode_sst_meta_data(&kv_metas[0]).unwrap();
    MetaData::from(parquet_meta_data)
}
