// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use analytic_engine::sst::{parquet::encoding, writer::MetaData};
use object_store::{ObjectStoreRef, Path};
use parquet::file::footer;

/// Extract the meta data from the sst file.
pub async fn meta_from_sst(store: &ObjectStoreRef, sst_path: &Path) -> MetaData {
    let get_result = store.get(sst_path).await.unwrap();
    let chunk_reader = get_result.bytes().await.unwrap();
    let metadata = footer::parse_metadata(&chunk_reader).unwrap();

    let file_meta_data = metadata.file_metadata();
    let kv_metas = file_meta_data.key_value_metadata().unwrap();
    let kv_meta = kv_metas
        .iter()
        .find(|kv| kv.key == encoding::META_KEY)
        .unwrap();

    let parquet_meta_data = encoding::decode_sst_meta_data(kv_meta).unwrap();
    MetaData::from(parquet_meta_data)
}
