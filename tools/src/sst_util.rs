// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

    let parquet_meta_data = encoding::decode_sst_meta_data_from_kv(kv_meta).unwrap();
    MetaData::from(parquet_meta_data)
}
