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

use std::sync::Arc;

use async_trait::async_trait;
use macros::define_result;
use object_store::{ObjectStoreRef, Path};
use parquet::{data_type::AsBytes, file::metadata::KeyValue};
use snafu::{ensure, OptionExt, ResultExt};

use super::UnknownMetaVersion;
use crate::sst::{
    meta_data::{
        DecodeCustomMetaData, FetchAndDecodeSstMeta, FetchFromStore, KvMetaDataNotFound,
        KvMetaPathEmpty,
    },
    parquet::{
        encoding::{self, decode_sst_meta_data_v2, META_VERSION_CURRENT, META_VERSION_V1},
        meta_data::{ParquetMetaData, ParquetMetaDataRef},
    },
};

define_result!(super::Error);

#[async_trait]
pub trait CustomMetadataReader {
    async fn get_metadata(&self) -> Result<ParquetMetaData>;
}

pub struct MetaV1Reader<'a> {
    custom_kv_meta: Option<&'a KeyValue>,
}

impl<'a> MetaV1Reader<'a> {
    fn new(custom_kv_meta: Option<&'a KeyValue>) -> Self {
        Self { custom_kv_meta }
    }
}

#[async_trait]
impl CustomMetadataReader for MetaV1Reader<'_> {
    async fn get_metadata(&self) -> Result<ParquetMetaData> {
        let custom_kv_meta = self.custom_kv_meta.context(KvMetaDataNotFound)?;

        encoding::decode_sst_meta_data_v1(custom_kv_meta).context(DecodeCustomMetaData)
    }
}

pub struct MetaV2Reader {
    meta_path: Option<Path>,
    store: ObjectStoreRef,
}

impl MetaV2Reader {
    fn new(meta_path: Option<Path>, store: ObjectStoreRef) -> Self {
        Self { meta_path, store }
    }
}

#[async_trait]
impl CustomMetadataReader for MetaV2Reader {
    async fn get_metadata(&self) -> Result<ParquetMetaData> {
        match &self.meta_path {
            None => KvMetaPathEmpty {}.fail(),
            Some(meta_path) => {
                let metadata = self
                    .store
                    .get(meta_path)
                    .await
                    .with_context(|| FetchFromStore {
                        file_path: meta_path.to_string(),
                    })?
                    .bytes()
                    .await
                    .with_context(|| FetchAndDecodeSstMeta {
                        file_path: meta_path.to_string(),
                    })?;

                decode_sst_meta_data_v2(metadata.as_bytes()).context(DecodeCustomMetaData)
            }
        }
    }
}

pub async fn parse_metadata(
    meta_version: &str,
    custom_kv_meta: Option<&KeyValue>,
    ignore_sst_filter: bool,
    meta_path: Option<Path>,
    store: ObjectStoreRef,
) -> Result<ParquetMetaDataRef> {
    // Must ensure custom metadata only store in one place
    ensure!(
        custom_kv_meta.is_none() || meta_path.is_none(),
        KvMetaDataNotFound
    );

    let reader: Box<dyn CustomMetadataReader + Send + Sync + '_> = match meta_version {
        META_VERSION_V1 => Box::new(MetaV1Reader::new(custom_kv_meta)),
        META_VERSION_CURRENT => Box::new(MetaV2Reader::new(meta_path, store)),
        _ => {
            return UnknownMetaVersion {
                version: meta_version,
            }
            .fail()
        }
    };
    let mut metadata = reader.get_metadata().await?;
    if ignore_sst_filter {
        metadata.parquet_filter = None;
    }

    Ok(Arc::new(metadata))
}
