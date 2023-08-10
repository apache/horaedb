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

use snafu::{OptionExt, ResultExt};

use super::{
    DecodeCustomMetaData, FetchAndDecodeSstMeta, FetchFromStore, KvMetaDataNotFound,
    KvMetaPathEmpty, MetaPathVersionWrong,
};
use crate::sst::parquet::{
    encoding::{self, decode_sst_custom_meta_data, META_PATH_VERSION},
    meta_data::ParquetMetaData,
};

define_result!(super::Error);

#[async_trait]
pub trait CustomMetadataReader {
    async fn get_metadata(&self) -> Result<Arc<ParquetMetaData>>;
}

pub struct CustomMetadataReaderBuilder;

pub struct MetaPathV1Reader<'a> {
    custom_kv_meta: Option<&'a KeyValue>,
    ignore_sst_filter: bool,
}

impl<'a> MetaPathV1Reader<'a> {
    fn new(custom_kv_meta: Option<&'a KeyValue>, ignore_sst_filter: bool) -> Self {
        Self {
            custom_kv_meta,
            ignore_sst_filter,
        }
    }
}

#[async_trait]
impl CustomMetadataReader for MetaPathV1Reader<'_> {
    async fn get_metadata(&self) -> Result<Arc<ParquetMetaData>> {
        let custom_kv_meta = self.custom_kv_meta.context(KvMetaDataNotFound)?;
        let mut sst_meta =
            encoding::decode_sst_meta_data(custom_kv_meta).context(DecodeCustomMetaData)?;
        if self.ignore_sst_filter {
            sst_meta.parquet_filter = None;
        }
        Ok(Arc::new(sst_meta))
    }
}

pub struct MetaPathV2Reader {
    meta_path: Option<Path>,
    store: ObjectStoreRef,
}

impl MetaPathV2Reader {
    fn new(meta_path: Option<Path>, store: ObjectStoreRef) -> Self {
        Self { meta_path, store }
    }
}

#[async_trait]
impl CustomMetadataReader for MetaPathV2Reader {
    async fn get_metadata(&self) -> Result<Arc<ParquetMetaData>> {
        let decode_custom_metadata = match &self.meta_path {
            Some(meta_path) => {
                let metadata = self
                    .store
                    .get(meta_path)
                    .await
                    .with_context(|| FetchAndDecodeSstMeta {
                        file_path: meta_path.to_string(),
                    })?
                    .bytes()
                    .await
                    .with_context(|| FetchFromStore {
                        file_path: meta_path.to_string(),
                    })?;

                Some(
                    decode_sst_custom_meta_data(metadata.as_bytes())
                        .context(DecodeCustomMetaData)?,
                )
            }
            None => return KvMetaPathEmpty {}.fail(),
        };
        Ok(Arc::new(decode_custom_metadata.unwrap()))
    }
}

impl<'a> CustomMetadataReaderBuilder {
    pub fn build(
        meta_path_version: Option<String>,
        custom_kv_meta: Option<&'a KeyValue>,
        ignore_sst_filter: bool,
        meta_path: Option<Path>,
        store: ObjectStoreRef,
    ) -> Result<Box<dyn CustomMetadataReader + Send + Sync + 'a>> {
        match meta_path_version {
            None => Ok(Box::new(MetaPathV1Reader::new(
                custom_kv_meta,
                ignore_sst_filter,
            ))),
            Some(v) if v.as_str() == META_PATH_VERSION => {
                Ok(Box::new(MetaPathV2Reader::new(meta_path, store)))
            }
            _ => MetaPathVersionWrong {
                path_version: meta_path_version.unwrap(),
            }
            .fail(),
        }
    }
}
