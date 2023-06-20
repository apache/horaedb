// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::Debug,
    sync::{Arc, RwLock},
};

use lru::LruCache;
use parquet::file::metadata::FileMetaData;
use snafu::{OptionExt, ResultExt};

use crate::sst::{
    meta_data::{DecodeCustomMetaData, KvMetaDataNotFound, ParquetMetaDataRef, Result},
    parquet::encoding,
};

pub type MetaCacheRef = Arc<MetaCache>;

/// The metadata of one sst file, including the original metadata of parquet and
/// the custom metadata of ceresdb.
#[derive(Debug, Clone)]
pub struct MetaData {
    /// The extended information in the parquet is removed for less memory
    /// consumption.
    parquet: parquet_ext::ParquetMetaDataRef,
    custom: ParquetMetaDataRef,
}

impl MetaData {
    /// Build [`MetaData`] from the original parquet_meta_data.
    ///
    /// After the building, a new parquet meta data will be generated which
    /// contains no extended custom information.
    pub fn try_new(
        parquet_meta_data: &parquet_ext::ParquetMetaData,
        ignore_sst_filter: bool,
    ) -> Result<Self> {
        let file_meta_data = parquet_meta_data.file_metadata();
        let kv_metas = file_meta_data
            .key_value_metadata()
            .context(KvMetaDataNotFound)?;
        let kv_meta = kv_metas
            .iter()
            .find(|kv| kv.key == encoding::META_KEY)
            .context(KvMetaDataNotFound)?;

        let custom = {
            let mut sst_meta =
                encoding::decode_sst_meta_data(kv_meta).context(DecodeCustomMetaData)?;
            if ignore_sst_filter {
                sst_meta.parquet_filter = None;
            }

            Arc::new(sst_meta)
        };

        // let's build a new parquet metadata without the extended key value
        // metadata.
        let parquet = {
            let thin_file_meta_data = FileMetaData::new(
                file_meta_data.version(),
                file_meta_data.num_rows(),
                file_meta_data.created_by().map(|v| v.to_string()),
                // Remove the key value metadata.
                None,
                file_meta_data.schema_descr_ptr(),
                file_meta_data.column_orders().cloned(),
            );
            let thin_parquet_meta_data = parquet_ext::ParquetMetaData::new_with_page_index(
                thin_file_meta_data,
                parquet_meta_data.row_groups().to_vec(),
                parquet_meta_data.page_indexes().cloned(),
                parquet_meta_data.offset_indexes().cloned(),
            );

            Arc::new(thin_parquet_meta_data)
        };

        Ok(Self { parquet, custom })
    }

    #[inline]
    pub fn parquet(&self) -> &parquet_ext::ParquetMetaDataRef {
        &self.parquet
    }

    #[inline]
    pub fn custom(&self) -> &ParquetMetaDataRef {
        &self.custom
    }
}

/// A cache for storing [`MetaData`].
#[derive(Debug)]
pub struct MetaCache {
    cache: RwLock<LruCache<String, MetaData>>,
}

impl MetaCache {
    pub fn new(cap: usize) -> Self {
        Self {
            cache: RwLock::new(LruCache::new(cap)),
        }
    }

    pub fn get(&self, key: &str) -> Option<MetaData> {
        self.cache.write().unwrap().get(key).cloned()
    }

    pub fn put(&self, key: String, value: MetaData) {
        self.cache.write().unwrap().put(key, value);
    }
}
