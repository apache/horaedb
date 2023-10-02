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

use std::{
    fmt::Debug,
    sync::{Arc, RwLock},
};

use lru::LruCache;
use object_store::{ObjectStoreRef, Path};
use parquet::{file::metadata::FileMetaData, format::KeyValue};
use snafu::{ensure, OptionExt};

use crate::sst::{
    meta_data::{
        metadata_reader::parse_metadata, KvMetaDataNotFound, KvMetaVersionEmpty,
        ParquetMetaDataRef, Result,
    },
    metrics::{META_DATA_CACHE_HIT_COUNTER, META_DATA_CACHE_MISS_COUNTER},
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
    // TODO: remove it and use the suggested api.
    #[allow(deprecated)]
    pub async fn try_new(
        parquet_meta_data: &parquet_ext::ParquetMetaData,
        ignore_sst_filter: bool,
        store: ObjectStoreRef,
    ) -> Result<Self> {
        let file_meta_data = parquet_meta_data.file_metadata();
        let kv_metas = file_meta_data
            .key_value_metadata()
            .context(KvMetaDataNotFound)?;

        ensure!(!kv_metas.is_empty(), KvMetaDataNotFound);

        let mut meta_path = None;
        let mut other_kv_metas: Vec<KeyValue> = Vec::with_capacity(kv_metas.len() - 1);
        let mut custom_kv_meta = None;
        let mut meta_version = encoding::META_VERSION_V1; // default is v1

        for kv_meta in kv_metas {
            if kv_meta.key == encoding::META_KEY {
                custom_kv_meta = Some(kv_meta);
            } else if kv_meta.key == encoding::META_PATH_KEY {
                meta_path = kv_meta.value.as_ref().map(|path| Path::from(path.as_str()))
            } else if kv_meta.key == encoding::META_VERSION_KEY {
                meta_version = kv_meta.value.as_ref().context(KvMetaVersionEmpty)?;
            } else {
                other_kv_metas.push(kv_meta.clone());
            }
        }

        let custom = parse_metadata(
            meta_version,
            custom_kv_meta,
            ignore_sst_filter,
            meta_path.clone(),
            store,
        )
        .await?;

        // let's build a new parquet metadata without the extended key value
        // metadata.
        let other_kv_metas = if other_kv_metas.is_empty() {
            None
        } else {
            Some(other_kv_metas)
        };
        let parquet = {
            let thin_file_meta_data = FileMetaData::new(
                file_meta_data.version(),
                file_meta_data.num_rows(),
                file_meta_data.created_by().map(|v| v.to_string()),
                other_kv_metas,
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
        let v = self.cache.write().unwrap().get(key).cloned();
        if v.is_some() {
            META_DATA_CACHE_HIT_COUNTER.inc()
        } else {
            META_DATA_CACHE_MISS_COUNTER.inc()
        }

        v
    }

    pub fn put(&self, key: String, value: MetaData) {
        self.cache.write().unwrap().put(key, value);
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, path::Path, sync::Arc};

    use arrow::{
        array::UInt64Builder,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use bytes_ext::Bytes;
    use common_types::{
        column_schema::Builder as ColumnSchemaBuilder,
        schema::Builder as CustomSchemaBuilder,
        time::{TimeRange, Timestamp},
    };
    use object_store::{LocalFileSystem, ObjectStoreRef};
    use parquet::{arrow::ArrowWriter, file::footer};
    use parquet_ext::ParquetMetaData;

    use super::MetaData;
    use crate::{
        sst::parquet::{
            encoding::{self, META_PATH_KEY, META_VERSION_KEY},
            meta_data::ParquetMetaData as CustomParquetMetaData,
        },
        table::sst_util::new_metadata_path,
    };

    // TODO: remove it and use the suggested api.
    #[allow(deprecated)]
    fn check_parquet_meta_data(original: &ParquetMetaData, processed: &ParquetMetaData) {
        assert_eq!(original.page_indexes(), processed.page_indexes());
        assert_eq!(original.offset_indexes(), processed.offset_indexes());
        assert_eq!(original.num_row_groups(), processed.num_row_groups());
        assert_eq!(original.row_groups(), processed.row_groups());

        let original_file_md = original.file_metadata();
        let processed_file_md = processed.file_metadata();
        assert_eq!(original_file_md.num_rows(), processed_file_md.num_rows());
        assert_eq!(original_file_md.version(), processed_file_md.version());
        assert_eq!(
            original_file_md.created_by(),
            processed_file_md.created_by()
        );
        assert_eq!(original_file_md.schema(), processed_file_md.schema());
        assert_eq!(
            original_file_md.schema_descr(),
            processed_file_md.schema_descr()
        );
        assert_eq!(
            original_file_md.schema_descr_ptr(),
            processed_file_md.schema_descr_ptr()
        );
        assert_eq!(
            original_file_md.column_orders(),
            processed_file_md.column_orders()
        );

        if let Some(kv_metas) = original_file_md.key_value_metadata() {
            let processed_kv_metas = processed_file_md.key_value_metadata().unwrap();
            assert_eq!(kv_metas.len(), processed_kv_metas.len() + 2);
            for kv in kv_metas {
                match kv.key.as_str() {
                    "ARROW:schema" => {
                        // don't care this
                    }
                    encoding::META_KEY => assert!(kv.value.is_none()),
                    encoding::META_VERSION_KEY => assert_eq!("2", kv.value.clone().unwrap()),
                    encoding::META_PATH_KEY => {
                        let meta_path = kv.value.as_ref().unwrap();
                        assert!(meta_path.ends_with(".metadata"));
                    }
                    _ => panic!("Unknown parquet kv, value:{kv:?}"),
                }
            }
        } else {
            assert!(processed_file_md.key_value_metadata().is_none());
        }
    }

    async fn write_parquet_file_with_metadata(
        store: ObjectStoreRef,
        parquet_file_path: &Path,
        custom_meta_data: &CustomParquetMetaData,
    ) {
        let tsid_array = {
            let mut builder = UInt64Builder::new();
            builder.append_value(10);
            builder.append_null();
            builder.append_value(11);
            builder.finish()
        };
        let timestamp_array = {
            let mut builder = UInt64Builder::new();
            builder.append_value(1000);
            builder.append_null();
            builder.append_value(1001);
            builder.finish()
        };
        let file = File::create(parquet_file_path).unwrap();
        let schema = Schema::new(vec![
            Field::new("tsid", DataType::UInt64, true),
            Field::new("timestamp", DataType::UInt64, true),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(tsid_array), Arc::new(timestamp_array)],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        let meta_path = new_metadata_path(parquet_file_path.to_str().unwrap());
        writer.append_key_value_metadata(parquet::format::KeyValue {
            key: META_PATH_KEY.to_string(),
            value: Some(meta_path.clone()),
        });
        writer.append_key_value_metadata(parquet::format::KeyValue {
            key: META_VERSION_KEY.to_string(),
            value: Some("2".to_string()),
        });
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let bytes = encoding::encode_sst_meta_data(custom_meta_data.clone()).unwrap();
        let meta_path = object_store::Path::from(meta_path);
        store.put(&meta_path, bytes).await.unwrap();
    }

    #[tokio::test]
    async fn test_arrow_meta_data() {
        let temp_dir = tempfile::tempdir().unwrap();
        let parquet_file_path = temp_dir.path().join("test_arrow_meta_data.par");
        let schema = {
            let tsid_column_schema = ColumnSchemaBuilder::new(
                "tsid".to_string(),
                common_types::datum::DatumKind::UInt64,
            )
            .build()
            .unwrap();
            let timestamp_column_schema = ColumnSchemaBuilder::new(
                "timestamp".to_string(),
                common_types::datum::DatumKind::Timestamp,
            )
            .build()
            .unwrap();
            CustomSchemaBuilder::new()
                .auto_increment_column_id(true)
                .add_key_column(tsid_column_schema)
                .unwrap()
                .add_key_column(timestamp_column_schema)
                .unwrap()
                .build()
                .unwrap()
        };
        let custom_meta_data = CustomParquetMetaData {
            min_key: Bytes::from_static(&[0, 1]),
            max_key: Bytes::from_static(&[2, 2]),
            time_range: TimeRange::new_unchecked(Timestamp::new(0), Timestamp::new(10)),
            max_sequence: 1001,
            schema,
            parquet_filter: None,
            column_values: None,
        };
        let store = Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        write_parquet_file_with_metadata(
            store.clone(),
            parquet_file_path.as_path(),
            &custom_meta_data,
        )
        .await;

        let parquet_file = File::open(parquet_file_path.as_path()).unwrap();
        let parquet_meta_data = footer::parse_metadata(&parquet_file).unwrap();
        let meta_data = MetaData::try_new(&parquet_meta_data, false, store)
            .await
            .unwrap();

        assert_eq!(**meta_data.custom(), custom_meta_data);
        check_parquet_meta_data(&parquet_meta_data, meta_data.parquet());
    }
}
