// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{num::NonZeroUsize, sync::Arc};

use arrow::{
    array::{Array, ArrayRef},
    datatypes::{Schema, SchemaRef},
    record_batch::RecordBatch,
};
use clru::{CLruCache, CLruCacheConfig, WeightScale};
use common_types::hash::{ahash::RandomState, build_fixed_seed_ahasher_builder};
use partitioned_lock::PartitionedMutex;

use crate::sst::{
    manager::FileId,
    metrics::{ROW_GROUP_CACHE_HIT_COUNT, ROW_GROUP_CACHE_MISS_COUNT},
};

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
struct CacheKey {
    sst_id: FileId,
    row_group_idx: u32,
    column_idx: u32,
}

type CacheVal = ArrayRef;

#[derive(Debug)]
struct CustomScale;

impl WeightScale<CacheKey, CacheVal> for CustomScale {
    fn weight(&self, _: &CacheKey, value: &CacheVal) -> usize {
        std::mem::size_of::<CacheKey>() + value.get_array_memory_size()
    }
}

#[derive(Debug)]
struct ColumnCache {
    cache: PartitionedMutex<CLruCache<CacheKey, CacheVal, RandomState, CustomScale>, RandomState>,
}

impl ColumnCache {
    fn new(mem_cap: usize, partition_bits: usize) -> Self {
        let mem_cap_per_partition = mem_cap >> partition_bits;
        let cache_builder = |_| -> std::result::Result<_, ()> {
            let config = CLruCacheConfig::new(NonZeroUsize::new(mem_cap_per_partition).unwrap())
                .with_hasher(build_fixed_seed_ahasher_builder())
                .with_scale(CustomScale);
            Ok(CLruCache::with_config(config))
        };
        let partitioned_cache = PartitionedMutex::try_new(
            cache_builder,
            partition_bits,
            build_fixed_seed_ahasher_builder(),
        )
        .unwrap();

        Self {
            cache: partitioned_cache,
        }
    }

    fn put(&self, cache_key: CacheKey, cache_val: CacheVal) {
        self.cache
            .lock(&cache_key)
            .put_with_weight(cache_key, cache_val)
            .unwrap();
    }

    fn get(&self, cache_key: &CacheKey) -> Option<CacheVal> {
        self.cache.lock(&cache_key).get(cache_key).cloned()
    }

    fn compute_mem_usage(&self) -> usize {
        let mut total_usage = 0;
        for partition in self.cache.get_all_partition() {
            let partition = partition.lock().unwrap();
            total_usage += partition.weight();
        }

        total_usage
    }
}

pub type RowGroupCacheRef = Arc<RowGroupCache>;

#[derive(Debug)]
pub struct RowGroupCache {
    column_cache: ColumnCache,
}

impl RowGroupCache {
    pub fn new(mem_cap: usize, partition_bits: usize) -> Self {
        let column_cache = ColumnCache::new(mem_cap, partition_bits);
        Self { column_cache }
    }

    pub fn put(
        &self,
        sst_id: FileId,
        row_group_idx: usize,
        row_group: RecordBatch,
        projection: &[usize],
    ) {
        assert_eq!(row_group.num_columns(), projection.len());

        for (idx, column) in row_group.columns().iter().cloned().enumerate() {
            let cache_key = CacheKey {
                sst_id,
                row_group_idx: row_group_idx as u32,
                column_idx: projection[idx] as u32,
            };
            self.column_cache.put(cache_key, column);
        }
    }

    pub fn get(
        &self,
        sst_id: FileId,
        row_group_idx: usize,
        schema: &SchemaRef,
        projection: &[usize],
    ) -> Option<RecordBatch> {
        let mut columns = Vec::with_capacity(projection.len());
        for column_idx in projection {
            let cache_key = CacheKey {
                sst_id,
                row_group_idx: row_group_idx as u32,
                column_idx: *column_idx as u32,
            };

            match self.column_cache.get(&cache_key) {
                Some(v) => columns.push(v),
                None => {
                    ROW_GROUP_CACHE_MISS_COUNT.inc();
                    return None;
                }
            }
        }

        // Build the schema according to projection.
        let projected_schema = if projection.len() == schema.all_fields().len() {
            schema.clone()
        } else {
            let mut fields = Vec::with_capacity(projection.len());
            for field_idx in projection {
                fields.push(schema.field(*field_idx).clone());
            }
            Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
        };

        ROW_GROUP_CACHE_HIT_COUNT.inc();
        Some(RecordBatch::try_new(projected_schema, columns).unwrap())
    }

    /// Compute the consumed memory by the cache.
    #[inline]
    pub fn compute_mem_usage(&self) -> usize {
        self.column_cache.compute_mem_usage()
    }

    #[inline]
    pub fn hit_count(&self) -> usize {
        ROW_GROUP_CACHE_HIT_COUNT.get() as usize
    }

    #[inline]
    pub fn miss_count(&self) -> usize {
        ROW_GROUP_CACHE_MISS_COUNT.get() as usize
    }
}
