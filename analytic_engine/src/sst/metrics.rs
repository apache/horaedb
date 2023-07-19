// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::{exponential_buckets, register_counter, register_histogram, Counter, Histogram};

lazy_static! {
    // Histogram:
    // Buckets: 100B,200B,400B,...,2KB
    pub static ref SST_GET_RANGE_HISTOGRAM: Histogram = register_histogram!(
        "sst_get_range_length",
        "Histogram for sst get range length",
        exponential_buckets(100.0, 2.0, 5).unwrap()
    ).unwrap();

    pub static ref ROW_GROUP_CACHE_HIT_COUNT: Counter = register_counter!(
        "row_group_cache_hit_count",
        "Counter for row group cache hit"
    ).unwrap();

    pub static ref ROW_GROUP_CACHE_MISS_COUNT: Counter = register_counter!(
        "row_group_cache_miss_count",
        "Counter for row group cache miss"
    ).unwrap();

    pub static ref SST_META_CACHE_HIT_COUNT: Counter = register_counter!(
        "sst_meta_cache_hit_count",
        "Counter for sst meta cache hit"
    ).unwrap();

    pub static ref SST_META_CACHE_MISS_COUNT: Counter = register_counter!(
        "sst_meta_cache_miss_count",
        "Counter for sst meta cache miss"
    ).unwrap();
}
