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

use lazy_static::lazy_static;
use prometheus::{
    exponential_buckets, register_counter, register_histogram, register_int_counter_vec, Counter,
    Histogram, IntCounter, IntCounterVec,
};

lazy_static! {
    // Histogram:
    // Buckets: 100B,200B,400B,...,2KB
    pub static ref SST_GET_RANGE_HISTOGRAM: Histogram = register_histogram!(
        "sst_get_range_length",
        "Histogram for sst get range length",
        exponential_buckets(100.0, 2.0, 5).unwrap()
    ).unwrap();

    pub static ref META_DATA_CACHE_HIT_COUNTER: Counter = register_counter!(
        "META_DATA_CACHE_HIT",
        "The counter for meta data cache hit"
    ).unwrap();

    pub static ref META_DATA_CACHE_MISS_COUNTER: Counter = register_counter!(
        "META_DATA_CACHE_MISS",
        "The counter for meta data cache miss"
    ).unwrap();

    static ref SST_BEFORE_PRUNE_COUNTER: IntCounterVec = register_int_counter_vec!(
        "sst_before_prune",
        "The counter for sst before prune",
        &["table"]
    ).unwrap();

    static ref SST_AFTER_PRUNE_COUNTER: IntCounterVec = register_int_counter_vec!(
        "sst_after_prune",
        "The counter for sst after prune",
        &["table"]
    ).unwrap();
}

#[derive(Debug)]
pub struct MaybeTableLevelMetrics {
    pub sst_before_prune_counter: IntCounter,
    pub sst_after_prune_counter: IntCounter,
}

impl MaybeTableLevelMetrics {
    pub fn new(table: &str) -> Self {
        Self {
            sst_before_prune_counter: SST_BEFORE_PRUNE_COUNTER.with_label_values(&[table]),
            sst_after_prune_counter: SST_AFTER_PRUNE_COUNTER.with_label_values(&[table]),
        }
    }
}
