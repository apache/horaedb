// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::{exponential_buckets, register_histogram_vec, HistogramVec};

lazy_static! {
    // Buckets: 0.001, .., 0.01 * 2^10
    pub static ref OBKV_OP_DURATION_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "obkv_op_duration",
        "Histogram for duration of different obkv operations",
        &["type"],
        exponential_buckets(0.001, 2.0, 10).unwrap()
        )
    .unwrap();
}
