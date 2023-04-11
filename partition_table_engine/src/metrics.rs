// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::{exponential_buckets, register_histogram_vec, HistogramVec};

lazy_static! {
    // Buckets: 0, 0.01, .., 0.01 * 2^12
    pub static ref PARTITION_TABLE_WRITE_DURATION_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "partition_table_write_duration",
        "Histogram for write duration of the partition table in seconds",
        &["type"],
        exponential_buckets(0.01, 2.0, 13).unwrap()
        )
    .unwrap();

    // Buckets: 0, 0.01, .., 0.01 * 2^12
    pub static ref PARTITION_TABLE_PARTITIONED_READ_DURATION_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "partition_table_partitioned_read_duration",
        "Histogram for partitioned read duration of the partition table in seconds",
        &["type"],
        exponential_buckets(0.01, 2.0, 13).unwrap()
        )
    .unwrap();
}
