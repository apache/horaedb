// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Metrics of table.

use std::time::Duration;

use lazy_static::lazy_static;
use prometheus::{
    exponential_buckets, local::LocalHistogram, register_histogram_vec, register_int_counter_vec,
    Histogram, HistogramVec, IntCounter, IntCounterVec,
};

const KB: f64 = 1024.0;

lazy_static! {
    // Counters:
    static ref TABLE_WRITE_REQUEST_COUNTER: IntCounterVec = register_int_counter_vec!(
        "table_write_request_counter",
        "Write request counter of table",
        &["table"]
    )
    .unwrap();
    static ref TABLE_WRITE_ROWS_COUNTER: IntCounterVec = register_int_counter_vec!(
        "table_write_rows_counter",
        "Number of rows wrote to table",
        &["table"]
    )
    .unwrap();
    static ref TABLE_READ_REQUEST_COUNTER: IntCounterVec = register_int_counter_vec!(
        "table_read_request_counter",
        "Read request counter of table",
        &["table"]
    )
    .unwrap();
    // End of counters.

    // Histograms:
    // Buckets: 0, 0.002, .., 0.002 * 4^9
    static ref TABLE_FLUSH_DURATION_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "table_flush_duration",
        "Histogram for flush duration of the table in seconds",
        &["table"],
        exponential_buckets(0.002, 4.0, 10).unwrap()
    ).unwrap();
    // Buckets: 0, 1, .., 2^7
    static ref TABLE_FLUSH_SST_NUM_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "table_flush_sst_num",
        "Histogram for number of ssts flushed by the table",
        &["table"],
        exponential_buckets(1.0, 2.0, 8).unwrap()
    ).unwrap();
    // Buckets: 0, 1, ..., 4^11 (4GB)
    static ref TABLE_FLUSH_SST_SIZE_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "table_flush_sst_size",
        "Histogram for size of ssts flushed by the table in KB",
        &["table"],
        exponential_buckets(1.0, 4.0, 12).unwrap()
    ).unwrap();

    // Buckets: 0, 0.02, .., 0.02 * 4^9
    static ref TABLE_COMPACT_DURATION_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "table_compaction_duration",
        "Histogram for compaction duration of the table in seconds",
        &["table"],
        exponential_buckets(0.02, 4.0, 10).unwrap()
    ).unwrap();
    // Buckets: 0, 1, .., 2^7
    static ref TABLE_COMPACTION_SST_NUM_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "table_compaction_sst_num",
        "Histogram for number of ssts compacted by the table",
        &["table"],
        exponential_buckets(1.0, 2.0, 8).unwrap()
    ).unwrap();
    // Buckets: 0, 1, ..., 4^11 (4GB)
    static ref TABLE_COMPACTION_SST_SIZE_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "table_compaction_sst_size",
        "Histogram for size of ssts compacted by the table in KB",
        &["table", "type"],
        exponential_buckets(1.0, 4.0, 12).unwrap()
    ).unwrap();
    // Buckets: 0, 1, ..., 10^12(1 billion)
    static ref TABLE_COMPACTION_SST_ROW_NUM_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "table_compaction_sst_row_num",
        "Histogram for row num of ssts compacted by the table",
        &["table", "type"],
        exponential_buckets(1.0, 10.0, 13).unwrap()
    ).unwrap();

    // Buckets: 0, 0.01, .., 0.01 * 2^12
    static ref TABLE_WRITE_STALL_DURATION_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "table_write_stall_duration",
        "Histogram for write stall duration of the table in seconds",
        &["table"],
        exponential_buckets(0.01, 2.0, 13).unwrap()
    ).unwrap();
    // End of histograms.
}

/// Table metrics.
///
/// Now the registered labels won't remove from the metrics vec to avoid panic
/// on concurrent removal.
pub struct Metrics {
    // Counters:
    pub write_request_counter: IntCounter,
    write_rows_counter: IntCounter,
    pub read_request_counter: IntCounter,
    // End of counters.

    // Histograms:
    pub flush_duration_histogram: Histogram,
    flush_sst_num_histogram: Histogram,
    flush_sst_size_histogram: Histogram,
    flush_memtables_num_histogram: Histogram,

    pub compaction_duration_histogram: Histogram,
    compaction_sst_num_histogram: Histogram,
    compaction_input_sst_size_histogram: Histogram,
    compaction_output_sst_size_histogram: Histogram,
    compaction_input_sst_row_num_histogram: Histogram,
    compaction_output_sst_row_num_histogram: Histogram,

    // Write stall metrics.
    write_stall_duration_histogram: Histogram,
    // End of histograms.
}

impl Metrics {
    pub fn new(table_name: &str) -> Self {
        Self {
            write_request_counter: TABLE_WRITE_REQUEST_COUNTER.with_label_values(&[table_name]),
            write_rows_counter: TABLE_WRITE_ROWS_COUNTER.with_label_values(&[table_name]),
            read_request_counter: TABLE_READ_REQUEST_COUNTER.with_label_values(&[table_name]),

            flush_duration_histogram: TABLE_FLUSH_DURATION_HISTOGRAM
                .with_label_values(&[table_name]),
            flush_sst_num_histogram: TABLE_FLUSH_SST_NUM_HISTOGRAM.with_label_values(&[table_name]),
            flush_sst_size_histogram: TABLE_FLUSH_SST_SIZE_HISTOGRAM
                .with_label_values(&[table_name]),
            flush_memtables_num_histogram: TABLE_FLUSH_SST_NUM_HISTOGRAM
                .with_label_values(&[table_name]),

            compaction_duration_histogram: TABLE_COMPACT_DURATION_HISTOGRAM
                .with_label_values(&[table_name]),
            compaction_sst_num_histogram: TABLE_COMPACTION_SST_NUM_HISTOGRAM
                .with_label_values(&[table_name]),
            compaction_input_sst_size_histogram: TABLE_COMPACTION_SST_SIZE_HISTOGRAM
                .with_label_values(&[table_name, "input"]),
            compaction_output_sst_size_histogram: TABLE_COMPACTION_SST_SIZE_HISTOGRAM
                .with_label_values(&[table_name, "output"]),
            compaction_input_sst_row_num_histogram: TABLE_COMPACTION_SST_ROW_NUM_HISTOGRAM
                .with_label_values(&[table_name, "input"]),
            compaction_output_sst_row_num_histogram: TABLE_COMPACTION_SST_ROW_NUM_HISTOGRAM
                .with_label_values(&[table_name, "output"]),

            write_stall_duration_histogram: TABLE_WRITE_STALL_DURATION_HISTOGRAM
                .with_label_values(&[table_name]),
        }
    }

    #[inline]
    pub fn on_write_request_begin(&self) {
        self.write_request_counter.inc();
    }

    #[inline]
    pub fn on_write_request_done(&self, num_rows: usize) {
        self.write_rows_counter.inc_by(num_rows as u64);
    }

    #[inline]
    pub fn on_read_request_begin(&self) {
        self.read_request_counter.inc();
    }

    #[inline]
    pub fn on_write_stall(&self, duration: Duration) {
        self.write_stall_duration_histogram
            .observe(duration.as_secs_f64());
    }

    pub fn local_flush_metrics(&self) -> LocalFlushMetrics {
        LocalFlushMetrics {
            flush_duration_histogram: self.flush_duration_histogram.local(),
            flush_sst_num_histogram: self.flush_sst_num_histogram.local(),
            flush_sst_size_histogram: self.flush_sst_size_histogram.local(),
            flush_memtables_num_histogram: self.flush_memtables_num_histogram.local(),
        }
    }

    pub fn compaction_observe_sst_num(&self, sst_num: usize) {
        self.compaction_sst_num_histogram.observe(sst_num as f64);
    }

    pub fn compaction_observe_input_sst_size(&self, sst_size: u64) {
        // Convert bytes to KB.
        self.compaction_input_sst_size_histogram
            .observe(sst_size as f64 / KB);
    }

    pub fn compaction_observe_output_sst_size(&self, sst_size: u64) {
        // Convert bytes to KB.
        self.compaction_output_sst_size_histogram
            .observe(sst_size as f64 / KB);
    }

    pub fn compaction_observe_input_sst_row_num(&self, sst_row_num: u64) {
        self.compaction_input_sst_row_num_histogram
            .observe(sst_row_num as f64);
    }

    pub fn compaction_observe_output_sst_row_num(&self, sst_row_num: u64) {
        self.compaction_output_sst_row_num_histogram
            .observe(sst_row_num as f64);
    }
}

pub struct LocalFlushMetrics {
    pub flush_duration_histogram: LocalHistogram,
    flush_sst_num_histogram: LocalHistogram,
    flush_sst_size_histogram: LocalHistogram,
    flush_memtables_num_histogram: LocalHistogram,
}

impl LocalFlushMetrics {
    pub fn observe_sst_num(&self, sst_num: usize) {
        self.flush_sst_num_histogram.observe(sst_num as f64);
    }

    pub fn observe_sst_size(&self, sst_size: u64) {
        // Convert bytes to KB.
        self.flush_sst_size_histogram.observe(sst_size as f64 / KB);
    }

    pub fn observe_memtables_num(&self, num: usize) {
        self.flush_memtables_num_histogram.observe(num as f64);
    }
}
