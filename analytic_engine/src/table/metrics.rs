// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Metrics of table.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use lazy_static::lazy_static;
use prometheus::{
    exponential_buckets,
    local::{LocalHistogram, LocalHistogramTimer},
    register_histogram, register_histogram_vec, register_int_counter, Histogram, HistogramTimer,
    HistogramVec, IntCounter,
};
use table_engine::table::TableStats;

const KB: f64 = 1024.0;

lazy_static! {
    // Counters:
    static ref TABLE_WRITE_REQUEST_COUNTER: IntCounter = register_int_counter!(
        "table_write_request_counter",
        "Write request counter of table"
    )
    .unwrap();

    static ref TABLE_WRITE_ROWS_COUNTER: IntCounter = register_int_counter!(
        "table_write_rows_counter",
        "Number of rows wrote to table"
    )
    .unwrap();

    static ref TABLE_READ_REQUEST_COUNTER: IntCounter = register_int_counter!(
        "table_read_request_counter",
        "Read request counter of table"
    )
    .unwrap();
    // End of counters.

    // Histograms:
    // Buckets: 0, 0.002, .., 0.002 * 4^9
    static ref TABLE_FLUSH_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "table_flush_duration",
        "Histogram for flush duration of the table in seconds",
        exponential_buckets(0.002, 4.0, 10).unwrap()
    ).unwrap();

    // Buckets: 0, 1, .., 2^7
    static ref TABLE_FLUSH_SST_NUM_HISTOGRAM: Histogram = register_histogram!(
        "table_flush_sst_num",
        "Histogram for number of ssts flushed by the table",
        exponential_buckets(1.0, 2.0, 8).unwrap()
    ).unwrap();

    // Buckets: 0, 1, ..., 4^11 (4GB)
    static ref TABLE_FLUSH_SST_SIZE_HISTOGRAM: Histogram = register_histogram!(
        "table_flush_sst_size",
        "Histogram for size of ssts flushed by the table in KB",
        exponential_buckets(1.0, 4.0, 12).unwrap()
    ).unwrap();

    // Buckets: 0, 0.02, .., 0.02 * 4^9
    static ref TABLE_COMPACTION_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "table_compaction_duration",
        "Histogram for compaction duration of the table in seconds",
        exponential_buckets(0.02, 4.0, 10).unwrap()
    ).unwrap();

    // Buckets: 0, 1, .., 2^7
    static ref TABLE_COMPACTION_SST_NUM_HISTOGRAM: Histogram = register_histogram!(
        "table_compaction_sst_num",
        "Histogram for number of ssts compacted by the table",
        exponential_buckets(1.0, 2.0, 8).unwrap()
    ).unwrap();

    // Buckets: 0, 1, ..., 4^11 (4GB)
    static ref TABLE_COMPACTION_SST_SIZE_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "table_compaction_sst_size",
        "Histogram for size of ssts compacted by the table in KB",
        &["type"],
        exponential_buckets(1.0, 4.0, 12).unwrap()
    ).unwrap();

    // Buckets: 0, 1, ..., 10^12(1 billion)
    static ref TABLE_COMPACTION_SST_ROW_NUM_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "table_compaction_sst_row_num",
        "Histogram for row num of ssts compacted by the table",
        &["type"],
        exponential_buckets(1.0, 10.0, 13).unwrap()
    ).unwrap();

    // Buckets: 0, 0.01, .., 0.01 * 2^12
    static ref TABLE_WRITE_STALL_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "table_write_stall_duration",
        "Histogram for write stall duration of the table in seconds",
        exponential_buckets(0.01, 2.0, 13).unwrap()
    ).unwrap();

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
    // End of histograms.
}

#[derive(Default)]
struct AtomicTableStats {
    num_write: AtomicU64,
    num_read: AtomicU64,
    num_flush: AtomicU64,
}

impl From<&AtomicTableStats> for TableStats {
    fn from(stats: &AtomicTableStats) -> Self {
        Self {
            num_write: stats.num_write.load(Ordering::Relaxed),
            num_read: stats.num_read.load(Ordering::Relaxed),
            num_flush: stats.num_flush.load(Ordering::Relaxed),
        }
    }
}

/// Table metrics.
///
/// Now the registered labels won't remove from the metrics vec to avoid panic
/// on concurrent removal.
pub struct Metrics {
    // Stats of a single table.
    stats: Arc<AtomicTableStats>,

    compaction_input_sst_size_histogram: Histogram,
    compaction_output_sst_size_histogram: Histogram,
    compaction_input_sst_row_num_histogram: Histogram,
    compaction_output_sst_row_num_histogram: Histogram,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            stats: Arc::new(AtomicTableStats::default()),
            compaction_input_sst_size_histogram: TABLE_COMPACTION_SST_SIZE_HISTOGRAM
                .with_label_values(&["input"]),
            compaction_output_sst_size_histogram: TABLE_COMPACTION_SST_SIZE_HISTOGRAM
                .with_label_values(&["output"]),
            compaction_input_sst_row_num_histogram: TABLE_COMPACTION_SST_ROW_NUM_HISTOGRAM
                .with_label_values(&["input"]),
            compaction_output_sst_row_num_histogram: TABLE_COMPACTION_SST_ROW_NUM_HISTOGRAM
                .with_label_values(&["output"]),
        }
    }
}

impl Metrics {
    pub fn table_stats(&self) -> TableStats {
        TableStats::from(&*self.stats)
    }

    #[inline]
    pub fn on_write_request_begin(&self) {
        self.stats.num_write.fetch_add(1, Ordering::Relaxed);
        TABLE_WRITE_REQUEST_COUNTER.inc();
    }

    #[inline]
    pub fn on_write_request_done(&self, num_rows: usize) {
        TABLE_WRITE_ROWS_COUNTER.inc_by(num_rows as u64);
    }

    #[inline]
    pub fn on_read_request_begin(&self) {
        self.stats.num_read.fetch_add(1, Ordering::Relaxed);
        TABLE_READ_REQUEST_COUNTER.inc();
    }

    #[inline]
    pub fn on_write_stall(&self, duration: Duration) {
        TABLE_WRITE_STALL_DURATION_HISTOGRAM.observe(duration.as_secs_f64());
    }

    #[inline]
    pub fn start_compaction_timer(&self) -> HistogramTimer {
        TABLE_COMPACTION_DURATION_HISTOGRAM.start_timer()
    }

    #[inline]
    pub fn compaction_observe_duration(&self, duration: Duration) {
        TABLE_COMPACTION_DURATION_HISTOGRAM.observe(duration.as_secs_f64());
    }

    #[inline]
    pub fn compaction_observe_sst_num(&self, sst_num: usize) {
        TABLE_COMPACTION_SST_NUM_HISTOGRAM.observe(sst_num as f64);
    }

    #[inline]
    pub fn compaction_observe_input_sst_size(&self, sst_size: u64) {
        // Convert bytes to KB.
        self.compaction_input_sst_size_histogram
            .observe(sst_size as f64 / KB);
    }

    #[inline]
    pub fn compaction_observe_output_sst_size(&self, sst_size: u64) {
        // Convert bytes to KB.
        self.compaction_output_sst_size_histogram
            .observe(sst_size as f64 / KB);
    }

    #[inline]
    pub fn compaction_observe_input_sst_row_num(&self, sst_row_num: u64) {
        self.compaction_input_sst_row_num_histogram
            .observe(sst_row_num as f64);
    }

    #[inline]
    pub fn compaction_observe_output_sst_row_num(&self, sst_row_num: u64) {
        self.compaction_output_sst_row_num_histogram
            .observe(sst_row_num as f64);
    }

    #[inline]
    pub fn local_flush_metrics(&self) -> LocalFlushMetrics {
        LocalFlushMetrics {
            stats: self.stats.clone(),
            flush_duration_histogram: TABLE_FLUSH_DURATION_HISTOGRAM.local(),
            flush_sst_num_histogram: TABLE_FLUSH_SST_NUM_HISTOGRAM.local(),
            flush_sst_size_histogram: TABLE_FLUSH_SST_SIZE_HISTOGRAM.local(),
        }
    }
}

pub struct LocalFlushMetrics {
    stats: Arc<AtomicTableStats>,

    flush_duration_histogram: LocalHistogram,
    flush_sst_num_histogram: LocalHistogram,
    flush_sst_size_histogram: LocalHistogram,
}

impl LocalFlushMetrics {
    pub fn start_flush_timer(&self) -> LocalHistogramTimer {
        self.stats.num_flush.fetch_add(1, Ordering::Relaxed);
        self.flush_duration_histogram.start_timer()
    }

    pub fn observe_sst_num(&self, sst_num: usize) {
        self.flush_sst_num_histogram.observe(sst_num as f64);
    }

    pub fn observe_sst_size(&self, sst_size: u64) {
        // Convert bytes to KB.
        self.flush_sst_size_histogram.observe(sst_size as f64 / KB);
    }
}
