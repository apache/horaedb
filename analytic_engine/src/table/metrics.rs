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
use table_engine::{partition::maybe_extract_partitioned_table_name, table::TableStats};

use crate::{sst::metrics::MaybeTableLevelMetrics as SstMaybeTableLevelMetrics, MetricsOptions};

const KB: f64 = 1024.0;
const DEFAULT_METRICS_KEY: &str = "total";

lazy_static! {
    // Counters:
    static ref TABLE_WRITE_REQUEST_COUNTER: IntCounter = register_int_counter!(
        "table_write_request_counter",
        "Write request counter of table"
    )
    .unwrap();

    static ref TABLE_WRITE_BATCH_HISTOGRAM: Histogram = register_histogram!(
        "table_write_batch_size",
        "Histogram of write batch size",
        vec![10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0]
    )
    .unwrap();

    static ref TABLE_WRITE_FIELDS_COUNTER: IntCounter = register_int_counter!(
        "table_write_fields_counter",
        "Fields counter of table write"
    )
    .unwrap();

    static ref TABLE_READ_REQUEST_COUNTER: IntCounter = register_int_counter!(
        "table_read_request_counter",
        "Read request counter of table"
    )
    .unwrap();

    static ref TABLE_QUEUE_WRITER_CANCEL_COUNTER: IntCounter = register_int_counter!(
        "table_queue_writer_cancel_counter",
        "Counter for table queue writer cancel"
    ).unwrap();
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
    static ref TABLE_WRITE_DURATION_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "table_write_duration",
        "Histogram for write stall duration of the table in seconds",
        &["type"],
        exponential_buckets(0.01, 2.0, 13).unwrap()
    ).unwrap();

    static ref QUERY_TIME_RANGE: HistogramVec = register_histogram_vec!(
        "query_time_range",
        "Histogram for query time range((15m,30m,...,7d)",
        &["table"],
        exponential_buckets(900.0, 2.0, 10).unwrap()
    )
    .unwrap();

    static ref DURATION_SINCE_QUERY_START_TIME: HistogramVec = register_histogram_vec!(
        "duration_since_query_start_time",
        "Histogram for duration since query start time(15m,30m,...,7d)",
        &["table"],
        exponential_buckets(900.0, 2.0, 10).unwrap()
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

    // Maybe table level sst metrics
    maybe_table_level_metrics: Arc<MaybeTableLevelMetrics>,

    compaction_input_sst_size_histogram: Histogram,
    compaction_output_sst_size_histogram: Histogram,
    compaction_input_sst_row_num_histogram: Histogram,
    compaction_output_sst_row_num_histogram: Histogram,

    table_write_stall_duration: Histogram,
    table_write_encode_duration: Histogram,
    table_write_wal_duration: Histogram,
    table_write_memtable_duration: Histogram,
    table_write_preprocess_duration: Histogram,
    table_write_space_flush_wait_duration: Histogram,
    table_write_instance_flush_wait_duration: Histogram,
    table_write_flush_wait_duration: Histogram,
    table_write_execute_duration: Histogram,
    table_write_queue_waiter_duration: Histogram,
    table_write_queue_writer_duration: Histogram,
    table_write_total_duration: Histogram,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            stats: Arc::new(AtomicTableStats::default()),
            maybe_table_level_metrics: Arc::new(MaybeTableLevelMetrics::new(DEFAULT_METRICS_KEY)),
            compaction_input_sst_size_histogram: TABLE_COMPACTION_SST_SIZE_HISTOGRAM
                .with_label_values(&["input"]),
            compaction_output_sst_size_histogram: TABLE_COMPACTION_SST_SIZE_HISTOGRAM
                .with_label_values(&["output"]),
            compaction_input_sst_row_num_histogram: TABLE_COMPACTION_SST_ROW_NUM_HISTOGRAM
                .with_label_values(&["input"]),
            compaction_output_sst_row_num_histogram: TABLE_COMPACTION_SST_ROW_NUM_HISTOGRAM
                .with_label_values(&["output"]),

            table_write_stall_duration: TABLE_WRITE_DURATION_HISTOGRAM
                .with_label_values(&["stall"]),
            table_write_encode_duration: TABLE_WRITE_DURATION_HISTOGRAM
                .with_label_values(&["encode"]),
            table_write_wal_duration: TABLE_WRITE_DURATION_HISTOGRAM.with_label_values(&["wal"]),
            table_write_memtable_duration: TABLE_WRITE_DURATION_HISTOGRAM
                .with_label_values(&["memtable"]),
            table_write_preprocess_duration: TABLE_WRITE_DURATION_HISTOGRAM
                .with_label_values(&["preprocess"]),
            table_write_space_flush_wait_duration: TABLE_WRITE_DURATION_HISTOGRAM
                .with_label_values(&["wait_space_flush"]),
            table_write_instance_flush_wait_duration: TABLE_WRITE_DURATION_HISTOGRAM
                .with_label_values(&["wait_instance_flush"]),
            table_write_flush_wait_duration: TABLE_WRITE_DURATION_HISTOGRAM
                .with_label_values(&["wait_flush"]),
            table_write_execute_duration: TABLE_WRITE_DURATION_HISTOGRAM
                .with_label_values(&["execute"]),
            table_write_queue_waiter_duration: TABLE_WRITE_DURATION_HISTOGRAM
                .with_label_values(&["queue_waiter"]),
            table_write_queue_writer_duration: TABLE_WRITE_DURATION_HISTOGRAM
                .with_label_values(&["queue_writer"]),
            table_write_total_duration: TABLE_WRITE_DURATION_HISTOGRAM
                .with_label_values(&["total"]),
        }
    }
}

pub struct MaybeTableLevelMetrics {
    // TODO: maybe `query_time_range` and `duration_since_query_query_start_time`
    // should place on a higher level such `TableEngine`.
    // I do so originally, but I found no reasonable place to keep related contexts...
    pub query_time_range: Histogram,
    pub duration_since_query_query_start_time: Histogram,

    pub sst_metrics: Arc<SstMaybeTableLevelMetrics>,
}

impl MaybeTableLevelMetrics {
    pub fn new(maybe_table_name: &str) -> Self {
        let sst_metrics = Arc::new(SstMaybeTableLevelMetrics::new(maybe_table_name));

        Self {
            query_time_range: QUERY_TIME_RANGE.with_label_values(&[maybe_table_name]),
            duration_since_query_query_start_time: DURATION_SINCE_QUERY_START_TIME
                .with_label_values(&[maybe_table_name]),
            sst_metrics,
        }
    }
}

pub struct MetricsContext<'a> {
    /// If enable table level metrics, it should be a table name,
    /// Otherwise it should be `DEFAULT_METRICS_KEY`.
    table_name: &'a str,
    metric_opt: MetricsOptions,
    maybe_partitioned_table_name: Option<String>,
}

impl<'a> MetricsContext<'a> {
    pub fn new(table_name: &'a str, metric_opt: MetricsOptions) -> Self {
        Self {
            table_name,
            metric_opt,
            maybe_partitioned_table_name: None,
        }
    }

    fn maybe_table_name(&mut self) -> &str {
        if !self.metric_opt.enable_table_level_metrics {
            DEFAULT_METRICS_KEY
        } else {
            let maybe_partition_table = maybe_extract_partitioned_table_name(self.table_name);
            match maybe_partition_table {
                Some(partitioned) => {
                    self.maybe_partitioned_table_name = Some(partitioned);
                    self.maybe_partitioned_table_name.as_ref().unwrap()
                }
                None => self.table_name,
            }
        }
    }
}

impl Metrics {
    pub fn new(mut metric_ctx: MetricsContext) -> Self {
        Self {
            maybe_table_level_metrics: Arc::new(MaybeTableLevelMetrics::new(
                metric_ctx.maybe_table_name(),
            )),
            ..Default::default()
        }
    }

    #[inline]
    pub fn maybe_table_level_metrics(&self) -> Arc<MaybeTableLevelMetrics> {
        self.maybe_table_level_metrics.clone()
    }

    #[inline]
    pub fn table_stats(&self) -> TableStats {
        TableStats::from(&*self.stats)
    }

    #[inline]
    pub fn on_write_request_begin(&self) {
        self.stats.num_write.fetch_add(1, Ordering::Relaxed);
        TABLE_WRITE_REQUEST_COUNTER.inc();
    }

    #[inline]
    pub fn on_write_request_done(&self, num_rows: usize, num_columns: usize) {
        TABLE_WRITE_BATCH_HISTOGRAM.observe(num_rows as f64);
        TABLE_WRITE_FIELDS_COUNTER.inc_by((num_columns * num_rows) as u64);
    }

    #[inline]
    pub fn on_read_request_begin(&self) {
        self.stats.num_read.fetch_add(1, Ordering::Relaxed);
        TABLE_READ_REQUEST_COUNTER.inc();
    }

    #[inline]
    pub fn on_write_stall(&self, duration: Duration) {
        self.table_write_stall_duration
            .observe(duration.as_secs_f64());
    }

    #[inline]
    pub fn start_table_total_timer(&self) -> HistogramTimer {
        self.table_write_total_duration.start_timer()
    }

    #[inline]
    pub fn start_table_write_execute_timer(&self) -> HistogramTimer {
        self.table_write_execute_duration.start_timer()
    }

    #[inline]
    pub fn start_table_write_encode_timer(&self) -> HistogramTimer {
        self.table_write_encode_duration.start_timer()
    }

    #[inline]
    pub fn start_table_write_queue_waiter_timer(&self) -> HistogramTimer {
        self.table_write_queue_waiter_duration.start_timer()
    }

    #[inline]
    pub fn start_table_write_queue_writer_timer(&self) -> HistogramTimer {
        self.table_write_queue_writer_duration.start_timer()
    }

    #[inline]
    pub fn start_table_write_memtable_timer(&self) -> HistogramTimer {
        self.table_write_memtable_duration.start_timer()
    }

    #[inline]
    pub fn start_table_write_wal_timer(&self) -> HistogramTimer {
        self.table_write_wal_duration.start_timer()
    }

    #[inline]
    pub fn start_table_write_preprocess_timer(&self) -> HistogramTimer {
        self.table_write_preprocess_duration.start_timer()
    }

    #[inline]
    pub fn start_table_write_space_flush_wait_timer(&self) -> HistogramTimer {
        self.table_write_space_flush_wait_duration.start_timer()
    }

    #[inline]
    pub fn start_table_write_instance_flush_wait_timer(&self) -> HistogramTimer {
        self.table_write_instance_flush_wait_duration.start_timer()
    }

    #[inline]
    pub fn start_table_write_flush_wait_timer(&self) -> HistogramTimer {
        self.table_write_flush_wait_duration.start_timer()
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
