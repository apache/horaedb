// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Benchmark configs.

use std::env;

use analytic_engine::{space::SpaceId, sst::manager::FileId};
use common_types::time::{TimeRange, Timestamp};
use common_util::{
    config::{ReadableDuration, ReadableSize},
    toml,
};
use serde::Deserialize;
use table_engine::{
    predicate::{PredicateBuilder, PredicateRef},
    table::TableId,
};

const BENCH_CONFIG_PATH_KEY: &str = "ANALYTIC_BENCH_CONFIG_PATH";

#[derive(Deserialize)]
pub struct BenchConfig {
    pub sst_bench: SstBenchConfig,
    pub merge_sst_bench: MergeSstBenchConfig,
    pub scan_memtable_bench: ScanMemTableBenchConfig,
    pub merge_memtable_bench: MergeMemTableBenchConfig,
    pub wal_write_bench: WalWriteBenchConfig,
}

// TODO(yingwen): Maybe we can use layze static to load config first.
pub fn bench_config_from_env() -> BenchConfig {
    let path = match env::var(BENCH_CONFIG_PATH_KEY) {
        Ok(v) => v,
        Err(e) => panic!("Env {BENCH_CONFIG_PATH_KEY} is required to run benches, err:{e}."),
    };

    let mut toml_buf = String::new();
    toml::parse_toml_from_path(&path, &mut toml_buf).expect("Failed to parse config.")
}

#[derive(Deserialize)]
pub struct SstBenchConfig {
    pub store_path: String,
    pub sst_file_name: String,
    pub runtime_thread_num: usize,
    pub is_async: bool,

    pub bench_measurement_time: ReadableDuration,
    pub bench_sample_size: usize,

    /// Max number of projection columns.
    pub max_projections: usize,
    pub num_rows_per_row_group: usize,
    pub predicate: BenchPredicate,
    pub sst_meta_cache_cap: Option<usize>,
    pub sst_data_cache_cap: Option<usize>,
    pub reverse: bool,
}

#[derive(Deserialize)]
pub struct MergeSstBenchConfig {
    pub store_path: String,
    pub space_id: SpaceId,
    pub table_id: TableId,
    pub sst_file_ids: Vec<FileId>,
    pub runtime_thread_num: usize,

    pub bench_measurement_time: ReadableDuration,
    pub bench_sample_size: usize,

    /// Max number of projection columns.
    pub max_projections: usize,
    pub num_rows_per_row_group: usize,
    pub predicate: BenchPredicate,
}

#[derive(Deserialize)]
pub struct ScanMemTableBenchConfig {
    pub store_path: String,
    pub sst_file_name: String,
    pub runtime_thread_num: usize,

    /// Max number of projection columns.
    pub max_projections: usize,

    pub arena_block_size: ReadableSize,
}

#[derive(Debug, Deserialize)]
pub struct BenchPredicate {
    /// Inclusive start time in millis.
    start_time_ms: i64,
    /// Exclusive end time in millis.
    ///
    /// Set to current time millis if start_time_ms == end_time_ms.
    end_time_ms: i64,
}

impl BenchPredicate {
    pub fn into_predicate(self) -> PredicateRef {
        let start = Timestamp::new(self.start_time_ms);
        let end = if self.start_time_ms == self.end_time_ms {
            Timestamp::now()
        } else {
            Timestamp::new(self.end_time_ms)
        };
        let time_range = TimeRange::new(start, end).unwrap();

        PredicateBuilder::default()
            .set_time_range(time_range)
            .build()
    }
}

#[derive(Deserialize)]
pub struct MergeMemTableBenchConfig {
    pub store_path: String,
    pub space_id: SpaceId,
    pub table_id: TableId,
    pub sst_file_ids: Vec<FileId>,
    pub runtime_thread_num: usize,

    /// Max number of projection columns.
    pub max_projections: usize,

    pub arena_block_size: ReadableSize,
}

#[derive(Deserialize)]
pub struct WalWriteBenchConfig {
    pub bench_measurement_time: ReadableDuration,
    pub bench_sample_size: usize,
    pub batch_size: usize,
    pub value_size: usize,
}
