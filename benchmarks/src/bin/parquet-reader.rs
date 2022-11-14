// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::Once;

use benchmarks::{
    config::{self, BenchConfig},
    parquet_bench::ParquetBench,
};
use env_logger::Env;

static INIT_LOG: Once = Once::new();

pub fn init_bench() -> BenchConfig {
    INIT_LOG.call_once(|| {
        env_logger::from_env(Env::default().default_filter_or("info")).init();
    });

    config::bench_config_from_env()
}

fn main() {
    let config = init_bench();
    let bench = ParquetBench::new(config.sst_bench);

    for _ in 0..10 {
        bench.run_bench();
    }
    println!("done");
}
