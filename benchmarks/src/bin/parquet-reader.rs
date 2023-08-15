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
