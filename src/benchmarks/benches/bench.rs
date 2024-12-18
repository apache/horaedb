// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Benchmarks

use std::{cell::RefCell, sync::Once};

use benchmarks::{
    config::{self, BenchConfig},
    encoding_bench::EncodingBench,
};
use criterion::*;

static INIT_LOG: Once = Once::new();

pub fn init_bench() -> BenchConfig {
    INIT_LOG.call_once(|| {
        env_logger::init();
    });

    config::bench_config_from_env()
}

// fn bench_encoding_iter(b: &mut Bencher<'_>, bench: &RefCell<EncodingBench>) {
//     let mut bench = bench.borrow_mut();
//     b.iter(|| bench.pb_encoding_bench())
// }

fn bench_encoding_iter(b: &mut Bencher<'_>, bench: &RefCell<EncodingBench>) {
    let mut bench = bench.borrow_mut();
    b.iter(|| bench.raw_bytes_bench())
}

fn bench_encoding(c: &mut Criterion) {
    let config = init_bench();

    let mut group = c.benchmark_group("bench_encoding");

    group.measurement_time(config.encoding_bench.bench_measurement_time.0);
    group.sample_size(config.encoding_bench.bench_sample_size);

    let bench = RefCell::new(EncodingBench::new(config.encoding_bench));
    // group.bench_with_input(
    //     BenchmarkId::new("protobuf_encoding", 0),
    //     &bench,
    //     bench_encoding_iter,
    // );
    group.bench_with_input(
        BenchmarkId::new("new_format_encoding", 0),
        &bench,
        bench_encoding_iter,
    );
    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default();
    targets = bench_encoding,
);

criterion_main!(benches);
