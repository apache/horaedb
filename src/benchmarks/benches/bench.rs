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
    remote_write_bench::RemoteWriteBench,
};
use criterion::*;

static INIT_LOG: Once = Once::new();

pub fn init_bench() -> BenchConfig {
    INIT_LOG.call_once(|| {
        // install global collector configured based on RUST_LOG env var.
        tracing_subscriber::fmt::init();
    });

    config::config_from_env()
}

fn bench_manifest_encoding(c: &mut Criterion) {
    let config = init_bench();

    let mut group = c.benchmark_group("manifest_encoding");

    group.measurement_time(config.manifest.bench_measurement_time.0);
    group.sample_size(config.manifest.bench_sample_size);

    let bench = RefCell::new(EncodingBench::new(config.manifest));
    group.bench_with_input(
        BenchmarkId::new("snapshot_encoding", 0),
        &bench,
        |b, bench| {
            let mut bench = bench.borrow_mut();
            b.iter(|| bench.raw_bytes_bench())
        },
    );
    group.finish();
}

fn bench_remote_write(c: &mut Criterion) {
    let config = init_bench();

    let sequential_scales = config.remote_write.sequential_scales.clone();
    let concurrent_scales = config.remote_write.concurrent_scales.clone();
    let bench = RefCell::new(RemoteWriteBench::new(config.remote_write));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // Sequential parse bench.
    let mut group = c.benchmark_group("remote_write_sequential");

    for &n in &sequential_scales {
        group.bench_with_input(
            BenchmarkId::new("prost", n),
            &(&bench, n),
            |b, (bench, scale)| {
                let bench = bench.borrow();
                b.iter(|| bench.prost_parser_sequential(*scale).unwrap())
            },
        );

        group.bench_with_input(
            BenchmarkId::new("pooled", n),
            &(&bench, &rt, n),
            |b, (bench, rt, scale)| {
                let bench = bench.borrow();
                b.iter(|| rt.block_on(bench.pooled_parser_sequential(*scale)).unwrap())
            },
        );

        group.bench_with_input(
            BenchmarkId::new("quick_protobuf", n),
            &(&bench, n),
            |b, (bench, scale)| {
                let bench = bench.borrow();
                b.iter(|| bench.quick_protobuf_parser_sequential(*scale).unwrap())
            },
        );

        group.bench_with_input(
            BenchmarkId::new("rust_protobuf", n),
            &(&bench, n),
            |b, (bench, scale)| {
                let bench = bench.borrow();
                b.iter(|| bench.rust_protobuf_parser_sequential(*scale).unwrap())
            },
        );
    }
    group.finish();

    // Concurrent parse bench.
    let mut group = c.benchmark_group("remote_write_concurrent");

    for &scale in &concurrent_scales {
        group.bench_with_input(
            BenchmarkId::new("prost", scale),
            &(&bench, &rt, scale),
            |b, (bench, rt, scale)| {
                let bench = bench.borrow();
                b.iter(|| rt.block_on(bench.prost_parser_concurrent(*scale)).unwrap())
            },
        );

        group.bench_with_input(
            BenchmarkId::new("pooled", scale),
            &(&bench, &rt, scale),
            |b, (bench, rt, scale)| {
                let bench = bench.borrow();
                b.iter(|| rt.block_on(bench.pooled_parser_concurrent(*scale)).unwrap())
            },
        );

        group.bench_with_input(
            BenchmarkId::new("quick_protobuf", scale),
            &(&bench, &rt, scale),
            |b, (bench, rt, scale)| {
                let bench = bench.borrow();
                b.iter(|| {
                    rt.block_on(bench.quick_protobuf_parser_concurrent(*scale))
                        .unwrap()
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("rust_protobuf", scale),
            &(&bench, &rt, scale),
            |b, (bench, rt, scale)| {
                let bench = bench.borrow();
                b.iter(|| {
                    rt.block_on(bench.rust_protobuf_parser_concurrent(*scale))
                        .unwrap()
                })
            },
        );
    }
    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default();
    targets = bench_manifest_encoding, bench_remote_write,
);

criterion_main!(benches);
