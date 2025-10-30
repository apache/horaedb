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

use anyhow::Result;
use benchmarks::util::{human_bytes, run_concurrent_threads, MemoryBenchConfig};
use bytes::Bytes;
use hotpath::{GuardBuilder, MetricType, MetricsProvider, Reporter};
use pb_types::WriteRequest as ProstWriteRequest;
use prost::Message;
use protobuf::Message as ProtobufMessage;
use quick_protobuf::{BytesReader, MessageRead};
use remote_write::pooled_parser::PooledParser;

fn main() -> Result<()> {
    let config = MemoryBenchConfig::from_args();
    let args: Vec<String> = std::env::args().collect();
    let parser = args.get(3).map(|s| s.as_str()).unwrap_or("pooled");

    let _guard = GuardBuilder::new("parser_mem")
        .reporter(Box::new(TotalAllocReporter))
        .build();

    match config.mode.as_str() {
        "sequential" => match parser {
            "pooled" => pooled_worker(config.test_data.clone(), config.scale),
            "prost" => prost_worker(config.test_data.clone(), config.scale),
            "rust-protobuf" => rust_protobuf_worker(config.test_data.clone(), config.scale),
            "quick-protobuf" => quick_protobuf_worker(config.test_data.clone(), config.scale),
            other => panic!("unknown parser: {}", other),
        },
        "concurrent" => match parser {
            "pooled" => run_concurrent_threads(config.scale, move |n| {
                pooled_worker(config.test_data.clone(), n);
                Ok(())
            })
            .map_err(anyhow::Error::msg)?,
            "prost" => run_concurrent_threads(config.scale, move |n| {
                prost_worker(config.test_data.clone(), n);
                Ok(())
            })
            .map_err(anyhow::Error::msg)?,
            "rust-protobuf" => run_concurrent_threads(config.scale, move |n| {
                rust_protobuf_worker(config.test_data.clone(), n);
                Ok(())
            })
            .map_err(anyhow::Error::msg)?,
            "quick-protobuf" => run_concurrent_threads(config.scale, move |n| {
                quick_protobuf_worker(config.test_data.clone(), n);
                Ok(())
            })
            .map_err(anyhow::Error::msg)?,
            other => panic!("unknown parser: {}", other),
        },
        _ => panic!("invalid mode"),
    }

    Ok(())
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn pooled_worker(data: Bytes, iterations: usize) {
    let parser = PooledParser;
    for _ in 0..iterations {
        let _ = parser.decode(data.clone());
    }
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn prost_worker(data: Bytes, iterations: usize) {
    for _ in 0..iterations {
        let _ = ProstWriteRequest::decode(data.clone());
    }
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn rust_protobuf_worker(data: Bytes, iterations: usize) {
    for _ in 0..iterations {
        let _ = benchmarks::rust_protobuf_remote_write::WriteRequest::parse_from_bytes(&data);
    }
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn quick_protobuf_worker(data: Bytes, iterations: usize) {
    for _ in 0..iterations {
        let mut reader = BytesReader::from_bytes(&data);
        let _ =
            benchmarks::quick_protobuf_remote_write::WriteRequest::from_reader(&mut reader, &data);
    }
}

struct TotalAllocReporter;

impl Reporter for TotalAllocReporter {
    fn report(&self, metrics: &dyn MetricsProvider<'_>) -> Result<(), Box<dyn std::error::Error>> {
        // In hotpath, each profiled function will output a row of [metrics](https://github.com/pawurb/hotpath/blob/main/hotpath-alloc-report.png):
        // [calls, avg, pXX, total, %total], we only care about the `total`
        // metric to verify our zero-allocation optimization. Since each function's
        // metrics include those of its nested calls, we need to use the
        // maximum value of the `total` field across all functions as
        // the total memory allocated during decoding.
        let mut max_total: u64 = 0;
        for values in metrics.metric_data().values() {
            if values.len() < 2 {
                continue;
            }
            let total_idx = values.len() - 2;
            if let MetricType::AllocBytes(bytes) = values[total_idx] {
                max_total = std::cmp::max(max_total, bytes);
            }
        }
        println!(
            "Total allocated bytes (cumulative): {} ({})",
            max_total,
            human_bytes(max_total)
        );
        Ok(())
    }
}
