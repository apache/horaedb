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

//! This test is used to verify the correctness of the pooled parser rigorously
//! (50 iterations, 25 iterations for each input data), by comparing the results
//! with the prost parser.
//!
//! Test with `--features unsafe-split` to enable the unsafe optimization.

use std::{fs, sync::Arc, thread};

use bytes::Bytes;
use pb_types::{Exemplar, Label, MetricMetadata, Sample, TimeSeries, WriteRequest};
use prost::Message;
use remote_write::pooled_parser::PooledParser;

const ITERATIONS: usize = 50;

fn load_test_data() -> (Bytes, Bytes) {
    let data1 = Bytes::from(
        fs::read("tests/workloads/1709380533560664458.data").expect("test data load failed"),
    );

    let data2 = Bytes::from(
        fs::read("tests/workloads/1709380533705807779.data").expect("test data load failed"),
    );

    (data1, data2)
}

fn parse_with_prost(data: &Bytes) -> WriteRequest {
    WriteRequest::decode(data.clone()).expect("prost decode failed")
}

fn parse_with_pooled(data: &Bytes) -> WriteRequest {
    let data_copy = data.clone();
    let parser = PooledParser;
    let pooled_request = parser.decode(data_copy).expect("pooled decode failed");

    // Convert pooled types to pb_types to compare with prost.
    let mut write_request = WriteRequest {
        timeseries: Vec::new(),
        metadata: Vec::new(),
    };

    for pooled_ts in &pooled_request.timeseries {
        let mut timeseries = TimeSeries {
            labels: Vec::new(),
            samples: Vec::new(),
            exemplars: Vec::new(),
        };

        for pooled_label in &pooled_ts.labels {
            timeseries.labels.push(Label {
                name: String::from_utf8_lossy(&pooled_label.name).to_string(),
                value: String::from_utf8_lossy(&pooled_label.value).to_string(),
            });
        }

        for pooled_sample in &pooled_ts.samples {
            timeseries.samples.push(Sample {
                value: pooled_sample.value,
                timestamp: pooled_sample.timestamp,
            });
        }

        for pooled_exemplar in &pooled_ts.exemplars {
            let mut exemplar = Exemplar {
                labels: Vec::new(),
                value: pooled_exemplar.value,
                timestamp: pooled_exemplar.timestamp,
            };

            for pooled_label in &pooled_exemplar.labels {
                exemplar.labels.push(Label {
                    name: String::from_utf8_lossy(&pooled_label.name).to_string(),
                    value: String::from_utf8_lossy(&pooled_label.value).to_string(),
                });
            }

            timeseries.exemplars.push(exemplar);
        }

        write_request.timeseries.push(timeseries);
    }

    for pooled_metadata in pooled_request.metadata.iter() {
        let metadata = MetricMetadata {
            r#type: pooled_metadata.metric_type as i32,
            metric_family_name: String::from_utf8_lossy(&pooled_metadata.metric_family_name)
                .to_string(),
            help: String::from_utf8_lossy(&pooled_metadata.help).to_string(),
            unit: String::from_utf8_lossy(&pooled_metadata.unit).to_string(),
        };

        write_request.metadata.push(metadata);
    }

    // pooled_request will be dropped here and returned to the pool.
    write_request
}

#[test]
fn test_sequential_correctness() {
    let (data1, data2) = load_test_data();
    let datasets = [&data1, &data2];

    for iteration in 0..ITERATIONS {
        let data_index = iteration % 2;
        let data = datasets[data_index];

        let prost_result = parse_with_prost(data);
        let pooled_result = parse_with_pooled(data);

        assert_eq!(
            &prost_result, &pooled_result,
            "Data {} WriteRequest mismatch",
            data_index
        );
    }
}

#[test]
fn test_concurrent_correctness() {
    let (data1, data2) = load_test_data();
    let data1 = Arc::new(data1);
    let data2 = Arc::new(data2);

    let mut handles = Vec::new();

    for iteration in 0..ITERATIONS {
        let data1_clone = Arc::clone(&data1);
        let data2_clone = Arc::clone(&data2);

        let handle = thread::spawn(move || {
            let data_index = iteration % 2;
            let data = if data_index == 0 {
                &*data1_clone
            } else {
                &*data2_clone
            };

            let prost_result = parse_with_prost(data);
            let pooled_result = parse_with_pooled(data);

            assert_eq!(
                &prost_result, &pooled_result,
                "Data {} WriteRequest mismatch",
                data_index
            );
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("thread completion failed");
    }
}
