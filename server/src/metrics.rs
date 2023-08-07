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

//! Metrics util for server.

use lazy_static::lazy_static;
use log::warn;
use prometheus::{exponential_buckets, register_histogram_vec, Encoder, HistogramVec, TextEncoder};

lazy_static! {
    pub static ref HTTP_HANDLER_DURATION_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "http_handler_duration",
        "Bucketed histogram of http server handler",
        &["path", "code"],
        // 0.01s, 0.02s, ... 163.84s
        exponential_buckets(0.01, 2.0, 15).unwrap()
    )
    .unwrap();
}

/// Gather and dump prometheus to string.
pub fn dump() -> String {
    let mut buffer = vec![];
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    for mf in metric_families {
        if let Err(e) = encoder.encode(&[mf], &mut buffer) {
            warn!("prometheus encoding error, err:{}", e);
        }
    }
    String::from_utf8(buffer).unwrap()
}
