// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

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
