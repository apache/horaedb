// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Metrics util for server.

use log::warn;
use prometheus::{Encoder, TextEncoder};

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
