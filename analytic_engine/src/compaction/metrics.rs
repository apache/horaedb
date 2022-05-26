// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Metrics of compaction.

use lazy_static::lazy_static;
use prometheus::{register_int_gauge, IntGauge};

lazy_static! {
    // Counters:
    pub static ref COMPACTION_PENDING_REQUEST_GAUGE: IntGauge = register_int_gauge!(
        "compaction_pending_request_gauge",
        "Pending request queue length of compaction"
    )
        .unwrap();
}
