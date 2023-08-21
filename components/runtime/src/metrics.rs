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

use lazy_static::lazy_static;
use prometheus::{register_int_gauge_vec, IntGauge, IntGaugeVec};

lazy_static! {
    // Gauges:
    static ref RUNTIME_THREAD_ALIVE_GAUGE: IntGaugeVec = register_int_gauge_vec!(
        "runtime_thread_alive_gauge",
        "alive thread number for runtime",
        &["name"]
    )
        .unwrap();
    static ref RUNTIME_THREAD_IDLE_GAUGE: IntGaugeVec = register_int_gauge_vec!(
        "runtime_thread_idle_gauge",
        "idle thread number for runtime",
        &["name"]
    )
        .unwrap();
}

/// Runtime metrics.
#[derive(Debug)]
pub struct Metrics {
    // Gauges:
    pub thread_alive_gauge: IntGauge,
    pub thread_idle_gauge: IntGauge,
}

impl Metrics {
    pub fn new(name: &str) -> Self {
        Self {
            thread_alive_gauge: RUNTIME_THREAD_ALIVE_GAUGE.with_label_values(&[name]),
            thread_idle_gauge: RUNTIME_THREAD_IDLE_GAUGE.with_label_values(&[name]),
        }
    }

    #[inline]
    pub fn on_thread_start(&self) {
        self.thread_alive_gauge.inc();
    }

    #[inline]
    pub fn on_thread_stop(&self) {
        self.thread_alive_gauge.dec();
    }

    #[inline]
    pub fn on_thread_park(&self) {
        self.thread_idle_gauge.inc();
    }

    #[inline]
    pub fn on_thread_unpark(&self) {
        self.thread_idle_gauge.dec();
    }
}
