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

use std::time::{Duration, Instant};

/// Timer for collecting slow query
#[derive(Debug)]
pub(crate) struct SlowTimer {
    slow_threshold: Duration,
    timer: Instant,
}

impl SlowTimer {
    pub fn new(threshold: Duration) -> SlowTimer {
        SlowTimer {
            slow_threshold: threshold,
            timer: Instant::now(),
        }
    }

    pub fn elapsed(&self) -> Duration {
        self.timer.elapsed()
    }

    pub fn is_slow(&self) -> bool {
        self.elapsed() >= self.slow_threshold
    }

    pub fn now(&self) -> Instant {
        self.timer
    }
}

#[macro_export]
macro_rules! maybe_slow_query {
    ($t:expr, $($args:tt)*) => {{
        if $t.is_slow() {
            info!(target: "slow", $($args)*);
        }
    }}
}

#[macro_export]
macro_rules! failed_query {
    ($($args:tt)*) => {{
        info!(target: "failed", $($args)*);
    }}
}
