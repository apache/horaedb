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

/// Copied from https://github.com/sunng87/metriki/blob/master/metriki-core/src/metrics/meter.rs
/// But supports 1 hour and 2 hour rate.
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use crossbeam_utils::atomic::AtomicCell;
#[cfg(feature = "ser")]
use serde::ser::SerializeMap;
#[cfg(feature = "ser")]
use serde::{Serialize, Serializer};

/// Meters are used to calculate rate of an event.
#[derive(Debug)]
pub struct Meter {
    moving_averages: ExponentiallyWeightedMovingAverages,
    count: AtomicU64,
    start_time: SystemTime,
}

impl Default for Meter {
    fn default() -> Self {
        Self::new()
    }
}

impl Meter {
    pub fn new() -> Meter {
        Meter {
            moving_averages: ExponentiallyWeightedMovingAverages::new(),
            count: AtomicU64::from(0),
            start_time: SystemTime::now(),
        }
    }

    pub fn mark(&self) {
        self.mark_n(1)
    }

    pub fn mark_n(&self, n: u64) {
        self.count.fetch_add(n, Ordering::Relaxed);
        self.moving_averages.tick_if_needed();
        self.moving_averages.update(n);
    }

    pub fn h1_rate(&self) -> f64 {
        self.moving_averages.tick_if_needed();
        self.moving_averages.h1_rate()
    }

    pub fn h2_rate(&self) -> f64 {
        self.moving_averages.tick_if_needed();
        self.moving_averages.h2_rate()
    }

    pub fn m15_rate(&self) -> f64 {
        self.moving_averages.tick_if_needed();
        self.moving_averages.m15_rate()
    }

    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    pub fn mean_rate(&self) -> f64 {
        let count = self.count();
        if count > 0 {
            if let Ok(elapsed) = SystemTime::now()
                .duration_since(self.start_time)
                .map(|d| d.as_secs() as f64)
            {
                count as f64 / elapsed
            } else {
                0f64
            }
        } else {
            0f64
        }
    }
}

#[derive(Debug)]
struct ExponentiallyWeightedMovingAverage {
    alpha: f64,
    interval_nanos: u64,

    uncounted: AtomicCell<u64>,
    rate: AtomicCell<Option<f64>>,
}

impl ExponentiallyWeightedMovingAverage {
    fn new(alpha: f64, interval_secs: u64) -> ExponentiallyWeightedMovingAverage {
        ExponentiallyWeightedMovingAverage {
            alpha,
            interval_nanos: time_ext::secs_to_nanos(interval_secs),

            uncounted: AtomicCell::new(0),
            rate: AtomicCell::new(None),
        }
    }

    fn update(&self, n: u64) {
        self.uncounted.fetch_add(n);
    }

    fn tick(&self) {
        let count = self.uncounted.swap(0);
        let instant_rate = count as f64 / self.interval_nanos as f64;

        if let Some(prev_rate) = self.rate.load() {
            let new_rate = prev_rate + (self.alpha * (instant_rate - prev_rate));
            self.rate.store(Some(new_rate));
        } else {
            self.rate.store(Some(instant_rate));
        }
    }

    fn get_rate(&self) -> f64 {
        if let Some(rate) = self.rate.load() {
            rate * time_ext::secs_to_nanos(1) as f64
        } else {
            0f64
        }
    }
}

#[derive(Debug)]
struct ExponentiallyWeightedMovingAverages {
    h1: ExponentiallyWeightedMovingAverage,
    h2: ExponentiallyWeightedMovingAverage,
    m15: ExponentiallyWeightedMovingAverage,

    last_tick: AtomicCell<Instant>,
}

#[inline]
fn alpha(interval_secs: u64, minutes: u64) -> f64 {
    1.0 - (-(interval_secs as f64) / 60.0 / minutes as f64).exp()
}

const DEFAULT_INTERVAL_SECS: u64 = 5;
const DEFAULT_INTERVAL_MILLIS: u64 = DEFAULT_INTERVAL_SECS * 1000;

impl ExponentiallyWeightedMovingAverages {
    fn new() -> ExponentiallyWeightedMovingAverages {
        ExponentiallyWeightedMovingAverages {
            h1: ExponentiallyWeightedMovingAverage::new(
                alpha(DEFAULT_INTERVAL_SECS, 60),
                DEFAULT_INTERVAL_SECS,
            ),

            h2: ExponentiallyWeightedMovingAverage::new(
                alpha(DEFAULT_INTERVAL_SECS, 120),
                DEFAULT_INTERVAL_SECS,
            ),

            m15: ExponentiallyWeightedMovingAverage::new(
                alpha(DEFAULT_INTERVAL_SECS, 15),
                DEFAULT_INTERVAL_SECS,
            ),

            last_tick: AtomicCell::new(Instant::now()),
        }
    }

    fn update(&self, n: u64) {
        self.h1.update(n);
        self.h2.update(n);
        self.m15.update(n);
    }

    fn tick_if_needed(&self) {
        let previous_tick = self.last_tick.load();
        let current_tick = Instant::now();

        let tick_age = (current_tick - previous_tick).as_millis() as u64;

        if tick_age > DEFAULT_INTERVAL_MILLIS {
            let latest_tick =
                current_tick - Duration::from_millis(tick_age % DEFAULT_INTERVAL_MILLIS);
            if self
                .last_tick
                .compare_exchange(previous_tick, latest_tick)
                .is_ok()
            {
                let required_ticks = tick_age / DEFAULT_INTERVAL_MILLIS;
                for _ in 0..required_ticks {
                    self.h1.tick();
                    self.h2.tick();
                    self.m15.tick();
                }
            }
        }
    }

    fn h1_rate(&self) -> f64 {
        self.h1.get_rate()
    }

    fn h2_rate(&self) -> f64 {
        self.h2.get_rate()
    }

    fn m15_rate(&self) -> f64 {
        self.m15.get_rate()
    }
}

#[cfg(feature = "ser")]
impl Serialize for Meter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(4))?;

        map.serialize_entry("count", &self.count())?;
        map.serialize_entry("h1_rate", &self.h1_rate())?;
        map.serialize_entry("h2_rate", &self.h2_rate())?;
        map.serialize_entry("m15_rate", &self.m15_rate())?;

        map.end()
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time};

    use super::*;

    macro_rules! assert_float_eq {
        ($left:expr, $right:expr) => {{
            match (&$left, &$right) {
                (left_val, right_val) => {
                    let diff = (left_val - right_val).abs();

                    if diff > f64::EPSILON {
                        panic!(
                            "assertion failed: `(left == right)`\n      left: `{:?}`,\n     right: `{:?}`",
                            &*left_val, &*right_val
                        )
                    }
                }
            }
        }};
    }

    #[test]
    fn test_meter() {
        let m = Meter::new();

        for _ in 0..10 {
            m.mark();
        }

        thread::sleep(time::Duration::from_millis(DEFAULT_INTERVAL_MILLIS + 10));

        assert_eq!(10, m.count());
        assert_float_eq!(2.0, m.m15_rate());
        assert_float_eq!(2.0, m.h1_rate());
        assert_float_eq!(2.0, m.h2_rate());
    }
}
