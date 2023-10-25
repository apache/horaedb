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

use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

use chrono::prelude::*;

// Cache the provided value and update it in a sampling rate.
pub struct SamplingCachedUsize {
    interval_ms: i64,

    last_updated_at: AtomicI64,
    cached_val: AtomicUsize,
}

impl SamplingCachedUsize {
    pub fn new(interval_ms: i64) -> Self {
        Self {
            interval_ms,
            last_updated_at: AtomicI64::default(),
            cached_val: AtomicUsize::default(),
        }
    }

    /// Read the cached value and update it by the `val_source` if necessary.
    ///
    /// The returned error only results from the `val_source`.
    pub fn read<F, E>(&self, val_source: F) -> std::result::Result<usize, E>
    where
        F: FnOnce() -> std::result::Result<usize, E>,
    {
        let now_ms = Utc::now().timestamp_millis();
        let last_updated_at = self.last_updated_at.load(Ordering::Relaxed);
        let deadline_ms = last_updated_at + self.interval_ms;

        assert!(deadline_ms >= 0);
        if now_ms >= deadline_ms {
            let new_value = val_source()?;
            self.last_updated_at.store(now_ms, Ordering::Relaxed);
            self.cached_val.store(new_value, Ordering::Relaxed);
            Ok(new_value)
        } else {
            Ok(self.cached_val.load(Ordering::Relaxed))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Mutex, time::Duration};

    use super::*;

    #[derive(Default)]
    struct ValueSource {
        val: Mutex<usize>,
    }

    impl ValueSource {
        fn get(&self) -> std::result::Result<usize, ()> {
            Ok(*self.val.lock().unwrap())
        }

        fn inc(&self) {
            let mut val = self.val.lock().unwrap();
            *val += 1;
        }
    }

    #[test]
    fn test_always_update() {
        let updater = SamplingCachedUsize::new(0);
        let val_source = ValueSource::default();

        let v = updater.read(|| val_source.get()).unwrap();
        assert_eq!(v, 0);
        val_source.inc();
        let v = updater.read(|| val_source.get()).unwrap();
        assert_eq!(v, 1);
    }

    #[test]
    fn test_normal_update() {
        let interval_ms = 100i64;
        let interval = Duration::from_millis(interval_ms as u64);
        let updater = SamplingCachedUsize::new(interval_ms);
        let val_source = ValueSource::default();

        let v = updater.read(|| val_source.get()).unwrap();
        assert_eq!(v, 0);
        val_source.inc();
        let v = updater.read(|| val_source.get()).unwrap();
        assert_eq!(v, 0);

        std::thread::sleep(interval / 2);
        let v = updater.read(|| val_source.get()).unwrap();
        assert_eq!(v, 0);

        std::thread::sleep(interval / 3);
        let v = updater.read(|| val_source.get()).unwrap();
        assert_eq!(v, 0);

        std::thread::sleep(interval / 2);
        let v = updater.read(|| val_source.get()).unwrap();
        assert_eq!(v, 1);
    }
}
