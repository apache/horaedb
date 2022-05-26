// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Time utilities

// TODO(yingwen): Move to common_types ?

use std::{
    convert::TryInto,
    time::{Duration, Instant},
};

pub trait DurationExt {
    /// Convert into u64.
    ///
    /// Returns u64::MAX if overflow
    fn as_millis_u64(&self) -> u64;
}

impl DurationExt for Duration {
    #[inline]
    fn as_millis_u64(&self) -> u64 {
        match self.as_millis().try_into() {
            Ok(v) => v,
            Err(_) => u64::MAX,
        }
    }
}

pub trait InstantExt {
    fn saturating_elapsed(&self) -> Duration;
}

impl InstantExt for Instant {
    fn saturating_elapsed(&self) -> Duration {
        Instant::now().saturating_duration_since(*self)
    }
}

#[inline]
pub fn secs_to_nanos(s: u64) -> u64 {
    s * 1000000000
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;
    #[test]
    fn test_as_mills_u64() {
        let d = Duration::from_millis(100);
        assert_eq!(100, d.as_millis_u64());

        let d = Duration::from_secs(100);
        assert_eq!(100000, d.as_millis_u64());
    }

    #[test]
    fn test_saturating_elapsed() {
        let ins = Instant::now();
        let one_hundred_mills = Duration::from_millis(100);
        let error = 10;
        thread::sleep(one_hundred_mills);
        assert!(ins.saturating_elapsed().as_millis_u64() - 100 < error);
        thread::sleep(one_hundred_mills);
        assert!(ins.saturating_elapsed().as_millis_u64() - 200 < 2 * error);
    }
}
