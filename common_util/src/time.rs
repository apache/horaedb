// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Time utilities

// TODO(yingwen): Move to common_types ?

use std::{
    convert::TryInto,
    time::{Duration, Instant, UNIX_EPOCH},
};

use chrono::{DateTime, Utc};
use common_types::time::Timestamp;
use snafu::{Backtrace, GenerateBacktrace, Snafu};

use crate::config::ReadableDuration;

#[derive(Debug, Snafu)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("Failed to parse duration, err:{}.\nBacktrace:\n{}", err, backtrace))]
    ParseDuration { err: String, backtrace: Backtrace },
}
define_result!(Error);

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

    /// Check whether this instant is reached
    fn check_deadline(&self) -> bool;
}

impl InstantExt for Instant {
    fn saturating_elapsed(&self) -> Duration {
        Instant::now().saturating_duration_since(*self)
    }

    fn check_deadline(&self) -> bool {
        self.saturating_elapsed().is_zero()
    }
}

#[inline]
pub fn secs_to_nanos(s: u64) -> u64 {
    s * 1_000_000_000
}

#[inline]
pub fn current_time_millis() -> u64 {
    Utc::now().timestamp_millis() as u64
}

#[inline]
pub fn current_as_rfc3339() -> String {
    Utc::now().to_rfc3339()
}

#[inline]
pub fn format_as_ymdhms(unix_timestamp: i64) -> String {
    let dt = DateTime::<Utc>::from(UNIX_EPOCH + Duration::from_millis(unix_timestamp as u64));
    dt.format("%Y-%m-%d %H:%M:%S").to_string()
}

pub fn try_to_millis(ts: i64) -> Option<Timestamp> {
    // https://help.aliyun.com/document_detail/60683.html
    if (4294968..=4294967295).contains(&ts) {
        return Some(Timestamp::new(ts * 1000));
    }
    if (4294967296..=9999999999999).contains(&ts) {
        return Some(Timestamp::new(ts));
    }
    None
}

pub fn parse_duration(v: &str) -> Result<ReadableDuration> {
    v.parse::<ReadableDuration>()
        .map_err(|err| Error::ParseDuration {
            err,
            backtrace: Backtrace::generate(),
        })
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
