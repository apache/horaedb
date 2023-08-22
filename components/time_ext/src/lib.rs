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

//! Time utilities

// TODO(yingwen): Move to common_types ?

use std::{
    convert::TryInto,
    fmt::{self, Write},
    ops::{Add, AddAssign, Div, DivAssign, Mul, MulAssign, Sub, SubAssign},
    str::FromStr,
    time::{Duration, Instant, UNIX_EPOCH},
};

use ceresdbproto::manifest as manifest_pb;
use chrono::{DateTime, Utc};
use common_types::time::Timestamp;
use macros::define_result;
use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use snafu::{Backtrace, GenerateBacktrace, Snafu};

#[derive(Debug, Snafu)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("Failed to parse duration, err:{}.\nBacktrace:\n{}", err, backtrace))]
    ParseDuration { err: String, backtrace: Backtrace },
}

define_result!(Error);

const TIME_MAGNITUDE_1: u64 = 1000;
const TIME_MAGNITUDE_2: u64 = 60;
const TIME_MAGNITUDE_3: u64 = 24;
const UNIT: u64 = 1;
const MS: u64 = UNIT;
const SECOND: u64 = MS * TIME_MAGNITUDE_1;
const MINUTE: u64 = SECOND * TIME_MAGNITUDE_2;
const HOUR: u64 = MINUTE * TIME_MAGNITUDE_2;
const DAY: u64 = HOUR * TIME_MAGNITUDE_3;

/// Convert Duration to milliseconds.
///
/// Panic if overflow. Mainly used by `ReadableDuration`.
#[inline]
fn duration_to_ms(d: Duration) -> u64 {
    let nanos = u64::from(d.subsec_nanos());
    // Most of case, we can't have so large Duration, so here just panic if overflow
    // now.
    d.as_secs() * 1_000 + (nanos / 1_000_000)
}

#[derive(Clone, Debug, Copy, PartialEq, Eq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TimeUnit {
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Seconds,
    Minutes,
    Hours,
    Days,
}

impl From<TimeUnit> for manifest_pb::TimeUnit {
    fn from(unit: TimeUnit) -> Self {
        match unit {
            TimeUnit::Nanoseconds => manifest_pb::TimeUnit::Nanoseconds,
            TimeUnit::Microseconds => manifest_pb::TimeUnit::Microseconds,
            TimeUnit::Milliseconds => manifest_pb::TimeUnit::Milliseconds,
            TimeUnit::Seconds => manifest_pb::TimeUnit::Seconds,
            TimeUnit::Minutes => manifest_pb::TimeUnit::Minutes,
            TimeUnit::Hours => manifest_pb::TimeUnit::Hours,
            TimeUnit::Days => manifest_pb::TimeUnit::Days,
        }
    }
}

impl From<manifest_pb::TimeUnit> for TimeUnit {
    fn from(unit: manifest_pb::TimeUnit) -> Self {
        match unit {
            manifest_pb::TimeUnit::Nanoseconds => TimeUnit::Nanoseconds,
            manifest_pb::TimeUnit::Microseconds => TimeUnit::Microseconds,
            manifest_pb::TimeUnit::Milliseconds => TimeUnit::Milliseconds,
            manifest_pb::TimeUnit::Seconds => TimeUnit::Seconds,
            manifest_pb::TimeUnit::Minutes => TimeUnit::Minutes,
            manifest_pb::TimeUnit::Hours => TimeUnit::Hours,
            manifest_pb::TimeUnit::Days => TimeUnit::Days,
        }
    }
}

impl FromStr for TimeUnit {
    type Err = String;

    fn from_str(tu_str: &str) -> std::result::Result<TimeUnit, String> {
        let tu_str = tu_str.trim();
        if !tu_str.is_ascii() {
            return Err(format!("unexpected ascii string: {tu_str}"));
        }

        match tu_str.to_lowercase().as_str() {
            "nanoseconds" => Ok(TimeUnit::Nanoseconds),
            "microseconds" => Ok(TimeUnit::Microseconds),
            "milliseconds" => Ok(TimeUnit::Milliseconds),
            "seconds" => Ok(TimeUnit::Seconds),
            "minutes" => Ok(TimeUnit::Minutes),
            "hours" => Ok(TimeUnit::Hours),
            "days" => Ok(TimeUnit::Days),
            _ => Err(format!("unexpected TimeUnit: {tu_str}")),
        }
    }
}

impl fmt::Display for TimeUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            TimeUnit::Nanoseconds => "nanoseconds",
            TimeUnit::Microseconds => "microseconds",
            TimeUnit::Milliseconds => "milliseconds",
            TimeUnit::Seconds => "seconds",
            TimeUnit::Minutes => "minutes",
            TimeUnit::Hours => "hours",
            TimeUnit::Days => "days",
        };
        write!(f, "{s}")
    }
}

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Default)]
pub struct ReadableDuration(pub Duration);

impl Add for ReadableDuration {
    type Output = ReadableDuration;

    fn add(self, rhs: ReadableDuration) -> ReadableDuration {
        Self(self.0 + rhs.0)
    }
}

impl AddAssign for ReadableDuration {
    fn add_assign(&mut self, rhs: ReadableDuration) {
        *self = *self + rhs;
    }
}

impl Sub for ReadableDuration {
    type Output = ReadableDuration;

    fn sub(self, rhs: ReadableDuration) -> ReadableDuration {
        Self(self.0 - rhs.0)
    }
}

impl SubAssign for ReadableDuration {
    fn sub_assign(&mut self, rhs: ReadableDuration) {
        *self = *self - rhs;
    }
}

impl Mul<u32> for ReadableDuration {
    type Output = ReadableDuration;

    fn mul(self, rhs: u32) -> Self::Output {
        Self(self.0 * rhs)
    }
}

impl MulAssign<u32> for ReadableDuration {
    fn mul_assign(&mut self, rhs: u32) {
        *self = *self * rhs;
    }
}

impl Div<u32> for ReadableDuration {
    type Output = ReadableDuration;

    fn div(self, rhs: u32) -> ReadableDuration {
        Self(self.0 / rhs)
    }
}

impl DivAssign<u32> for ReadableDuration {
    fn div_assign(&mut self, rhs: u32) {
        *self = *self / rhs;
    }
}

impl From<ReadableDuration> for Duration {
    fn from(readable: ReadableDuration) -> Duration {
        readable.0
    }
}

// yingwen: Support From<Duration>.
impl From<Duration> for ReadableDuration {
    fn from(t: Duration) -> ReadableDuration {
        ReadableDuration(t)
    }
}

impl FromStr for ReadableDuration {
    type Err = String;

    fn from_str(dur_str: &str) -> std::result::Result<ReadableDuration, String> {
        let dur_str = dur_str.trim();
        if !dur_str.is_ascii() {
            return Err(format!("unexpected ascii string: {dur_str}"));
        }
        let err_msg = "valid duration, only d, h, m, s, ms are supported.".to_owned();
        let mut left = dur_str.as_bytes();
        let mut last_unit = DAY + 1;
        let mut dur = 0f64;
        while let Some(idx) = left.iter().position(|c| b"dhms".contains(c)) {
            let (first, second) = left.split_at(idx);
            let unit = if second.starts_with(b"ms") {
                left = &left[idx + 2..];
                MS
            } else {
                let u = match second[0] {
                    b'd' => DAY,
                    b'h' => HOUR,
                    b'm' => MINUTE,
                    b's' => SECOND,
                    _ => return Err(err_msg),
                };
                left = &left[idx + 1..];
                u
            };
            if unit >= last_unit {
                return Err("d, h, m, s, ms should occur in given order.".to_owned());
            }
            // do we need to check 12h360m?
            let number_str = unsafe { std::str::from_utf8_unchecked(first) };
            dur += match number_str.trim().parse::<f64>() {
                Ok(n) => n * unit as f64,
                Err(_) => return Err(err_msg),
            };
            last_unit = unit;
        }
        if !left.is_empty() {
            return Err(err_msg);
        }
        if dur.is_sign_negative() {
            return Err("duration should be positive.".to_owned());
        }
        let secs = dur as u64 / SECOND;
        let millis = (dur as u64 % SECOND) as u32 * 1_000_000;
        Ok(ReadableDuration(Duration::new(secs, millis)))
    }
}

impl ReadableDuration {
    pub const fn secs(secs: u64) -> ReadableDuration {
        ReadableDuration(Duration::from_secs(secs))
    }

    pub const fn millis(millis: u64) -> ReadableDuration {
        ReadableDuration(Duration::from_millis(millis))
    }

    pub const fn minutes(minutes: u64) -> ReadableDuration {
        ReadableDuration::secs(minutes * 60)
    }

    pub const fn hours(hours: u64) -> ReadableDuration {
        ReadableDuration::minutes(hours * 60)
    }

    pub const fn days(days: u64) -> ReadableDuration {
        ReadableDuration::hours(days * 24)
    }

    pub fn as_secs(&self) -> u64 {
        self.0.as_secs()
    }

    pub fn as_millis(&self) -> u64 {
        duration_to_ms(self.0)
    }

    pub fn is_zero(&self) -> bool {
        self.0.as_nanos() == 0
    }
}

impl fmt::Display for ReadableDuration {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut dur = duration_to_ms(self.0);
        let mut written = false;
        if dur >= DAY {
            written = true;
            write!(f, "{}d", dur / DAY)?;
            dur %= DAY;
        }
        if dur >= HOUR {
            written = true;
            write!(f, "{}h", dur / HOUR)?;
            dur %= HOUR;
        }
        if dur >= MINUTE {
            written = true;
            write!(f, "{}m", dur / MINUTE)?;
            dur %= MINUTE;
        }
        if dur >= SECOND {
            written = true;
            write!(f, "{}s", dur / SECOND)?;
            dur %= SECOND;
        }
        if dur > 0 {
            written = true;
            write!(f, "{dur}ms")?;
        }
        if !written {
            write!(f, "0s")?;
        }
        Ok(())
    }
}

impl Serialize for ReadableDuration {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut buffer = String::new();
        write!(buffer, "{self}").unwrap();
        serializer.serialize_str(&buffer)
    }
}

impl<'de> Deserialize<'de> for ReadableDuration {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DurVisitor;

        impl<'de> Visitor<'de> for DurVisitor {
            type Value = ReadableDuration;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("valid duration")
            }

            fn visit_str<E>(self, dur_str: &str) -> std::result::Result<ReadableDuration, E>
            where
                E: de::Error,
            {
                dur_str.parse().map_err(E::custom)
            }
        }

        deserializer.deserialize_str(DurVisitor)
    }
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

    #[test]
    fn test_duration_construction() {
        let mut dur = ReadableDuration::secs(1);
        assert_eq!(dur.0, Duration::new(1, 0));
        assert_eq!(dur.as_secs(), 1);
        assert_eq!(dur.as_millis(), 1000);
        dur = ReadableDuration::millis(1001);
        assert_eq!(dur.0, Duration::new(1, 1_000_000));
        assert_eq!(dur.as_secs(), 1);
        assert_eq!(dur.as_millis(), 1001);
        dur = ReadableDuration::minutes(2);
        assert_eq!(dur.0, Duration::new(2 * 60, 0));
        assert_eq!(dur.as_secs(), 120);
        assert_eq!(dur.as_millis(), 120000);
        dur = ReadableDuration::hours(2);
        assert_eq!(dur.0, Duration::new(2 * 3600, 0));
        assert_eq!(dur.as_secs(), 7200);
        assert_eq!(dur.as_millis(), 7200000);
    }

    #[test]
    fn test_parse_readable_duration() {
        #[derive(Serialize, Deserialize)]
        struct DurHolder {
            d: ReadableDuration,
        }

        let legal_cases = vec![
            (0, 0, "0s"),
            (0, 1, "1ms"),
            (2, 0, "2s"),
            (24 * 3600, 0, "1d"),
            (2 * 24 * 3600, 10, "2d10ms"),
            (4 * 60, 0, "4m"),
            (5 * 3600, 0, "5h"),
            (3600 + 2 * 60, 0, "1h2m"),
            (5 * 24 * 3600 + 3600 + 2 * 60, 0, "5d1h2m"),
            (3600 + 2, 5, "1h2s5ms"),
            (3 * 24 * 3600 + 7 * 3600 + 2, 5, "3d7h2s5ms"),
        ];
        for (secs, ms, exp) in legal_cases {
            let d = DurHolder {
                d: ReadableDuration(Duration::new(secs, ms * 1_000_000)),
            };
            let res_str = toml::to_string(&d).unwrap();
            let exp_str = format!("d = {exp:?}\n");
            assert_eq!(res_str, exp_str);
            let res_dur: DurHolder = toml::from_str(&exp_str).unwrap();
            assert_eq!(res_dur.d.0, d.d.0);
        }

        let decode_cases = vec![(" 0.5 h2m ", 3600 / 2 + 2 * 60, 0)];
        for (src, secs, ms) in decode_cases {
            let src = format!("d = {src:?}");
            let res: DurHolder = toml::from_str(&src).unwrap();
            assert_eq!(res.d.0, Duration::new(secs, ms * 1_000_000));
        }

        let illegal_cases = vec!["1H", "1M", "1S", "1MS", "1h1h", "h"];
        for src in illegal_cases {
            let src_str = format!("d = {src:?}");
            assert!(toml::from_str::<DurHolder>(&src_str).is_err(), "{}", src);
        }
        assert!(toml::from_str::<DurHolder>("d = 23").is_err());
    }

    #[test]
    fn test_parse_timeunit() {
        let s = "milliseconds";
        assert_eq!(TimeUnit::Milliseconds, s.parse::<TimeUnit>().unwrap());
        let s = "seconds";
        assert_eq!(TimeUnit::Seconds, s.parse::<TimeUnit>().unwrap());
        let s = "minutes";
        assert_eq!(TimeUnit::Minutes, s.parse::<TimeUnit>().unwrap());
        let s = "hours";
        assert_eq!(TimeUnit::Hours, s.parse::<TimeUnit>().unwrap());
        let s = "days";
        assert_eq!(TimeUnit::Days, s.parse::<TimeUnit>().unwrap());
        let s = "microseconds";
        assert_eq!(TimeUnit::Microseconds, s.parse::<TimeUnit>().unwrap());
    }
}
