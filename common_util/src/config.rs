// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! Configure utils

//This module is forked from tikv and remove unnecessary code.
//https://github.com/tikv/tikv/blob/HEAD/src/util/config.rs
use std::{
    fmt::{self, Write},
    ops::{Add, AddAssign, Div, DivAssign, Mul, MulAssign, Sub, SubAssign},
    str::{self, FromStr},
    time::Duration,
};

use ceresdbproto::manifest as manifest_pb;
use serde::{
    de::{self, Unexpected, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};

const UNIT: u64 = 1;

const BINARY_DATA_MAGNITUDE: u64 = 1024;
pub const B: u64 = UNIT;
pub const KIB: u64 = UNIT * BINARY_DATA_MAGNITUDE;
pub const MIB: u64 = KIB * BINARY_DATA_MAGNITUDE;
pub const GIB: u64 = MIB * BINARY_DATA_MAGNITUDE;
pub const TIB: u64 = GIB * BINARY_DATA_MAGNITUDE;
pub const PIB: u64 = TIB * BINARY_DATA_MAGNITUDE;

const TIME_MAGNITUDE_1: u64 = 1000;
const TIME_MAGNITUDE_2: u64 = 60;
const TIME_MAGNITUDE_3: u64 = 24;
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

    fn from_str(tu_str: &str) -> Result<TimeUnit, String> {
        let tu_str = tu_str.trim();
        if !tu_str.is_ascii() {
            return Err(format!("unexpect ascii string: {tu_str}"));
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

#[derive(Clone, Debug, Copy, PartialEq, Eq, PartialOrd)]
pub struct ReadableSize(pub u64);

impl ReadableSize {
    pub const fn kb(count: u64) -> ReadableSize {
        ReadableSize(count * KIB)
    }

    pub const fn mb(count: u64) -> ReadableSize {
        ReadableSize(count * MIB)
    }

    pub const fn gb(count: u64) -> ReadableSize {
        ReadableSize(count * GIB)
    }

    pub const fn as_mb(self) -> u64 {
        self.0 / MIB
    }

    pub const fn as_byte(self) -> u64 {
        self.0
    }
}

impl Div<u64> for ReadableSize {
    type Output = ReadableSize;

    fn div(self, rhs: u64) -> ReadableSize {
        ReadableSize(self.0 / rhs)
    }
}

impl Div<ReadableSize> for ReadableSize {
    type Output = u64;

    fn div(self, rhs: ReadableSize) -> u64 {
        self.0 / rhs.0
    }
}

impl Mul<u64> for ReadableSize {
    type Output = ReadableSize;

    fn mul(self, rhs: u64) -> ReadableSize {
        ReadableSize(self.0 * rhs)
    }
}

impl Serialize for ReadableSize {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let size = self.0;
        let mut buffer = String::new();
        if size == 0 {
            write!(buffer, "{size}KiB").unwrap();
        } else if size % PIB == 0 {
            write!(buffer, "{}PiB", size / PIB).unwrap();
        } else if size % TIB == 0 {
            write!(buffer, "{}TiB", size / TIB).unwrap();
        } else if size % GIB == 0 {
            write!(buffer, "{}GiB", size / GIB).unwrap();
        } else if size % MIB == 0 {
            write!(buffer, "{}MiB", size / MIB).unwrap();
        } else if size % KIB == 0 {
            write!(buffer, "{}KiB", size / KIB).unwrap();
        } else {
            return serializer.serialize_u64(size);
        }
        serializer.serialize_str(&buffer)
    }
}

impl FromStr for ReadableSize {
    type Err = String;

    // This method parses value in binary unit.
    fn from_str(s: &str) -> Result<ReadableSize, String> {
        let size_str = s.trim();
        if size_str.is_empty() {
            return Err(format!("{s:?} is not a valid size."));
        }

        if !size_str.is_ascii() {
            return Err(format!("ASCII string is expected, but got {s:?}"));
        }

        // size: digits and '.' as decimal separator
        let size_len = size_str
            .to_string()
            .chars()
            .take_while(|c| char::is_ascii_digit(c) || ['.', 'e', 'E', '-', '+'].contains(c))
            .count();

        // unit: alphabetic characters
        let (size, unit) = size_str.split_at(size_len);

        let unit = match unit.trim() {
            "K" | "KB" | "KiB" => KIB,
            "M" | "MB" | "MiB" => MIB,
            "G" | "GB" | "GiB" => GIB,
            "T" | "TB" | "TiB" => TIB,
            "P" | "PB" | "PiB" => PIB,
            "B" | "" => UNIT,
            _ => {
                return Err(format!(
                    "only B, KB, KiB, MB, MiB, GB, GiB, TB, TiB, PB, and PiB are supported: {s:?}"
                ));
            }
        };

        match size.parse::<f64>() {
            Ok(n) => Ok(ReadableSize((n * unit as f64) as u64)),
            Err(_) => Err(format!("invalid size string: {s:?}")),
        }
    }
}

impl<'de> Deserialize<'de> for ReadableSize {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SizeVisitor;

        impl<'de> Visitor<'de> for SizeVisitor {
            type Value = ReadableSize;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("valid size")
            }

            fn visit_i64<E>(self, size: i64) -> Result<ReadableSize, E>
            where
                E: de::Error,
            {
                if size >= 0 {
                    self.visit_u64(size as u64)
                } else {
                    Err(E::invalid_value(Unexpected::Signed(size), &self))
                }
            }

            fn visit_u64<E>(self, size: u64) -> Result<ReadableSize, E>
            where
                E: de::Error,
            {
                Ok(ReadableSize(size))
            }

            fn visit_str<E>(self, size_str: &str) -> Result<ReadableSize, E>
            where
                E: de::Error,
            {
                size_str.parse().map_err(E::custom)
            }
        }

        deserializer.deserialize_any(SizeVisitor)
    }
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

    fn from_str(dur_str: &str) -> Result<ReadableDuration, String> {
        let dur_str = dur_str.trim();
        if !dur_str.is_ascii() {
            return Err(format!("unexpect ascii string: {dur_str}"));
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
            let number_str = unsafe { str::from_utf8_unchecked(first) };
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
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut buffer = String::new();
        write!(buffer, "{self}").unwrap();
        serializer.serialize_str(&buffer)
    }
}

impl<'de> Deserialize<'de> for ReadableDuration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DurVisitor;

        impl<'de> Visitor<'de> for DurVisitor {
            type Value = ReadableDuration;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("valid duration")
            }

            fn visit_str<E>(self, dur_str: &str) -> Result<ReadableDuration, E>
            where
                E: de::Error,
            {
                dur_str.parse().map_err(E::custom)
            }
        }

        deserializer.deserialize_str(DurVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_readable_size() {
        let s = ReadableSize::kb(2);
        assert_eq!(s.0, 2048);
        assert_eq!(s.as_mb(), 0);
        let s = ReadableSize::mb(2);
        assert_eq!(s.0, 2 * 1024 * 1024);
        assert_eq!(s.as_mb(), 2);
        let s = ReadableSize::gb(2);
        assert_eq!(s.0, 2 * 1024 * 1024 * 1024);
        assert_eq!(s.as_mb(), 2048);

        assert_eq!((ReadableSize::mb(2) / 2).0, MIB);
        assert_eq!((ReadableSize::mb(1) / 2).0, 512 * KIB);
        assert_eq!(ReadableSize::mb(2) / ReadableSize::kb(1), 2048);
    }

    #[test]
    fn test_parse_readable_size() {
        #[derive(Serialize, Deserialize)]
        struct SizeHolder {
            s: ReadableSize,
        }

        let legal_cases = vec![
            (0, "0KiB"),
            (2 * KIB, "2KiB"),
            (4 * MIB, "4MiB"),
            (5 * GIB, "5GiB"),
            (7 * TIB, "7TiB"),
            (11 * PIB, "11PiB"),
        ];
        for (size, exp) in legal_cases {
            let c = SizeHolder {
                s: ReadableSize(size),
            };
            let res_str = toml::to_string(&c).unwrap();
            let exp_str = format!("s = {exp:?}\n");
            assert_eq!(res_str, exp_str);
            let res_size: SizeHolder = toml::from_str(&exp_str).unwrap();
            assert_eq!(res_size.s.0, size);
        }

        let c = SizeHolder {
            s: ReadableSize(512),
        };
        let res_str = toml::to_string(&c).unwrap();
        assert_eq!(res_str, "s = 512\n");
        let res_size: SizeHolder = toml::from_str(&res_str).unwrap();
        assert_eq!(res_size.s.0, c.s.0);

        let decode_cases = vec![
            (" 0.5 PB", PIB / 2),
            ("0.5 TB", TIB / 2),
            ("0.5GB ", GIB / 2),
            ("0.5MB", MIB / 2),
            ("0.5KB", KIB / 2),
            ("0.5P", PIB / 2),
            ("0.5T", TIB / 2),
            ("0.5G", GIB / 2),
            ("0.5M", MIB / 2),
            ("0.5K", KIB / 2),
            ("23", 23),
            ("1", 1),
            ("1024B", KIB),
            // units with binary prefixes
            (" 0.5 PiB", PIB / 2),
            ("1PiB", PIB),
            ("0.5 TiB", TIB / 2),
            ("2 TiB", TIB * 2),
            ("0.5GiB ", GIB / 2),
            ("787GiB ", GIB * 787),
            ("0.5MiB", MIB / 2),
            ("3MiB", MIB * 3),
            ("0.5KiB", KIB / 2),
            ("1 KiB", KIB),
            // scientific notation
            ("0.5e6 B", B * 500000),
            ("0.5E6 B", B * 500000),
            ("1e6B", B * 1000000),
            ("8E6B", B * 8000000),
            ("8e7", B * 80000000),
            ("1e-1MB", MIB / 10),
            ("1e+1MB", MIB * 10),
            ("0e+10MB", 0),
        ];
        for (src, exp) in decode_cases {
            let src = format!("s = {src:?}");
            let res: SizeHolder = toml::from_str(&src).unwrap();
            assert_eq!(res.s.0, exp);
        }

        let illegal_cases = vec![
            "0.5kb", "0.5kB", "0.5Kb", "0.5k", "0.5g", "b", "gb", "1b", "B", "1K24B", " 5_KB",
            "4B7", "5M_",
        ];
        for src in illegal_cases {
            let src_str = format!("s = {src:?}");
            assert!(toml::from_str::<SizeHolder>(&src_str).is_err(), "{}", src);
        }
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
