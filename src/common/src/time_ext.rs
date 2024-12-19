// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{
    fmt::{self, Write},
    str::FromStr,
    time::Duration,
};

use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};

const TIME_MAGNITUDE_1: u64 = 1000;
const TIME_MAGNITUDE_2: u64 = 60;
const TIME_MAGNITUDE_3: u64 = 24;
const UNIT: u64 = 1;
const MS: u64 = UNIT;
const SECOND: u64 = MS * TIME_MAGNITUDE_1;
const MINUTE: u64 = SECOND * TIME_MAGNITUDE_2;
const HOUR: u64 = MINUTE * TIME_MAGNITUDE_2;
const DAY: u64 = HOUR * TIME_MAGNITUDE_3;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Default)]
pub struct ReadableDuration(pub Duration);

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
        self.0.as_millis() as u64
    }

    pub fn is_zero(&self) -> bool {
        self.0.as_nanos() == 0
    }
}

impl From<Duration> for ReadableDuration {
    fn from(t: Duration) -> ReadableDuration {
        ReadableDuration(t)
    }
}

impl From<ReadableDuration> for Duration {
    fn from(readable: ReadableDuration) -> Duration {
        readable.0
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

impl fmt::Display for ReadableDuration {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut dur = self.0.as_millis() as u64;
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

        impl Visitor<'_> for DurVisitor {
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
