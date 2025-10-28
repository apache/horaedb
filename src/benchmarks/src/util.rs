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

//! Utilities for benchmarks.

use std::{
    env,
    fmt::{self, Write},
    fs,
    str::FromStr,
    time::Duration,
};

use bytes::Bytes;
use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use serde_json::json;
use tikv_jemalloc_ctl::{epoch, stats, thread};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Default)]
pub struct ReadableDuration(pub Duration);
impl From<Duration> for ReadableDuration {
    fn from(t: Duration) -> ReadableDuration {
        ReadableDuration(t)
    }
}
const TIME_MAGNITUDE_1: u64 = 1000;
const TIME_MAGNITUDE_2: u64 = 60;
const TIME_MAGNITUDE_3: u64 = 24;
const UNIT: u64 = 1;
const MS: u64 = UNIT;
const SECOND: u64 = MS * TIME_MAGNITUDE_1;
const MINUTE: u64 = SECOND * TIME_MAGNITUDE_2;
const HOUR: u64 = MINUTE * TIME_MAGNITUDE_2;
const DAY: u64 = HOUR * TIME_MAGNITUDE_3;

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

// Memory bench utilities.
#[derive(Debug, Clone)]
pub struct MemoryStats {
    pub thread_allocated: u64,
    pub thread_deallocated: u64,
    pub allocated: u64,
    pub active: u64,
    pub metadata: u64,
    pub mapped: u64,
    pub resident: u64,
    pub retained: u64,
}

#[derive(Debug, Clone)]
pub struct MemoryStatsDiff {
    pub thread_allocated_diff: i64,
    pub thread_deallocated_diff: i64,
    pub allocated: i64,
    pub active: i64,
    pub metadata: i64,
    pub mapped: i64,
    pub resident: i64,
    pub retained: i64,
}

impl MemoryStats {
    pub fn collect() -> Result<Self, String> {
        epoch::advance().map_err(|e| format!("failed to advance jemalloc epoch: {}", e))?;

        Ok(MemoryStats {
            thread_allocated: thread::allocatedp::read()
                .map_err(|e| format!("failed to read thread.allocatedp: {}", e))?
                .get(),
            thread_deallocated: thread::deallocatedp::read()
                .map_err(|e| format!("failed to read thread.deallocatedp: {}", e))?
                .get(),
            allocated: stats::allocated::read()
                .map_err(|e| format!("failed to read allocated: {}", e))?
                .try_into()
                .unwrap(),
            active: stats::active::read()
                .map_err(|e| format!("failed to read active: {}", e))?
                .try_into()
                .unwrap(),
            metadata: stats::metadata::read()
                .map_err(|e| format!("failed to read metadata: {}", e))?
                .try_into()
                .unwrap(),
            mapped: stats::mapped::read()
                .map_err(|e| format!("failed to read mapped: {}", e))?
                .try_into()
                .unwrap(),
            resident: stats::resident::read()
                .map_err(|e| format!("failed to read resident: {}", e))?
                .try_into()
                .unwrap(),
            retained: stats::retained::read()
                .map_err(|e| format!("failed to read retained: {}", e))?
                .try_into()
                .unwrap(),
        })
    }

    pub fn diff(&self, other: &MemoryStats) -> MemoryStatsDiff {
        MemoryStatsDiff {
            thread_allocated_diff: other.thread_allocated as i64 - self.thread_allocated as i64,
            thread_deallocated_diff: other.thread_deallocated as i64
                - self.thread_deallocated as i64,
            allocated: other.allocated as i64 - self.allocated as i64,
            active: other.active as i64 - self.active as i64,
            metadata: other.metadata as i64 - self.metadata as i64,
            mapped: other.mapped as i64 - self.mapped as i64,
            resident: other.resident as i64 - self.resident as i64,
            retained: other.retained as i64 - self.retained as i64,
        }
    }
}

pub struct MemoryBenchConfig {
    pub test_data: Bytes,
    pub scale: usize,
    pub mode: String,
}

impl MemoryBenchConfig {
    pub fn from_args() -> Self {
        let args: Vec<String> = env::args().collect();
        let mode = args[1].clone();
        let scale: usize = args[2].parse().expect("invalid scale");
        let test_data = Bytes::from(
            fs::read("../remote_write/tests/workloads/1709380533560664458.data")
                .expect("test data load failed"),
        );

        MemoryBenchConfig {
            test_data,
            scale,
            mode,
        }
    }

    pub fn output_json(&self, memory_diff: &MemoryStatsDiff) {
        let result = json!({
            "mode": self.mode,
            "scale": self.scale,
            "memory": {
                "thread_allocated_diff": memory_diff.thread_allocated_diff,
                "thread_deallocated_diff": memory_diff.thread_deallocated_diff,
                "allocated": memory_diff.allocated,
                "active": memory_diff.active,
                "metadata": memory_diff.metadata,
                "mapped": memory_diff.mapped,
                "resident": memory_diff.resident,
                "retained": memory_diff.retained
            }
        });
        println!("{}", result);
    }
}
