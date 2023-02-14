// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Time types

// TODO(yingwen): Support timezone

use std::{
    convert::{TryFrom, TryInto},
    time::{self, Duration, SystemTime},
};

use ceresdbproto::time_range;
use snafu::{Backtrace, OptionExt, Snafu};

/// Error of time module.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid time range, start:{}, end:{}", start, end))]
    InvalidTimeRange {
        start: i64,
        end: i64,
        backtrace: Backtrace,
    },
}

/// Unix timestamp type in millis
// Use i64 so we can store timestamp before 1970-01-01
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct Timestamp(i64);

impl Timestamp {
    pub const MAX: Timestamp = Timestamp(i64::MAX);
    pub const MIN: Timestamp = Timestamp(i64::MIN);
    pub const ZERO: Timestamp = Timestamp(0);

    pub const fn new(ts: i64) -> Self {
        Self(ts)
    }

    /// Return current (non-negative) unix timestamp in millis.
    pub fn now() -> Self {
        SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .map(|duration| {
                duration
                    .as_millis()
                    .try_into()
                    .map(Timestamp)
                    .unwrap_or(Timestamp::MAX)
            })
            .unwrap_or(Timestamp::ZERO)
    }

    /// Returns the earliest expired timestamp.
    #[inline]
    pub fn expire_time(ttl: Duration) -> Timestamp {
        Timestamp::now().sub_duration_or_min(ttl)
    }

    #[inline]
    pub fn as_i64(&self) -> i64 {
        self.0
    }

    /// Truncate the value of this timestamp by given duration, return that
    /// value and keeps current timestamp unchanged.
    ///
    /// This function won't do overflow check.
    #[must_use]
    pub fn truncate_by(&self, duration: Duration) -> Self {
        let duration_millis = duration.as_millis() as i64;
        Timestamp::new(self.0 / duration_millis * duration_millis)
    }

    /// Floor the timestamp by the `duration_ms` (in millisecond) and return a
    /// new Timestamp instance or None if overflow occurred.
    ///
    /// The `duration_ms` must be positive
    #[inline]
    pub fn checked_floor_by_i64(&self, duration_ms: i64) -> Option<Self> {
        assert!(duration_ms > 0);
        let normalized_ts = if self.0 >= 0 {
            // self / duration_ms * duration_ms
            self.0
        } else {
            // (self - (duration_ms - 1)) / duration_ms * duration_ms
            self.0.checked_sub(duration_ms - 1)?
        };

        normalized_ts
            .checked_div(duration_ms)
            .and_then(|v| v.checked_mul(duration_ms))
            .map(Timestamp)
    }

    /// Returns the result of this `timestamp + offset_ms`, or None if overflow
    /// occurred.
    ///
    /// The `offset_ms` is in millis resolution
    pub fn checked_add_i64(&self, offset_ms: i64) -> Option<Self> {
        self.0.checked_add(offset_ms).map(Timestamp)
    }

    pub fn checked_add(&self, other: Self) -> Option<Self> {
        self.0.checked_add(other.0).map(Timestamp)
    }

    pub fn checked_sub(&self, other: Self) -> Option<Self> {
        self.0.checked_sub(other.0).map(Timestamp)
    }

    /// Returns the result of this `timestamp` - `duration`, or None if overflow
    /// occurred.
    pub fn checked_sub_duration(&self, duration: Duration) -> Option<Self> {
        let duration_millis = duration.as_millis().try_into().ok()?;
        self.0.checked_sub(duration_millis).map(Timestamp)
    }

    /// Return true if the time is expired
    pub fn is_expired(&self, expired_time: Timestamp) -> bool {
        *self < expired_time
    }

    /// Returns the result of this `timestamp` - `duration`, or MIN if overflow
    /// occurred.
    #[must_use]
    pub fn sub_duration_or_min(&self, duration: Duration) -> Timestamp {
        self.checked_sub_duration(duration)
            .unwrap_or(Timestamp::MIN)
    }
}

impl From<Timestamp> for i64 {
    fn from(timestamp: Timestamp) -> Self {
        timestamp.0
    }
}

impl From<i64> for Timestamp {
    fn from(ts: i64) -> Self {
        Self::new(ts)
    }
}

impl From<&i64> for Timestamp {
    fn from(ts: &i64) -> Self {
        Self::new(*ts)
    }
}

/// Unix timestamp range in millis
///
/// The start time is inclusive and the end time is exclusive: [start, end).
/// The range is empty if start equals end.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeRange {
    /// The start timestamp (inclusive)
    inclusive_start: Timestamp,
    /// The end timestamp (exclusive)
    exclusive_end: Timestamp,
}

impl TimeRange {
    /// Create a new time range, returns None if the start/end is invalid
    pub fn new(inclusive_start: Timestamp, exclusive_end: Timestamp) -> Option<Self> {
        if inclusive_start <= exclusive_end {
            Some(Self {
                inclusive_start,
                exclusive_end,
            })
        } else {
            None
        }
    }

    /// Create a new time range, panic if the start/end is invalid.
    pub fn new_unchecked(inclusive_start: Timestamp, exclusive_end: Timestamp) -> Self {
        Self::new(inclusive_start, exclusive_end).unwrap()
    }

    #[cfg(any(test, feature = "test"))]
    pub fn new_unchecked_for_test(inclusive_start: i64, exclusive_end: i64) -> Self {
        Self::new(
            Timestamp::new(inclusive_start),
            Timestamp::new(exclusive_end),
        )
        .unwrap()
    }

    /// Create a time range only including the single timestamp.
    pub fn from_timestamp(t: Timestamp) -> Self {
        // FIXME(xikai): now the time range can not express the `exclusive_end` as
        //  infinite.
        let end = t.checked_add_i64(1).unwrap_or(t);
        Self::new(t, end).unwrap()
    }

    /// Create a new time range of [0, max)
    pub fn min_to_max() -> Self {
        Self {
            inclusive_start: Timestamp::MIN,
            exclusive_end: Timestamp::MAX,
        }
    }

    /// Create a empty time range.
    pub fn empty() -> Self {
        Self {
            inclusive_start: Timestamp::ZERO,
            exclusive_end: Timestamp::ZERO,
        }
    }

    /// The inclusive start timestamp
    #[inline]
    pub fn inclusive_start(&self) -> Timestamp {
        self.inclusive_start
    }

    /// The exclusive end timestamp
    #[inline]
    pub fn exclusive_end(&self) -> Timestamp {
        self.exclusive_end
    }

    /// Return the reference to the exclusive end timestamp.
    #[inline]
    pub fn exclusive_end_ref(&self) -> &Timestamp {
        &self.exclusive_end
    }

    /// Returns true if the time range contains the given `ts`
    #[inline]
    pub fn contains(&self, ts: Timestamp) -> bool {
        self.inclusive_start <= ts && ts < self.exclusive_end
    }

    /// Returns a time bucket with fixed bucket size that the timestamp belongs
    /// to. Returns None if overflow occurred, the bucket_duration is greater
    /// than [i64::MAX] or not positive.
    pub fn bucket_of(timestamp: Timestamp, bucket_duration: Duration) -> Option<Self> {
        let bucket_duration_ms: i64 = bucket_duration.as_millis().try_into().ok()?;
        if bucket_duration_ms <= 0 {
            return None;
        }

        let inclusive_start = timestamp.checked_floor_by_i64(bucket_duration_ms)?;
        // end = start + bucket_duration
        let exclusive_end = inclusive_start.checked_add_i64(bucket_duration_ms)?;

        Some(Self {
            inclusive_start,
            exclusive_end,
        })
    }

    /// Returns true if this time range intersect with `other`
    pub fn intersect_with(&self, other: TimeRange) -> bool {
        !self.not_intersecting(other)
    }

    /// Return true if the time range is expired (`exclusive_end_time` <
    /// `expire_time`).
    pub fn is_expired(&self, expire_time: Option<Timestamp>) -> bool {
        expire_time.is_some() && self.exclusive_end() <= expire_time.unwrap()
    }

    #[inline]
    fn not_intersecting(&self, other: TimeRange) -> bool {
        other.exclusive_end <= self.inclusive_start || other.inclusive_start >= self.exclusive_end
    }

    pub fn intersected_range(&self, other: TimeRange) -> Option<TimeRange> {
        TimeRange::new(
            self.inclusive_start.max(other.inclusive_start),
            self.exclusive_end.min(other.exclusive_end),
        )
    }
}

impl From<TimeRange> for time_range::TimeRange {
    fn from(src: TimeRange) -> Self {
        time_range::TimeRange {
            start: src.inclusive_start.as_i64(),
            end: src.exclusive_end.as_i64(),
        }
    }
}

impl TryFrom<time_range::TimeRange> for TimeRange {
    type Error = Error;

    fn try_from(src: time_range::TimeRange) -> Result<Self, Error> {
        Self::new(Timestamp::new(src.start), Timestamp::new(src.end)).context(InvalidTimeRange {
            start: src.start,
            end: src.end,
        })
    }
}

#[cfg(feature = "datafusion")]
mod datafusion_ext {
    use datafusion::{
        prelude::{col, lit, Expr},
        scalar::ScalarValue,
    };

    use crate::time::TimeRange;

    impl TimeRange {
        /// Creates expression like:
        /// start <= time && time < end
        pub fn to_df_expr(&self, column_name: impl AsRef<str>) -> Expr {
            let ts_start =
                ScalarValue::TimestampMillisecond(Some(self.inclusive_start.as_i64()), None);
            let ts_end = ScalarValue::TimestampMillisecond(Some(self.exclusive_end.as_i64()), None);
            let column_name = column_name.as_ref();
            let ts_low = col(column_name).gt_eq(lit(ts_start));
            let ts_high = col(column_name).lt(lit(ts_end));

            ts_low.and(ts_high)
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use crate::time::{TimeRange, Timestamp};

    #[test]
    fn test_timestamp() {
        // 1637723901000: 2021-11-24 11:18:21
        let timestamp = Timestamp::new(1637723901000);
        // 1d
        let ttl = Duration::from_secs(24 * 3600);
        assert_eq!(
            timestamp.sub_duration_or_min(ttl),
            Timestamp::new(1637637501000)
        );
        assert_eq!(timestamp.truncate_by(ttl), Timestamp::new(1637712000000));
        assert_eq!(
            timestamp.checked_floor_by_i64(2000),
            Some(Timestamp::new(1637723900000))
        );
        assert_eq!(
            timestamp.checked_add_i64(2000),
            Some(Timestamp::new(1637723903000))
        );
        assert_eq!(
            timestamp.checked_sub_duration(ttl),
            Some(Timestamp::new(1637637501000))
        );
    }

    #[test]
    fn test_time_range() {
        // [100,200)
        let time_range = TimeRange::new_unchecked_for_test(100, 200);
        assert!(time_range.contains(Timestamp::new(150)));
        assert!(time_range.contains(Timestamp::new(100)));
        assert!(!time_range.contains(Timestamp::new(200)));

        assert!(!time_range.is_expired(Some(Timestamp::new(50))));
        assert!(time_range.is_expired(Some(Timestamp::new(200))));

        assert_eq!(
            TimeRange::bucket_of(Timestamp::new(100), Duration::from_millis(2)),
            Some(TimeRange::new_unchecked_for_test(100, 102))
        );

        let time_range2 = TimeRange::new_unchecked_for_test(200, 300);
        assert!(!time_range.intersect_with(time_range2));
        let time_range3 = TimeRange::new_unchecked_for_test(50, 200);
        assert!(time_range.intersect_with(time_range3));

        assert!(time_range.not_intersecting(time_range2));
        assert!(!time_range.not_intersecting(time_range3));
    }

    #[test]
    fn test_bucket_of_negative_timestamp() {
        let ts = Timestamp::new(-126316800000);
        let range = TimeRange::bucket_of(ts, Duration::from_millis(25920000000)).unwrap();
        assert!(range.contains(ts), "range:{:?}", range);
    }
}
