// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Segment duration sampler.

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};

use common_types::time::{TimeRange, Timestamp};
use common_util::define_result;
use snafu::{ensure, Backtrace, Snafu};

use crate::table_options;

/// Initial size of timestamps set.
const INIT_CAPACITY: usize = 1000;
const HOUR_MS: u64 = 3600 * 1000;
const DAY_MS: u64 = 24 * HOUR_MS;
const AVAILABLE_DURATIONS: [u64; 8] = [
    2 * HOUR_MS,
    DAY_MS,
    7 * DAY_MS,
    30 * DAY_MS,
    180 * DAY_MS,
    360 * DAY_MS,
    5 * 360 * DAY_MS,
    10 * 360 * DAY_MS,
];
const INTERVAL_RATIO: f64 = 0.9;
/// Expected points per timeseries in a segment, used to pick a proper segment
/// duration.
const POINTS_PER_SERIES: u64 = 100;
/// Max timestamp that wont overflow even using max duration.
const MAX_TIMESTAMP_MS_FOR_DURATION: i64 =
    i64::MAX - 2 * AVAILABLE_DURATIONS[AVAILABLE_DURATIONS.len() - 1] as i64;
/// Minimun sample timestamps to compute duration.
const MIN_SAMPLES: usize = 2;

#[derive(Debug, Snafu)]
#[snafu(display(
    "Invalid timestamp to collect, timestamp:{:?}.\nBacktrace:\n{}",
    timestamp,
    backtrace
))]
pub struct Error {
    timestamp: Timestamp,
    backtrace: Backtrace,
}

define_result!(Error);

/// Segment duration sampler.
///
/// Collects all timestamps and then yield a suggested segment duration to hold
/// all data with similar timestamp interval.
pub trait DurationSampler {
    /// Collect a timestamp.
    fn collect(&self, timestamp: Timestamp) -> Result<()>;

    /// Returns a suggested duration to partition the timestamps or default
    /// duration if no enough timestamp has been sampled.
    ///
    /// Note that this method may be invoked more than once.
    fn suggest_duration(&self) -> Duration;

    /// Returns a vector of time range with suggested duration that can hold all
    /// timestamps collected by this sampler.
    fn ranges(&self) -> Vec<TimeRange>;

    // TODO(yingwen): Memory usage.
}

pub type SamplerRef = Arc<dyn DurationSampler + Send + Sync>;

struct State {
    /// Deduplicated timestamps.
    deduped_timestamps: HashSet<Timestamp>,
    /// Cached suggested duration.
    duration: Option<Duration>,
    /// Sorted timestamps cache, empty if `duration` is None.
    sorted_timestamps: Vec<Timestamp>,
}

impl State {
    fn clear_cache(&mut self) {
        self.duration = None;
        self.sorted_timestamps.clear();
    }
}

pub struct DefaultSampler {
    state: Mutex<State>,
}

impl Default for DefaultSampler {
    fn default() -> Self {
        Self {
            state: Mutex::new(State {
                deduped_timestamps: HashSet::with_capacity(INIT_CAPACITY),
                duration: None,
                sorted_timestamps: Vec::new(),
            }),
        }
    }
}

impl DurationSampler for DefaultSampler {
    fn collect(&self, timestamp: Timestamp) -> Result<()> {
        ensure!(
            timestamp.as_i64() < MAX_TIMESTAMP_MS_FOR_DURATION,
            Context { timestamp }
        );

        let mut state = self.state.lock().unwrap();
        state.deduped_timestamps.insert(timestamp);
        state.clear_cache();

        Ok(())
    }

    fn suggest_duration(&self) -> Duration {
        if let Some(v) = self.duration() {
            return v;
        }

        let timestamps = self.compute_sorted_timestamps();
        let picked = match evaluate_interval(&timestamps) {
            Some(interval) => pick_duration(interval),
            None => table_options::DEFAULT_SEGMENT_DURATION,
        };

        {
            // Cache the picked duration.
            let mut state = self.state.lock().unwrap();
            state.duration = Some(picked);
            state.sorted_timestamps = timestamps;
        }

        picked
    }

    fn ranges(&self) -> Vec<TimeRange> {
        let duration = self.suggest_duration();
        let sorted_timestamps = self.cached_sorted_timestamps();
        // This type hint is needed to make `ranges.last()` work.
        let mut ranges: Vec<TimeRange> = Vec::new();

        for ts in sorted_timestamps {
            if let Some(range) = ranges.last() {
                if range.contains(ts) {
                    continue;
                }
            }

            // collect() ensures timestamp won't overflow.
            let range = TimeRange::bucket_of(ts, duration).unwrap();
            ranges.push(range);
        }

        ranges
    }
}

impl DefaultSampler {
    fn cached_sorted_timestamps(&self) -> Vec<Timestamp> {
        self.state.lock().unwrap().sorted_timestamps.clone()
    }

    fn compute_sorted_timestamps(&self) -> Vec<Timestamp> {
        let mut timestamps: Vec<_> = {
            let state = self.state.lock().unwrap();
            state.deduped_timestamps.iter().copied().collect()
        };

        timestamps.sort_unstable();

        timestamps
    }

    fn duration(&self) -> Option<Duration> {
        self.state.lock().unwrap().duration
    }
}

fn evaluate_interval(sorted_timestamps: &[Timestamp]) -> Option<u64> {
    if sorted_timestamps.len() < MIN_SAMPLES {
        return None;
    }

    let mut intervals = Vec::with_capacity(sorted_timestamps.len());
    for i in 0..sorted_timestamps.len() - 1 {
        let current = sorted_timestamps[i];
        let next = sorted_timestamps[i + 1];
        let interval = next.as_i64() - current.as_i64();
        intervals.push(interval);
    }

    intervals.sort_unstable();

    let mut index = (intervals.len() as f64 * INTERVAL_RATIO) as usize;
    if index > 1 {
        index -= 1;
    };
    let selected = intervals[index];
    // Interval should larger than 0.
    assert!(selected > 0);

    Some(selected as u64)
}

fn pick_duration(interval: u64) -> Duration {
    let scaled_interval = interval.checked_mul(POINTS_PER_SERIES).unwrap_or(u64::MAX);
    for du_ms in AVAILABLE_DURATIONS {
        if du_ms > scaled_interval {
            return Duration::from_millis(du_ms);
        }
    }

    // No duration larger than scaled interval, returns the largest duration.
    let du_ms = AVAILABLE_DURATIONS[AVAILABLE_DURATIONS.len() - 1];

    Duration::from_millis(du_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEC_MS: u64 = 1000;
    const MIN_MS: u64 = 60 * SEC_MS;

    #[test]
    fn test_pick_duration() {
        let cases = [
            (1, 2 * HOUR_MS),
            (5 * SEC_MS, 2 * HOUR_MS),
            (15 * SEC_MS, 2 * HOUR_MS),
            (MIN_MS, 2 * HOUR_MS),
            (5 * MIN_MS, DAY_MS),
            (10 * MIN_MS, DAY_MS),
            (30 * MIN_MS, 7 * DAY_MS),
            (HOUR_MS, 7 * DAY_MS),
            (4 * HOUR_MS, 30 * DAY_MS),
            (8 * HOUR_MS, 180 * DAY_MS),
            (DAY_MS, 180 * DAY_MS),
            (3 * DAY_MS, 360 * DAY_MS),
            (7 * DAY_MS, 5 * 360 * DAY_MS),
            (30 * DAY_MS, 10 * 360 * DAY_MS),
            (360 * DAY_MS, 10 * 360 * DAY_MS),
            (10 * 360 * DAY_MS, 10 * 360 * DAY_MS),
            (20 * 360 * DAY_MS, 10 * 360 * DAY_MS),
        ];

        for (i, (interval, expect)) in cases.iter().enumerate() {
            assert_eq!(
                *expect,
                pick_duration(*interval).as_millis() as u64,
                "Case {i}"
            );
        }
    }

    #[test]
    fn test_empty_sampler() {
        let sampler = DefaultSampler::default();

        assert_eq!(
            table_options::DEFAULT_SEGMENT_DURATION,
            sampler.suggest_duration()
        );
        assert!(sampler.ranges().is_empty());
    }

    #[test]
    fn test_one_sample() {
        let sampler = DefaultSampler::default();

        sampler.collect(Timestamp::new(0)).unwrap();

        assert_eq!(
            table_options::DEFAULT_SEGMENT_DURATION,
            sampler.suggest_duration()
        );
        let time_range =
            TimeRange::bucket_of(Timestamp::new(0), table_options::DEFAULT_SEGMENT_DURATION)
                .unwrap();
        assert_eq!(&[time_range], &sampler.ranges()[..]);
    }

    #[test]
    fn test_all_sample_same() {
        let sampler = DefaultSampler::default();

        let ts = Timestamp::now();
        for _ in 0..5 {
            sampler.collect(ts).unwrap();
        }

        assert_eq!(
            table_options::DEFAULT_SEGMENT_DURATION,
            sampler.suggest_duration()
        );
        let time_range = TimeRange::bucket_of(ts, table_options::DEFAULT_SEGMENT_DURATION).unwrap();
        assert_eq!(&[time_range], &sampler.ranges()[..]);
    }

    #[test]
    fn test_collect_invalid() {
        let sampler = DefaultSampler::default();

        assert!(sampler
            .collect(Timestamp::new(MAX_TIMESTAMP_MS_FOR_DURATION - 1))
            .is_ok());
        assert!(sampler
            .collect(Timestamp::new(MAX_TIMESTAMP_MS_FOR_DURATION))
            .is_err());
    }

    #[test]
    fn test_sampler_cache() {
        let sampler = DefaultSampler::default();

        let ts1 = Timestamp::now();
        for i in 0..3 {
            sampler
                .collect(Timestamp::new(ts1.as_i64() + i * SEC_MS as i64))
                .unwrap();
        }

        assert_eq!(
            table_options::DEFAULT_SEGMENT_DURATION,
            sampler.suggest_duration()
        );
        let time_range1 =
            TimeRange::bucket_of(ts1, table_options::DEFAULT_SEGMENT_DURATION).unwrap();
        assert_eq!(&[time_range1], &sampler.ranges()[..]);

        // A new timestamp is sampled.
        let ts2 = Timestamp::new(ts1.as_i64() + DAY_MS as i64);
        sampler.collect(ts2).unwrap();

        assert!(sampler.state.lock().unwrap().duration.is_none());
        assert!(sampler.state.lock().unwrap().sorted_timestamps.is_empty());

        assert_eq!(
            table_options::DEFAULT_SEGMENT_DURATION,
            sampler.suggest_duration()
        );
        let time_range2 =
            TimeRange::bucket_of(ts2, table_options::DEFAULT_SEGMENT_DURATION).unwrap();
        assert_eq!(&[time_range1, time_range2], &sampler.ranges()[..]);
    }

    fn test_suggest_duration_and_ranges_case(
        timestamps: &[i64],
        duration: u64,
        ranges: &[(i64, i64)],
    ) {
        let sampler = DefaultSampler::default();

        for ts in timestamps {
            sampler.collect(Timestamp::new(*ts)).unwrap();
        }

        assert_eq!(Duration::from_millis(duration), sampler.suggest_duration());

        let suggested_ranges = sampler.ranges();
        for (range, suggested_range) in ranges.iter().zip(suggested_ranges) {
            assert_eq!(range.0, suggested_range.inclusive_start().as_i64());
            assert_eq!(range.1, suggested_range.exclusive_end().as_i64());
        }
    }

    #[test]
    fn test_suggest_duration_and_ranges() {
        test_suggest_duration_and_ranges_case(
            // Intervals: 3, 5
            &[100, 103, 108],
            2 * HOUR_MS,
            &[(0, 2 * HOUR_MS as i64)],
        );

        let now_ts = Timestamp::now();
        let now = now_ts.as_i64();
        let sec_ms_i64 = SEC_MS as i64;

        let bucket = TimeRange::bucket_of(now_ts, Duration::from_millis(2 * HOUR_MS)).unwrap();
        let expect_range = (
            bucket.inclusive_start().as_i64(),
            bucket.exclusive_end().as_i64(),
        );
        test_suggest_duration_and_ranges_case(
            // Intervals: 5s, 5s, 5s, 5s, 100s,
            &[
                now,
                now + 5 * sec_ms_i64,
                now + 2 * 5 * sec_ms_i64,
                now + 3 * 5 * sec_ms_i64,
                now + 4 * 5 * sec_ms_i64,
                now + 4 * 5 * sec_ms_i64 + 100 * sec_ms_i64,
            ],
            2 * HOUR_MS,
            &[expect_range],
        );

        // Same with previous case, but shuffle the input timestamps.
        test_suggest_duration_and_ranges_case(
            &[
                now + 3 * 5 * sec_ms_i64,
                now,
                now + 5 * sec_ms_i64,
                now + 4 * 5 * sec_ms_i64,
                now + 2 * 5 * sec_ms_i64,
                now + 4 * 5 * sec_ms_i64 + 100 * sec_ms_i64,
            ],
            2 * HOUR_MS,
            &[expect_range],
        );

        test_suggest_duration_and_ranges_case(
            // Intervals: nine 5s and one 8h
            &[
                now + 5 * 5 * sec_ms_i64 + 8 * HOUR_MS as i64,
                now,
                now + 5 * sec_ms_i64,
                now + 2 * 5 * sec_ms_i64,
                now + 7 * 5 * sec_ms_i64 + 8 * HOUR_MS as i64,
                now + 3 * 5 * sec_ms_i64,
                now + 4 * 5 * sec_ms_i64,
                now + 4 * 5 * sec_ms_i64 + 8 * HOUR_MS as i64,
                now + 6 * 5 * sec_ms_i64 + 8 * HOUR_MS as i64,
                now + 8 * 5 * sec_ms_i64 + 8 * HOUR_MS as i64,
                now + 9 * 5 * sec_ms_i64 + 8 * HOUR_MS as i64,
            ],
            2 * HOUR_MS,
            &[
                expect_range,
                (
                    expect_range.0 + 8 * HOUR_MS as i64,
                    expect_range.1 + 8 * HOUR_MS as i64,
                ),
            ],
        );
    }
}
