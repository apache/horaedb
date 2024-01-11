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

//! Compaction.

use std::{collections::HashMap, fmt, str::FromStr, sync::Arc};

use common_types::COMPACTION_STRATEGY;
use serde::{Deserialize, Serialize};
use size_ext::ReadableSize;
use snafu::{ensure, Backtrace, GenerateBacktrace, ResultExt, Snafu};
use time_ext::TimeUnit;
use tokio::sync::oneshot;

use crate::{
    compaction::picker::{CommonCompactionPicker, CompactionPickerRef},
    sst::file::{FileHandle, Level},
    table::data::TableDataRef,
};

mod metrics;
pub mod picker;
pub mod scheduler;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to parse compaction strategy, value: {}", value))]
    ParseStrategy { value: String, backtrace: Backtrace },
    #[snafu(display("Unable to parse float, key: {}, value: {}", key, value))]
    ParseFloat {
        key: String,
        value: String,
        source: std::num::ParseFloatError,
        backtrace: Backtrace,
    },
    #[snafu(display("Unable to parse int, key: {}, value: {}", key, value))]
    ParseInt {
        key: String,
        value: String,
        source: std::num::ParseIntError,
        backtrace: Backtrace,
    },
    #[snafu(display("Unable to parse readable size, key: {}, value: {}", key, value))]
    ParseSize {
        key: String,
        value: String,
        error: String,
        backtrace: Backtrace,
    },
    #[snafu(display("Unable to parse time unit, key: {}, value: {}", key, value))]
    ParseTimeUnit {
        key: String,
        value: String,
        error: String,
        backtrace: Backtrace,
    },
    #[snafu(display("Invalid compaction option value, err: {}", error))]
    InvalidOption { error: String, backtrace: Backtrace },
}

#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, Serialize)]
pub enum CompactionStrategy {
    #[default]
    Default,
    TimeWindow(TimeWindowCompactionOptions),
    SizeTiered(SizeTieredCompactionOptions),
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Serialize)]
pub struct SizeTieredCompactionOptions {
    pub bucket_low: f32,
    pub bucket_high: f32,
    pub min_sstable_size: ReadableSize,
    pub min_threshold: usize,
    pub max_threshold: usize,
    pub max_input_sstable_size: ReadableSize,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Serialize)]
pub struct TimeWindowCompactionOptions {
    pub size_tiered: SizeTieredCompactionOptions,
    // TODO(boyan) In fact right now we only supports TimeUnit::Milliseconds resolution.
    pub timestamp_resolution: TimeUnit,
}

// TODO: MAX_INPUT_SSTABLE_SIZE is a temp solution to control sst size
// Remove this when we can control compaction's output size
// https://github.com/apache/incubator-horaedb/issues/408
pub fn get_max_input_sstable_size() -> ReadableSize {
    match std::env::var("HORAEDB_MAX_INPUT_SSTABLE_SIZE") {
        Ok(size) => ReadableSize::from_str(&size).unwrap_or_else(|_| ReadableSize::mb(1200)),
        Err(_) => ReadableSize::mb(1200),
    }
}

impl Default for SizeTieredCompactionOptions {
    fn default() -> Self {
        Self {
            bucket_low: 0.5,
            bucket_high: 1.5,
            min_sstable_size: ReadableSize::mb(50),
            min_threshold: 4,
            max_threshold: 16,
            max_input_sstable_size: get_max_input_sstable_size(),
        }
    }
}

impl Default for TimeWindowCompactionOptions {
    fn default() -> Self {
        Self {
            size_tiered: SizeTieredCompactionOptions::default(),
            timestamp_resolution: TimeUnit::Milliseconds,
        }
    }
}

const BUCKET_LOW_KEY: &str = "compaction_bucket_low";
const BUCKET_HIGH_KEY: &str = "compaction_bucket_high";
const MIN_THRESHOLD_KEY: &str = "compaction_min_threshold";
const MAX_THRESHOLD_KEY: &str = "compaction_max_threshold";
const MIN_SSTABLE_SIZE_KEY: &str = "compaction_min_sstable_size";
const TIMESTAMP_RESOLUTION_KEY: &str = "compaction_timestamp_resolution";
const DEFAULT_STRATEGY: &str = "default";
const STC_STRATEGY: &str = "size_tiered";
const TWC_STRATEGY: &str = "time_window";

impl CompactionStrategy {
    pub(crate) fn parse_from(
        value: &str,
        options: &HashMap<String, String>,
    ) -> Result<CompactionStrategy, Error> {
        match value.trim().to_lowercase().as_str() {
            DEFAULT_STRATEGY => Ok(CompactionStrategy::Default),
            STC_STRATEGY => Ok(CompactionStrategy::SizeTiered(
                SizeTieredCompactionOptions::parse_from(options)?,
            )),
            TWC_STRATEGY => Ok(CompactionStrategy::TimeWindow(
                TimeWindowCompactionOptions::parse_from(options)?,
            )),
            _ => ParseStrategy {
                value: value.to_string(),
            }
            .fail(),
        }
    }

    pub(crate) fn fill_raw_map(&self, m: &mut HashMap<String, String>) {
        match self {
            CompactionStrategy::Default => {
                m.insert(
                    COMPACTION_STRATEGY.to_string(),
                    DEFAULT_STRATEGY.to_string(),
                );
            }
            CompactionStrategy::SizeTiered(opts) => {
                m.insert(COMPACTION_STRATEGY.to_string(), STC_STRATEGY.to_string());
                opts.fill_raw_map(m);
            }
            CompactionStrategy::TimeWindow(opts) => {
                m.insert(COMPACTION_STRATEGY.to_string(), TWC_STRATEGY.to_string());
                opts.fill_raw_map(m);
            }
        }
    }
}

impl SizeTieredCompactionOptions {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        ensure!(
            self.bucket_high > self.bucket_low,
            InvalidOption {
                error: format!(
                    "{} value({}) is less than or equal to the {} value({}) ",
                    BUCKET_HIGH_KEY, self.bucket_high, BUCKET_LOW_KEY, self.bucket_low
                ),
            }
        );

        Ok(())
    }

    fn fill_raw_map(&self, m: &mut HashMap<String, String>) {
        m.insert(BUCKET_LOW_KEY.to_string(), format!("{}", self.bucket_low));
        m.insert(BUCKET_HIGH_KEY.to_string(), format!("{}", self.bucket_high));
        m.insert(
            MIN_SSTABLE_SIZE_KEY.to_string(),
            format!("{}", self.min_sstable_size.0),
        );
        m.insert(
            MAX_THRESHOLD_KEY.to_string(),
            format!("{}", self.max_threshold),
        );
        m.insert(
            MIN_THRESHOLD_KEY.to_string(),
            format!("{}", self.min_threshold),
        );
    }

    pub(crate) fn parse_from(
        options: &HashMap<String, String>,
    ) -> Result<SizeTieredCompactionOptions, Error> {
        let mut opts = SizeTieredCompactionOptions::default();
        if let Some(v) = options.get(BUCKET_LOW_KEY) {
            opts.bucket_low = v.parse().context(ParseFloat {
                key: BUCKET_HIGH_KEY,
                value: v,
            })?;
        }
        if let Some(v) = options.get(BUCKET_HIGH_KEY) {
            opts.bucket_high = v.parse().context(ParseFloat {
                key: BUCKET_HIGH_KEY,
                value: v,
            })?;
        }
        if let Some(v) = options.get(MIN_SSTABLE_SIZE_KEY) {
            opts.min_sstable_size = v.parse::<ReadableSize>().map_err(|err| Error::ParseSize {
                key: MIN_SSTABLE_SIZE_KEY.to_string(),
                value: v.to_string(),
                error: err,
                backtrace: Backtrace::generate(),
            })?;
        }
        if let Some(v) = options.get(MAX_THRESHOLD_KEY) {
            opts.max_threshold = v.parse().context(ParseInt {
                key: MAX_THRESHOLD_KEY,
                value: v,
            })?;
        }
        if let Some(v) = options.get(MIN_THRESHOLD_KEY) {
            opts.min_threshold = v.parse().context(ParseInt {
                key: MIN_THRESHOLD_KEY,
                value: v,
            })?;
        }

        opts.validate()?;

        Ok(opts)
    }
}

impl TimeWindowCompactionOptions {
    /// TODO(boyan) In fact right now we only supports TimeUnit::Milliseconds
    /// resolution.
    fn valid_timestamp_unit(unit: TimeUnit) -> bool {
        matches!(
            unit,
            TimeUnit::Seconds
                | TimeUnit::Milliseconds
                | TimeUnit::Microseconds
                | TimeUnit::Nanoseconds
        )
    }

    fn fill_raw_map(&self, m: &mut HashMap<String, String>) {
        self.size_tiered.fill_raw_map(m);

        m.insert(
            TIMESTAMP_RESOLUTION_KEY.to_string(),
            format!("{}", self.timestamp_resolution),
        );
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        if !Self::valid_timestamp_unit(self.timestamp_resolution) {
            return InvalidOption {
                error: format!(
                    "{:?} is not valid for {}) ",
                    self.timestamp_resolution, TIMESTAMP_RESOLUTION_KEY
                ),
            }
            .fail();
        }

        Ok(())
    }

    pub(crate) fn parse_from(
        options: &HashMap<String, String>,
    ) -> Result<TimeWindowCompactionOptions, Error> {
        let mut opts = TimeWindowCompactionOptions {
            size_tiered: SizeTieredCompactionOptions::parse_from(options)?,
            ..Default::default()
        };

        if let Some(v) = options.get(TIMESTAMP_RESOLUTION_KEY) {
            opts.timestamp_resolution =
                v.parse::<TimeUnit>().map_err(|err| Error::ParseTimeUnit {
                    key: TIMESTAMP_RESOLUTION_KEY.to_string(),
                    value: v.to_string(),
                    error: err,
                    backtrace: Backtrace::generate(),
                })?;
        }

        opts.validate()?;

        Ok(opts)
    }
}

#[derive(Debug, Clone)]
pub struct CompactionInputFiles {
    /// Level of the files to be compacted.
    pub level: Level,
    /// Files to be compacted.
    pub files: Vec<FileHandle>,
    /// The output level of the merged file.
    pub output_level: Level,
}

#[derive(Debug, Default, Clone)]
pub struct ExpiredFiles {
    /// Level of the expired files.
    pub level: Level,
    /// Expired files.
    pub files: Vec<FileHandle>,
}

#[derive(Default, Clone)]
pub struct CompactionTask {
    inputs: Vec<CompactionInputFiles>,
    expired: Vec<ExpiredFiles>,
}

impl Drop for CompactionTask {
    fn drop(&mut self) {
        // When a CompactionTask is dropped, it means
        // 1. the task finished successfully, or
        // 2. the task is cancelled for some reason, like memory limit
        //
        // In case 2, we need to mark files as not compacted in order for them to be
        // scheduled again. In case 1, the files will be moved out of level controller,
        // so it doesn't care what the flag is, so it's safe to set false here.
        self.mark_files_being_compacted(false);
    }
}

impl CompactionTask {
    fn mark_files_being_compacted(&self, being_compacted: bool) {
        for input in &self.inputs {
            for file in &input.files {
                file.set_being_compacted(being_compacted);
            }
        }
        for expired in &self.expired {
            for file in &expired.files {
                file.set_being_compacted(being_compacted);
            }
        }
    }

    // Estimate the size of the total input files.
    #[inline]
    pub fn estimated_total_input_file_size(&self) -> usize {
        let total_input_size: u64 = self
            .inputs
            .iter()
            .map(|v| v.files.iter().map(|f| f.size()).sum::<u64>())
            .sum();

        total_input_size as usize
    }

    #[inline]
    pub fn num_compact_files(&self) -> usize {
        self.inputs.iter().map(|v| v.files.len()).sum()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.is_input_empty() && self.expired.is_empty()
    }

    #[inline]
    pub fn is_input_empty(&self) -> bool {
        self.inputs.is_empty()
    }

    #[inline]
    pub fn expired(&self) -> &[ExpiredFiles] {
        &self.expired
    }

    #[inline]
    pub fn inputs(&self) -> &[CompactionInputFiles] {
        &self.inputs
    }

    #[inline]
    pub fn contains_min_level(&self) -> bool {
        for input in &self.inputs {
            if input.level.is_min() {
                return true;
            }
        }

        false
    }
}

pub struct CompactionTaskBuilder {
    expired: Vec<ExpiredFiles>,
    inputs: Vec<CompactionInputFiles>,
}

impl CompactionTaskBuilder {
    pub fn with_expired(expired: Vec<ExpiredFiles>) -> Self {
        Self {
            expired,
            inputs: Vec::new(),
        }
    }

    pub fn add_inputs(&mut self, files: CompactionInputFiles) {
        self.inputs.push(files);
    }

    pub fn build(self) -> CompactionTask {
        let task = CompactionTask {
            expired: self.expired,
            inputs: self.inputs,
        };

        task.mark_files_being_compacted(true);

        task
    }
}

impl fmt::Debug for CompactionTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompactionTask")
            .field("inputs", &self.inputs)
            .field(
                "expired",
                &self
                    .expired
                    .iter()
                    .map(|expired| {
                        format!(
                            "level:{}, files:{:?}",
                            expired.level,
                            expired.files.iter().map(|f| f.id())
                        )
                    })
                    // only print first 10 files
                    .take(10)
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

#[derive(Default)]
pub struct PickerManager;

impl PickerManager {
    pub fn get_picker(&self, strategy: CompactionStrategy) -> CompactionPickerRef {
        Arc::new(CommonCompactionPicker::new(strategy))
    }
}

#[derive(Debug, Snafu)]
pub enum WaitError {
    #[snafu(display("The compaction is canceled"))]
    Canceled,

    #[snafu(display("Failed to compact, err:{}", source))]
    Compaction {
        source: Arc<dyn std::error::Error + Send + Sync>,
    },
}

pub type WaitResult<T> = std::result::Result<T, WaitError>;

pub struct WaiterNotifier {
    waiter: Option<oneshot::Sender<WaitResult<()>>>,
}

impl WaiterNotifier {
    pub fn new(waiter: Option<oneshot::Sender<WaitResult<()>>>) -> Self {
        Self { waiter }
    }

    pub fn notify_wait_result(mut self, res: WaitResult<()>) {
        // Ignore error if failed to send result.
        if let Some(waiter) = self.waiter.take() {
            let _ = waiter.send(res);
        }
    }
}

impl Drop for WaiterNotifier {
    fn drop(&mut self) {
        if let Some(waiter) = self.waiter.take() {
            // The compaction result hasn't been sent before the notifier dropped, we
            // send a canceled error to waiter.
            let _ = waiter.send(Canceled.fail());
        }
    }
}

/// Request to compact single table.
pub struct TableCompactionRequest {
    pub table_data: TableDataRef,
    pub waiter: Option<oneshot::Sender<WaitResult<()>>>,
}

impl TableCompactionRequest {
    pub fn new(table_data: TableDataRef) -> (Self, oneshot::Receiver<WaitResult<()>>) {
        let (tx, rx) = oneshot::channel::<WaitResult<()>>();
        let req = Self {
            table_data,
            waiter: Some(tx),
        };

        (req, rx)
    }

    pub fn no_waiter(table_data: TableDataRef) -> Self {
        TableCompactionRequest {
            table_data,
            waiter: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_fill_raw_map_then_parse() {
        let c = CompactionStrategy::Default;
        let mut m = HashMap::new();
        c.fill_raw_map(&mut m);
        assert_eq!(1, m.len());
        assert_eq!(m[COMPACTION_STRATEGY], "default");
        assert_eq!(c, CompactionStrategy::parse_from("default", &m).unwrap());

        let opts = SizeTieredCompactionOptions {
            bucket_low: 0.1,
            min_sstable_size: ReadableSize(1024),
            max_threshold: 10,
            ..Default::default()
        };

        let c = CompactionStrategy::SizeTiered(opts);
        let mut m = HashMap::new();
        c.fill_raw_map(&mut m);
        assert_eq!(6, m.len());
        assert_eq!(m[COMPACTION_STRATEGY], "size_tiered");
        assert_eq!(m[BUCKET_LOW_KEY], "0.1");
        assert_eq!(m[BUCKET_HIGH_KEY], "1.5");
        assert_eq!(m[MIN_SSTABLE_SIZE_KEY], "1024");
        assert_eq!(m[MIN_THRESHOLD_KEY], "4");
        assert_eq!(m[MAX_THRESHOLD_KEY], "10");
        assert_eq!(
            c,
            CompactionStrategy::parse_from("size_tiered", &m).unwrap()
        );

        let twc_opts = TimeWindowCompactionOptions {
            size_tiered: opts,
            ..Default::default()
        };
        let c = CompactionStrategy::TimeWindow(twc_opts);
        let mut m = HashMap::new();
        c.fill_raw_map(&mut m);

        assert_eq!(7, m.len());
        assert_eq!(m[COMPACTION_STRATEGY], "time_window");
        assert_eq!(m[BUCKET_LOW_KEY], "0.1");
        assert_eq!(m[BUCKET_HIGH_KEY], "1.5");
        assert_eq!(m[MIN_SSTABLE_SIZE_KEY], "1024");
        assert_eq!(m[MIN_THRESHOLD_KEY], "4");
        assert_eq!(m[MAX_THRESHOLD_KEY], "10");
        assert_eq!(m[TIMESTAMP_RESOLUTION_KEY], "milliseconds");

        assert_eq!(
            c,
            CompactionStrategy::parse_from("time_window", &m).unwrap()
        );
    }
}
