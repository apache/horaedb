// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Constants for table options.

use std::{collections::HashMap, string::ToString, time::Duration};

use arrow_deps::datafusion::parquet::basic::Compression as ParquetCompression;
use common_types::time::Timestamp;
use common_util::{
    config::{ReadableDuration, ReadableSize},
    define_result,
    time::DurationExt,
};
use proto::analytic_common::{
    CompactionOptions as CompactionOptionsPb, CompactionStrategy as CompactionStrategyPb,
    Compression as CompressionPb, StorageFormat as StorageFormatPb, TableOptions as TableOptionsPb,
    UpdateMode as UpdateModePb,
};
use serde_derive::Deserialize;
use snafu::{Backtrace, GenerateBacktrace, ResultExt, Snafu};
use table_engine::OPTION_KEY_ENABLE_TTL;

use crate::compaction::{
    CompactionStrategy, SizeTieredCompactionOptions, TimeWindowCompactionOptions,
};

pub const SEGMENT_DURATION: &str = "segment_duration";
pub const ENABLE_TTL: &str = OPTION_KEY_ENABLE_TTL;
pub const TTL: &str = "ttl";
pub const ARENA_BLOCK_SIZE: &str = "arena_block_size";
pub const WRITE_BUFFER_SIZE: &str = "write_buffer_size";
pub const COMPACTION_STRATEGY: &str = "compaction_strategy";
pub const NUM_ROWS_PER_ROW_GROUP: &str = "num_rows_per_row_group";
pub const UPDATE_MODE: &str = "update_mode";
pub const COMPRESSION: &str = "compression";
pub const STORAGE_FORMAT: &str = "storage_format";

const UPDATE_MODE_OVERWRITE: &str = "OVERWRITE";
const UPDATE_MODE_APPEND: &str = "APPEND";
const COMPRESSION_UNCOMPRESSED: &str = "UNCOMPRESSED";
const COMPRESSION_LZ4: &str = "LZ4";
const COMPRESSION_SNAPPY: &str = "SNAPPY";
const COMPRESSION_ZSTD: &str = "ZSTD";
const STORAGE_FORMAT_COLUMNAR: &str = "COLUMNAR";
const STORAGE_FORMAT_HYBRID: &str = "HYBRID";

/// Default bucket duration (1d)
const BUCKET_DURATION_1D: Duration = Duration::from_secs(24 * 60 * 60);
/// Default duration of a segment (2h).
pub const DEFAULT_SEGMENT_DURATION: Duration = Duration::from_secs(60 * 60 * 2);
/// Default arena block size (2M).
const DEFAULT_ARENA_BLOCK_SIZE: u32 = 2 * 1024 * 1024;
/// Default write buffer size (32M).
const DEFAULT_WRITE_BUFFER_SIZE: u32 = 32 * 1024 * 1024;
/// Default ttl of table (7d).
const DEFAULT_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);
/// Default row number of a row group.
const DEFAULT_NUM_ROW_PER_ROW_GROUP: usize = 8192;

/// Max arena block size (2G)
const MAX_ARENA_BLOCK_SIZE: u32 = 2 * 1024 * 1024 * 1024;
/// Min arena block size (1K)
const MIN_ARENA_BLOCK_SIZE: u32 = 1024;
const MIN_NUM_ROWS_PER_ROW_GROUP: usize = 100;
const MAX_NUM_ROWS_PER_ROW_GROUP: usize = 10_000_000;

#[derive(Debug, Snafu)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("Failed to parse duration, err:{}.\nBacktrace:\n{}", err, backtrace))]
    ParseDuration { err: String, backtrace: Backtrace },

    #[snafu(display("Failed to parse size, err:{}.\nBacktrace:\n{}", err, backtrace))]
    ParseSize { err: String, backtrace: Backtrace },

    #[snafu(display("Failed to parse compaction strategy: {}, err: {}", value, source))]
    ParseStrategy {
        value: String,
        source: crate::compaction::Error,
    },
    #[snafu(display("Failed to parse int, err:{}.\nBacktrace:\n{}", source, backtrace))]
    ParseInt {
        source: std::num::ParseIntError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to parse bool, err:{}.\nBacktrace:\n{}", source, backtrace))]
    ParseBool {
        source: std::str::ParseBoolError,
        backtrace: Backtrace,
    },
    #[snafu(display(
        "Failed to parse update mode, raw str:{}.\nBacktrace:\n{}",
        s,
        backtrace
    ))]
    ParseUpdateMode { s: String, backtrace: Backtrace },
    #[snafu(display(
        "Failed to parse compression, name:{}.\nBacktrace:\n{}",
        name,
        backtrace
    ))]
    ParseCompressionName { name: String, backtrace: Backtrace },

    #[snafu(display(
        "Unknown storage format. value:{:?}.\nBacktrace:\n{}",
        value,
        backtrace
    ))]
    UnknownStorageFormat { value: String, backtrace: Backtrace },
}

define_result!(Error);

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub enum UpdateMode {
    Overwrite,
    Append,
}

impl UpdateMode {
    pub fn parse_from(s: &str) -> Result<Self> {
        if s.eq_ignore_ascii_case(UPDATE_MODE_OVERWRITE) {
            Ok(UpdateMode::Overwrite)
        } else if s.eq_ignore_ascii_case(UPDATE_MODE_APPEND) {
            Ok(UpdateMode::Append)
        } else {
            ParseUpdateMode { s }.fail()
        }
    }
}

impl ToString for UpdateMode {
    fn to_string(&self) -> String {
        match self {
            UpdateMode::Append => UPDATE_MODE_APPEND.to_string(),
            UpdateMode::Overwrite => UPDATE_MODE_OVERWRITE.to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
pub enum Compression {
    Uncompressed,
    Lz4,
    Snappy,
    Zstd,
}

impl Compression {
    pub fn parse_from(name: &str) -> Result<Self> {
        if name.eq_ignore_ascii_case(COMPRESSION_UNCOMPRESSED) {
            Ok(Compression::Uncompressed)
        } else if name.eq_ignore_ascii_case(COMPRESSION_LZ4) {
            Ok(Compression::Lz4)
        } else if name.eq_ignore_ascii_case(COMPRESSION_SNAPPY) {
            Ok(Compression::Snappy)
        } else if name.eq_ignore_ascii_case(COMPRESSION_ZSTD) {
            Ok(Compression::Zstd)
        } else {
            ParseCompressionName { name }.fail()
        }
    }
}

impl ToString for Compression {
    fn to_string(&self) -> String {
        match self {
            Compression::Uncompressed => COMPRESSION_UNCOMPRESSED.to_string(),
            Compression::Lz4 => COMPRESSION_LZ4.to_string(),
            Compression::Snappy => COMPRESSION_SNAPPY.to_string(),
            Compression::Zstd => COMPRESSION_ZSTD.to_string(),
        }
    }
}

impl From<Compression> for CompressionPb {
    fn from(compression: Compression) -> Self {
        match compression {
            Compression::Uncompressed => CompressionPb::UNCOMPRESSED,
            Compression::Lz4 => CompressionPb::LZ4,
            Compression::Snappy => CompressionPb::SNAPPY,
            Compression::Zstd => CompressionPb::ZSTD,
        }
    }
}

impl From<CompressionPb> for Compression {
    fn from(compression: CompressionPb) -> Self {
        match compression {
            CompressionPb::UNCOMPRESSED => Compression::Uncompressed,
            CompressionPb::LZ4 => Compression::Lz4,
            CompressionPb::SNAPPY => Compression::Snappy,
            CompressionPb::ZSTD => Compression::Zstd,
        }
    }
}

impl From<Compression> for ParquetCompression {
    fn from(compression: Compression) -> Self {
        match compression {
            Compression::Uncompressed => ParquetCompression::UNCOMPRESSED,
            Compression::Lz4 => ParquetCompression::LZ4,
            Compression::Snappy => ParquetCompression::SNAPPY,
            Compression::Zstd => ParquetCompression::ZSTD,
        }
    }
}

/// StorageFormat specify how records are saved in persistent storage
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
pub enum StorageFormat {
    /// Traditional columnar format, every column is saved in one exact one
    /// column, for example:
    ///
    ///```plaintext
    /// | Timestamp | Device ID | Status Code | Tag 1 | Tag 2 |
    /// | --------- |---------- | ----------- | ----- | ----- |
    /// | 12:01     | A         | 0           | v1    | v1    |
    /// | 12:01     | B         | 0           | v2    | v2    |
    /// | 12:02     | A         | 0           | v1    | v1    |
    /// | 12:02     | B         | 1           | v2    | v2    |
    /// | 12:03     | A         | 0           | v1    | v1    |
    /// | 12:03     | B         | 0           | v2    | v2    |
    /// | .....     |           |             |       |       |
    /// ```
    Columnar,

    /// Design for time-series data
    /// Collapsible Columns within same primary key are collapsed
    /// into list, other columns are the same format with columar's.
    ///
    /// Whether a column is collapsible is decided by
    /// `Schema::is_collapsible_column`
    ///
    /// Note: minTime/maxTime is optional and not implemented yet, mainly used
    /// for time-range pushdown filter
    ///
    ///```plaintext
    /// | Device ID | Timestamp           | Status Code | Tag 1 | Tag 2 | minTime | maxTime |
    /// |-----------|---------------------|-------------|-------|-------|---------|---------|
    /// | A         | [12:01,12:02,12:03] | [0,0,0]     | v1    | v1    | 12:01   | 12:03   |
    /// | B         | [12:01,12:02,12:03] | [0,1,0]     | v2    | v2    | 12:01   | 12:03   |
    /// | ...       |                     |             |       |       |         |         |
    /// ```
    Hybrid,
}

impl From<StorageFormat> for StorageFormatPb {
    fn from(format: StorageFormat) -> Self {
        match format {
            StorageFormat::Columnar => Self::Columnar,
            StorageFormat::Hybrid => Self::Hybrid,
        }
    }
}

impl From<StorageFormatPb> for StorageFormat {
    fn from(format: StorageFormatPb) -> Self {
        match format {
            StorageFormatPb::Columnar => Self::Columnar,
            StorageFormatPb::Hybrid => Self::Hybrid,
        }
    }
}

impl TryFrom<&str> for StorageFormat {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        let format = match value.to_uppercase().as_str() {
            STORAGE_FORMAT_COLUMNAR => Self::Columnar,
            STORAGE_FORMAT_HYBRID => Self::Hybrid,
            _ => return UnknownStorageFormat { value }.fail(),
        };
        Ok(format)
    }
}

impl ToString for StorageFormat {
    fn to_string(&self) -> String {
        match self {
            Self::Columnar => STORAGE_FORMAT_COLUMNAR,
            Self::Hybrid => STORAGE_FORMAT_HYBRID,
        }
        .to_string()
    }
}

impl Default for StorageFormat {
    fn default() -> Self {
        Self::Columnar
    }
}

/// Options for a table.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(default)]
pub struct TableOptions {
    // The following options are immutable once table was created.
    /// Segment duration of the table.
    ///
    /// `None` means the table is doing the segment duration sampling and
    /// the actual duration is still unknown.
    pub segment_duration: Option<ReadableDuration>,
    /// Table update mode, now support Overwrite(Default) and Append
    pub update_mode: UpdateMode,
    /// Column's format in underlying storage
    pub storage_format: StorageFormat,

    // The following options can be altered.
    /// Enable ttl
    pub enable_ttl: bool,
    /// Time-to-live of the data.
    pub ttl: ReadableDuration,
    /// Arena block size of memtable.
    pub arena_block_size: u32,
    /// Write buffer size of memtable.
    pub write_buffer_size: u32,
    /// Compaction strategy of the table.
    pub compaction_strategy: CompactionStrategy,
    /// Row number in a row group.
    pub num_rows_per_row_group: usize,
    /// Table Compression
    pub compression: Compression,
}

impl TableOptions {
    #[inline]
    pub fn segment_duration(&self) -> Option<Duration> {
        self.segment_duration.map(|v| v.0)
    }

    #[inline]
    pub fn ttl(&self) -> Option<ReadableDuration> {
        if self.enable_ttl {
            Some(self.ttl)
        } else {
            None
        }
    }

    // for show create table
    pub fn to_raw_map(&self) -> HashMap<String, String> {
        let mut m = [
            (
                SEGMENT_DURATION.to_string(),
                self.segment_duration
                    .map(|v| v.to_string())
                    .unwrap_or_else(String::new),
            ),
            (UPDATE_MODE.to_string(), self.update_mode.to_string()),
            (ENABLE_TTL.to_string(), self.enable_ttl.to_string()),
            (TTL.to_string(), format!("{}", self.ttl)),
            (
                ARENA_BLOCK_SIZE.to_string(),
                format!("{}", self.arena_block_size),
            ),
            (
                WRITE_BUFFER_SIZE.to_string(),
                format!("{}", self.write_buffer_size),
            ),
            (
                NUM_ROWS_PER_ROW_GROUP.to_string(),
                format!("{}", self.num_rows_per_row_group),
            ),
            (COMPRESSION.to_string(), self.compression.to_string()),
            (STORAGE_FORMAT.to_string(), self.storage_format.to_string()),
        ]
        .into_iter()
        .collect();
        self.compaction_strategy.fill_raw_map(&mut m);

        m
    }

    /// Sanitize options silently.
    pub fn sanitize(&mut self) {
        let one_day_secs = BUCKET_DURATION_1D.as_secs();

        if let Some(segment_duration) = self.segment_duration {
            let mut segment_duration_secs = segment_duration.as_secs();
            if segment_duration_secs == 0 {
                segment_duration_secs = DEFAULT_SEGMENT_DURATION.as_secs()
            };
            self.segment_duration = Some(ReadableDuration::secs(segment_duration_secs));
        }

        let ttl_secs = self.ttl.as_secs();
        // Ttl must align to day.
        let ttl_secs = ttl_secs / one_day_secs * one_day_secs;
        self.ttl = ReadableDuration::secs(ttl_secs);

        if self.arena_block_size < MIN_ARENA_BLOCK_SIZE {
            self.arena_block_size = MIN_ARENA_BLOCK_SIZE;
        }

        if self.arena_block_size > MAX_ARENA_BLOCK_SIZE {
            self.arena_block_size = MAX_ARENA_BLOCK_SIZE;
        }

        if self.num_rows_per_row_group < MIN_NUM_ROWS_PER_ROW_GROUP {
            self.num_rows_per_row_group = MIN_NUM_ROWS_PER_ROW_GROUP;
        }

        if self.num_rows_per_row_group > MAX_NUM_ROWS_PER_ROW_GROUP {
            self.num_rows_per_row_group = MAX_NUM_ROWS_PER_ROW_GROUP;
        }
    }

    pub fn need_dedup(&self) -> bool {
        match self.update_mode {
            UpdateMode::Overwrite => true,
            UpdateMode::Append => false,
        }
    }

    pub fn is_expired(&self, timestamp: Timestamp) -> bool {
        self.enable_ttl && timestamp.is_expired(Timestamp::expire_time(self.ttl.0))
    }
}

impl From<SizeTieredCompactionOptions> for CompactionOptionsPb {
    fn from(opts: SizeTieredCompactionOptions) -> Self {
        let mut target = CompactionOptionsPb::new();
        target.set_bucket_low(opts.bucket_low);
        target.set_bucket_high(opts.bucket_high);
        target.set_min_sstable_size(opts.min_sstable_size.0 as u32);
        target.set_max_threshold(opts.max_threshold as u32);
        target.set_min_threshold(opts.min_threshold as u32);

        target
    }
}

impl From<CompactionOptionsPb> for SizeTieredCompactionOptions {
    fn from(opts: CompactionOptionsPb) -> Self {
        Self {
            bucket_low: opts.bucket_low,
            bucket_high: opts.bucket_high,
            min_sstable_size: ReadableSize(opts.min_sstable_size.into()),
            min_threshold: opts.min_threshold as usize,
            max_threshold: opts.max_threshold as usize,
        }
    }
}

impl From<TimeWindowCompactionOptions> for CompactionOptionsPb {
    fn from(opts: TimeWindowCompactionOptions) -> Self {
        let mut target = CompactionOptionsPb::new();
        target.set_bucket_low(opts.size_tiered.bucket_low);
        target.set_bucket_high(opts.size_tiered.bucket_high);
        target.set_min_sstable_size(opts.size_tiered.min_sstable_size.0 as u32);
        target.set_min_threshold(opts.size_tiered.min_threshold as u32);
        target.set_max_threshold(opts.size_tiered.max_threshold as u32);
        target.set_timestamp_resolution(opts.timestamp_resolution.into());

        target
    }
}

impl From<CompactionOptionsPb> for TimeWindowCompactionOptions {
    fn from(opts: CompactionOptionsPb) -> Self {
        let size_tiered: SizeTieredCompactionOptions = opts.clone().into();

        Self {
            size_tiered,
            timestamp_resolution: opts.timestamp_resolution.into(),
        }
    }
}

impl From<TableOptions> for TableOptionsPb {
    fn from(opts: TableOptions) -> Self {
        let mut target = TableOptionsPb::new();
        if let Some(segment_duration) = opts.segment_duration {
            target.set_segment_duration(segment_duration.0.as_millis_u64());
            target.set_sampling_segment_duration(false);
        } else {
            // The segment duration is unknown.
            target.set_sampling_segment_duration(true);
        }
        target.set_enable_ttl(opts.enable_ttl);
        target.set_ttl(opts.ttl.0.as_millis_u64());
        target.set_arena_block_size(opts.arena_block_size);
        target.set_num_rows_per_row_group(opts.num_rows_per_row_group as u64);

        match opts.compaction_strategy {
            CompactionStrategy::Default => {
                target.set_compaction_strategy(CompactionStrategyPb::DEFAULT);
            }
            CompactionStrategy::SizeTiered(opts) => {
                target.set_compaction_strategy(CompactionStrategyPb::SIZE_TIERED);
                target.set_compaction_options(opts.into());
            }
            CompactionStrategy::TimeWindow(opts) => {
                target.set_compaction_strategy(CompactionStrategyPb::TIME_WINDOW);
                target.set_compaction_options(opts.into());
            }
        }

        match opts.update_mode {
            UpdateMode::Overwrite => {
                target.set_update_mode(UpdateModePb::Overwrite);
            }
            UpdateMode::Append => {
                target.set_update_mode(UpdateModePb::Append);
            }
        }

        target.set_write_buffer_size(opts.write_buffer_size);
        target.set_compression(opts.compression.into());
        target.set_storage_format(opts.storage_format.into());

        target
    }
}

impl From<TableOptionsPb> for TableOptions {
    fn from(opts: TableOptionsPb) -> Self {
        let compaction_strategy = match opts.compaction_strategy {
            CompactionStrategyPb::DEFAULT => CompactionStrategy::default(),
            CompactionStrategyPb::SIZE_TIERED => {
                let opts = opts
                    .compaction_options
                    .map(SizeTieredCompactionOptions::from)
                    .unwrap_or_default();
                CompactionStrategy::SizeTiered(opts)
            }
            CompactionStrategyPb::TIME_WINDOW => {
                let opts = opts
                    .compaction_options
                    .map(TimeWindowCompactionOptions::from)
                    .unwrap_or_default();
                CompactionStrategy::TimeWindow(opts)
            }
        };

        let update_mode = match opts.update_mode {
            UpdateModePb::Overwrite => UpdateMode::Overwrite,
            UpdateModePb::Append => UpdateMode::Append,
        };
        let segment_duration = if opts.sampling_segment_duration {
            None
        } else if opts.segment_duration == 0 {
            // If segment duration is still zero. If the data had been used by an elder
            // version release that not yet support sampling, the
            // `sampling_segment_duration` flag would be truncated after
            // manifest snapshot, but left segment duration zero.
            Some(DEFAULT_SEGMENT_DURATION.into())
        } else {
            Some(Duration::from_millis(opts.segment_duration).into())
        };

        Self {
            segment_duration,
            enable_ttl: opts.enable_ttl,
            ttl: Duration::from_millis(opts.ttl).into(),
            arena_block_size: opts.arena_block_size,
            compaction_strategy,
            num_rows_per_row_group: opts.num_rows_per_row_group as usize,
            update_mode,
            write_buffer_size: opts.write_buffer_size,
            compression: opts.compression.into(),
            storage_format: opts.storage_format.into(),
        }
    }
}

impl Default for TableOptions {
    fn default() -> Self {
        Self {
            segment_duration: None,
            enable_ttl: true,
            ttl: DEFAULT_TTL.into(),
            arena_block_size: DEFAULT_ARENA_BLOCK_SIZE,
            compaction_strategy: CompactionStrategy::default(),
            num_rows_per_row_group: DEFAULT_NUM_ROW_PER_ROW_GROUP,
            update_mode: UpdateMode::Overwrite,
            write_buffer_size: DEFAULT_WRITE_BUFFER_SIZE,
            compression: Compression::Zstd,
            storage_format: StorageFormat::default(),
        }
    }
}

pub fn merge_table_options_for_create(
    options: &HashMap<String, String>,
    table_opts: &TableOptions,
) -> Result<TableOptions> {
    merge_table_options(options, table_opts, true)
}

pub fn merge_table_options_for_alter(
    options: &HashMap<String, String>,
    table_opts: &TableOptions,
) -> Result<TableOptions> {
    merge_table_options(options, table_opts, false)
}

/// The options will override the old options.
fn merge_table_options(
    options: &HashMap<String, String>,
    table_old_opts: &TableOptions,
    is_create: bool,
) -> Result<TableOptions> {
    let mut table_opts = table_old_opts.clone();
    if is_create {
        if let Some(v) = options.get(SEGMENT_DURATION) {
            table_opts.segment_duration = Some(parse_duration(v)?);
        }
        if let Some(v) = options.get(UPDATE_MODE) {
            table_opts.update_mode = UpdateMode::parse_from(v)?;
        }
    }

    if let Some(v) = options.get(TTL) {
        table_opts.ttl = parse_duration(v)?;
    }
    if let Some(v) = options.get(OPTION_KEY_ENABLE_TTL) {
        table_opts.enable_ttl = v.parse::<bool>().context(ParseBool)?;
    }
    if let Some(v) = options.get(ARENA_BLOCK_SIZE) {
        let size = parse_size(v)?;
        table_opts.arena_block_size = size.0 as u32;
    }
    if let Some(v) = options.get(WRITE_BUFFER_SIZE) {
        let size = parse_size(v)?;
        table_opts.write_buffer_size = size.0 as u32;
    }
    if let Some(v) = options.get(COMPACTION_STRATEGY) {
        table_opts.compaction_strategy =
            CompactionStrategy::parse_from(v, options).context(ParseStrategy { value: v })?;
    }
    if let Some(v) = options.get(NUM_ROWS_PER_ROW_GROUP) {
        table_opts.num_rows_per_row_group = v.parse().context(ParseInt)?;
    }
    if let Some(v) = options.get(COMPRESSION) {
        table_opts.compression = Compression::parse_from(v)?;
    }
    if let Some(v) = options.get(STORAGE_FORMAT) {
        table_opts.storage_format = v.as_str().try_into()?;
    }
    Ok(table_opts)
}

fn parse_duration(v: &str) -> Result<ReadableDuration> {
    v.parse::<ReadableDuration>()
        .map_err(|err| Error::ParseDuration {
            err,
            backtrace: Backtrace::generate(),
        })
}

fn parse_size(v: &str) -> Result<ReadableSize> {
    v.parse::<ReadableSize>().map_err(|err| Error::ParseSize {
        err,
        backtrace: Backtrace::generate(),
    })
}
