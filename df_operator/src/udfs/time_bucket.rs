// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! time_bucket UDF.

use std::time::Duration;

use chrono::{Datelike, FixedOffset, TimeZone};
use common_types::{
    column::{ColumnBlock, ColumnBlockBuilder, TimestampColumn},
    datum::{Datum, DatumKind},
    time::Timestamp,
};
use common_util::define_result;
use snafu::{ensure, OptionExt, ResultExt, Snafu};

use crate::{
    functions::{CallFunction, ColumnarValue, InvalidArguments, ScalarFunction, TypeSignature},
    registry::{self, FunctionRegistry},
    scalar::ScalarUdf,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid period, period:{}", period))]
    InvalidPeriod { period: String },

    #[snafu(display("Invalid period number, period:{}, err:{}", period, source))]
    InvalidPeriodNumber {
        period: String,
        source: std::num::ParseIntError,
    },

    #[snafu(display("Invalid argument number."))]
    InvalidArgNum,

    #[snafu(display("Invalid arguments, require timestamp column."))]
    NotTimestampColumn,

    #[snafu(display("Invalid arguments, require period."))]
    NotPeriod,

    #[snafu(display("Period of week only support P1W."))]
    UnsupportedWeek,

    #[snafu(display("Period of month only support P1M."))]
    UnsupportedMonth,

    #[snafu(display("Period of year only support P1Y."))]
    UnsupportedYear,

    #[snafu(display(
        "Failed to truncate timestamp, timestamp:{}, period:{:?}",
        timestamp,
        period
    ))]
    TruncateTimestamp { timestamp: i64, period: Period },

    #[snafu(display("Failed to build result column, err:{}", source))]
    BuildColumn { source: common_types::column::Error },
}

define_result!(Error);

/// Default timezone: +08:00
const DEFAULT_TIMEZONE_OFFSET_SECS: i32 = 8 * 3600;

pub fn register_to_registry(registry: &mut dyn FunctionRegistry) -> registry::Result<()> {
    registry.register_udf(new_udf())
}

fn new_udf() -> ScalarUdf {
    // args:
    // - timestamp column.
    // - period.
    // - input timestamp format in PARTITION BY (unsed now).
    // - input timezone (ignored now).
    // - timestamp output format (ignored now).
    let func = |args: &[ColumnarValue]| {
        let bucket = TimeBucket::parse_args(args)
            .map_err(|e| Box::new(e) as _)
            .context(InvalidArguments)?;

        let result_column = bucket
            .call()
            .map_err(|e| Box::new(e) as _)
            .context(CallFunction)?;

        Ok(ColumnarValue::Array(result_column))
    };

    let signature = make_signature();
    let scalar_function = ScalarFunction::make_by_fn(signature, DatumKind::Timestamp, func);

    ScalarUdf::create("time_bucket", scalar_function)
}

fn make_signature() -> TypeSignature {
    let sigs = vec![
        TypeSignature::Exact(vec![DatumKind::Timestamp, DatumKind::String]),
        TypeSignature::Exact(vec![
            DatumKind::Timestamp,
            DatumKind::String,
            DatumKind::String,
        ]),
        TypeSignature::Exact(vec![
            DatumKind::Timestamp,
            DatumKind::String,
            DatumKind::String,
            DatumKind::String,
        ]),
        TypeSignature::Exact(vec![
            DatumKind::Timestamp,
            DatumKind::String,
            DatumKind::String,
            DatumKind::String,
            DatumKind::String,
        ]),
    ];
    TypeSignature::OneOf(sigs)
}

struct TimeBucket<'a> {
    column: &'a TimestampColumn,
    period: Period,
}

impl<'a> TimeBucket<'a> {
    fn parse_args(args: &[ColumnarValue]) -> Result<TimeBucket> {
        ensure!(args.len() >= 2, InvalidArgNum);

        let column = match &args[0] {
            ColumnarValue::Array(block) => block.as_timestamp().context(NotTimestampColumn)?,
            _ => return NotTimestampColumn.fail(),
        };
        let period = match &args[1] {
            ColumnarValue::Scalar(value) => {
                let period_str = value.as_str().context(NotPeriod)?;
                Period::parse(period_str)?
            }
            _ => return NotPeriod.fail(),
        };

        Ok(TimeBucket { column, period })
    }

    fn call(&self) -> Result<ColumnBlock> {
        let mut out_column_builder =
            ColumnBlockBuilder::with_capacity(&DatumKind::Timestamp, self.column.num_rows());
        for ts_opt in self.column.iter() {
            match ts_opt {
                Some(ts) => {
                    let truncated = self.period.truncate(ts).context(TruncateTimestamp {
                        timestamp: ts,
                        period: self.period,
                    })?;
                    out_column_builder
                        .append(Datum::Timestamp(truncated))
                        .context(BuildColumn)?;
                }
                None => {
                    out_column_builder
                        .append(Datum::Null)
                        .context(BuildColumn)?;
                }
            }
        }
        Ok(out_column_builder.build())
    }
}

/// A time bucket period.
///
/// e.g.
/// - PT1S
/// - PT1M
/// - PT1H
/// - P1D
/// - P1W
/// - P1M
/// - P1Y
#[derive(Debug, Clone, Copy)]
pub enum Period {
    Second(u16),
    Minute(u16),
    Hour(u16),
    Day(u16),
    Week,
    Month,
    Year,
}

impl Period {
    fn parse(period: &str) -> Result<Period> {
        ensure!(period.len() >= 3, InvalidPeriod { period });
        let is_pt = if period.starts_with("PT") {
            true
        } else if period.starts_with('P') {
            false
        } else {
            return InvalidPeriod { period }.fail();
        };

        let back = period.chars().last().context(InvalidPeriod { period })?;
        let parsed = if is_pt {
            let number = &period[2..period.len() - 1];
            let number = number
                .parse::<u16>()
                .context(InvalidPeriodNumber { period })?;
            match back {
                'S' => Period::Second(number),
                'M' => Period::Minute(number),
                'H' => Period::Hour(number),
                _ => return InvalidPeriod { period }.fail(),
            }
        } else {
            let number = &period[1..period.len() - 1];
            let number = number
                .parse::<u16>()
                .context(InvalidPeriodNumber { period })?;
            match back {
                'D' => Period::Day(number),
                'W' => {
                    ensure!(number == 1, UnsupportedWeek);
                    Period::Week
                }
                'M' => {
                    ensure!(number == 1, UnsupportedMonth);
                    Period::Month
                }
                'Y' => {
                    ensure!(number == 1, UnsupportedYear);
                    Period::Year
                }
                _ => return InvalidPeriod { period }.fail(),
            }
        };

        Ok(parsed)
    }

    fn truncate(&self, ts: Timestamp) -> Option<Timestamp> {
        const MINUTE_SECONDS: u64 = 60;
        const HOUR_SECONDS: u64 = 60 * MINUTE_SECONDS;

        let truncated_ts = match self {
            Period::Second(period) => {
                let duration = Duration::from_secs(u64::from(*period));
                ts.truncate_by(duration)
            }
            Period::Minute(period) => {
                let duration = Duration::from_secs(u64::from(*period) * MINUTE_SECONDS);
                ts.truncate_by(duration)
            }
            Period::Hour(period) => {
                let duration = Duration::from_secs(u64::from(*period) * HOUR_SECONDS);
                ts.truncate_by(duration)
            }
            Period::Day(period) => Self::truncate_day(ts, *period)?,
            Period::Week => Self::truncate_week(ts),
            Period::Month => Self::truncate_month(ts),
            Period::Year => Self::truncate_year(ts),
        };

        Some(truncated_ts)
    }

    fn truncate_day(ts: Timestamp, period: u16) -> Option<Timestamp> {
        let offset = FixedOffset::east_opt(DEFAULT_TIMEZONE_OFFSET_SECS).expect("won't panic");
        // Convert to local time. Won't panic.
        let datetime = offset.timestamp_millis_opt(ts.as_i64()).unwrap();

        // Truncate day. Won't panic.
        let day = datetime.day();
        let day = day - (day % u32::from(period));
        let truncated_datetime = offset
            .with_ymd_and_hms(datetime.year(), datetime.month(), day, 0, 0, 0)
            .unwrap();
        let truncated_ts = truncated_datetime.timestamp_millis();

        Some(Timestamp::new(truncated_ts))
    }

    fn truncate_week(ts: Timestamp) -> Timestamp {
        let offset = FixedOffset::east_opt(DEFAULT_TIMEZONE_OFFSET_SECS).expect("won't panic");
        // Convert to local time. Won't panic.
        let datetime = offset.timestamp_millis_opt(ts.as_i64()).unwrap();

        // Truncate week. Won't panic.
        let week_offset = datetime.weekday().num_days_from_monday();
        let week_millis = 7 * 24 * 3600 * 1000;
        let ts_offset = week_offset * week_millis;
        // TODO(yingwen): Impl sub/divide for Timestamp
        let week_millis = i64::from(week_millis);
        let truncated_ts = (ts.as_i64() - i64::from(ts_offset)) / week_millis * week_millis;

        Timestamp::new(truncated_ts)
    }

    fn truncate_month(ts: Timestamp) -> Timestamp {
        let offset = FixedOffset::east_opt(DEFAULT_TIMEZONE_OFFSET_SECS).expect("won't panic");
        // Convert to local time. Won't panic.
        let datetime = offset.timestamp_millis_opt(ts.as_i64()).unwrap();

        // Truncate month. Won't panic.
        let truncated_datetime = offset
            .with_ymd_and_hms(datetime.year(), datetime.month(), 1, 0, 0, 0)
            .unwrap();
        let truncated_ts = truncated_datetime.timestamp_millis();

        Timestamp::new(truncated_ts)
    }

    fn truncate_year(ts: Timestamp) -> Timestamp {
        let offset = FixedOffset::east_opt(DEFAULT_TIMEZONE_OFFSET_SECS).expect("won't panic");
        // Convert to local time. Won't panic.
        let datetime = offset.timestamp_millis_opt(ts.as_i64()).unwrap();

        // Truncate year. Won't panic.
        let truncated_datetime = offset
            .with_ymd_and_hms(datetime.year(), 1, 1, 0, 0, 0)
            .unwrap();
        let truncated_ts = truncated_datetime.timestamp_millis();

        Timestamp::new(truncated_ts)
    }
}
