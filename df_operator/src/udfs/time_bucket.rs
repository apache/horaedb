// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! time_bucket UDF.

use std::sync::Arc;

use arrow::{array::TimestampNanosecondArray, datatypes::IntervalDayTimeType};
use common_types::{
    column::{cast_mills_to_nanosecond, cast_nanosecond_to_mills, ColumnBlock},
    datum::DatumKind,
};
use common_util::{define_result, error::BoxError};
use datafusion::{
    common::cast::as_timestamp_nanosecond_array,
    error::DataFusionError,
    physical_expr::datetime_expressions::{date_bin, date_trunc},
    physical_plan::ColumnarValue as DfColumnarValue,
    scalar::ScalarValue,
};
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

    #[snafu(display("time_bucket only support Array of values."))]
    UnsupportedScalar,

    #[snafu(display("Failed to truncate timestamp. err: {}", source))]
    TruncateTimestamp { source: DataFusionError },

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
            .box_err()
            .context(InvalidArguments)?;

        let result_column = bucket.call().box_err().context(CallFunction)?;

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

struct TimeBucket {
    column: DfColumnarValue,
    period: Period,
}

impl TimeBucket {
    fn parse_args(args: &[ColumnarValue]) -> Result<TimeBucket> {
        ensure!(args.len() >= 2, InvalidArgNum);

        let column = match &args[0] {
            ColumnarValue::Array(block) => {
                let mills_array = block.to_arrow_array_ref();
                let nanos_array = cast_mills_to_nanosecond(&mills_array).context(BuildColumn)?;
                DfColumnarValue::Array(nanos_array)
            }
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
        let truncate = self.period.truncate(&self.column)?;
        match truncate {
            DfColumnarValue::Array(array) => {
                let mills_array = cast_nanosecond_to_mills(&array).context(BuildColumn)?;
                ColumnBlock::try_cast_arrow_array_ref(&mills_array).context(BuildColumn)
            }
            _ => UnsupportedScalar.fail(),
        }
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

    fn truncate(&self, array: &DfColumnarValue) -> Result<DfColumnarValue> {
        const MILLIS_SECONDS: i32 = 1000;
        const MINUTE_SECONDS: i32 = 60 * MILLIS_SECONDS;
        const HOUR_SECONDS: i32 = 60 * MINUTE_SECONDS;

        match self {
            Period::Second(period) => {
                Self::truncate_by_date_bin(array, 0, i32::from(*period) * MILLIS_SECONDS)
            }
            Period::Minute(period) => {
                Self::truncate_by_date_bin(array, 0, i32::from(*period) * MINUTE_SECONDS)
            }
            Period::Hour(period) => {
                Self::truncate_by_date_bin(array, 0, i32::from(*period) * HOUR_SECONDS)
            }
            Period::Day(period) => Self::truncate_by_date_bin(array, *period as i32, 0),
            Period::Week => Self::truncate_by_date_trunc(array, "week"),
            Period::Month => Self::truncate_by_date_trunc(array, "month"),
            Period::Year => Self::truncate_by_date_trunc(array, "year"),
        }
    }

    fn truncate_by_date_bin(
        array: &DfColumnarValue,
        day: i32,
        mills: i32,
    ) -> Result<DfColumnarValue> {
        let truncate_time = IntervalDayTimeType::make_value(day, mills);
        let stride = DfColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(truncate_time)));
        let origin = DfColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
            Some(-DEFAULT_TIMEZONE_OFFSET_SECS as i64 * 1_000_000_000),
            Some("+00:00".to_owned()),
        ));

        date_bin(&[stride, array.clone(), origin]).context(TruncateTimestamp)
    }

    fn truncate_by_date_trunc(
        array: &DfColumnarValue,
        granularity: &str,
    ) -> Result<DfColumnarValue> {
        let granularity = DfColumnarValue::Scalar(ScalarValue::Utf8(Some(granularity.to_string())));
        let trunc_array = date_trunc(&[granularity, array.clone()]).context(TruncateTimestamp)?;

        let list = match trunc_array {
            DfColumnarValue::Array(array) => {
                let array = as_timestamp_nanosecond_array(&array).context(TruncateTimestamp)?;
                array
                    .iter()
                    .map(|ts| {
                        ts.map(|t| Ok(t - DEFAULT_TIMEZONE_OFFSET_SECS as i64 * 1_000_000_000))
                            .transpose()
                    })
                    .collect::<Result<TimestampNanosecondArray>>()?
            }
            _ => return UnsupportedScalar.fail(),
        };
        Ok(DfColumnarValue::Array(Arc::new(list)))
    }
}
