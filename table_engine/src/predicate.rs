// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Predict for query table.
//! Reference to: https://github.com/influxdata/influxdb_iox/blob/29b10413051f8c4a2193e8633aa133e45b0e505a/query/src/predicate.rs

use std::sync::Arc;

use common_types::{
    schema::Schema,
    time::{TimeRange, Timestamp},
};
use datafusion::{
    logical_expr::{Expr, Operator},
    scalar::ScalarValue,
};
use datafusion_proto::bytes::Serializeable;
use generic_error::{BoxError, GenericError};
use log::debug;
use macros::define_result;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(display("Failed to do pruning, err:{}", source))]
    Prune {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Failed to convert predicate to pb, msg:{}, err:{}", msg, source))]
    PredicateToPb {
        msg: String,
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Empty time range.\nBacktrace:\n{}", backtrace))]
    EmptyTimeRange { backtrace: Backtrace },

    #[snafu(display("Invalid time range.\nBacktrace:\n{}", backtrace))]
    InvalidTimeRange { backtrace: Backtrace },

    #[snafu(display("Expr decode failed., err:{}", source))]
    DecodeExpr { source: GenericError },
}

define_result!(Error);

/// Predicate helps determine whether specific row group should be read.
#[derive(Debug, Clone)]
pub struct Predicate {
    /// Predicates in the query for filter out the columns that meet all the
    /// exprs.
    exprs: Vec<Expr>,
    /// The time range involved by the query.
    time_range: TimeRange,
}

pub type PredicateRef = Arc<Predicate>;

impl Predicate {
    /// Create an empty predicate.
    pub fn empty() -> Self {
        Self {
            exprs: vec![],
            time_range: TimeRange::min_to_max(),
        }
    }

    pub fn exprs(&self) -> &[Expr] {
        &self.exprs
    }

    pub fn time_range(&self) -> TimeRange {
        self.time_range
    }

    /// Return a DataFusion [`Expr`] predicate representing the
    /// combination of AND'ing all (`exprs`) and timestamp restriction
    /// in this Predicate.
    pub fn to_df_expr(&self, time_column_name: impl AsRef<str>) -> Expr {
        self.exprs
            .iter()
            .cloned()
            .fold(self.time_range.to_df_expr(time_column_name), |acc, expr| {
                acc.and(expr)
            })
    }
}

impl TryFrom<&Predicate> for ceresdbproto::remote_engine::Predicate {
    type Error = Error;

    fn try_from(predicate: &Predicate) -> std::result::Result<Self, Self::Error> {
        let time_range = predicate.time_range;
        let mut exprs = Vec::with_capacity(predicate.exprs.len());
        for expr in &predicate.exprs {
            let expr = expr
                .to_bytes()
                .context(PredicateToPb {
                    msg: format!("convert expr failed, expr:{expr}"),
                })?
                .to_vec();
            exprs.push(expr);
        }

        Ok(Self {
            exprs,
            time_range: Some(time_range.into()),
        })
    }
}

impl TryFrom<ceresdbproto::remote_engine::Predicate> for Predicate {
    type Error = Error;

    fn try_from(
        pb: ceresdbproto::remote_engine::Predicate,
    ) -> std::result::Result<Self, Self::Error> {
        let time_range = pb.time_range.context(EmptyTimeRange)?;
        let mut exprs = Vec::with_capacity(pb.exprs.len());
        for pb_expr in pb.exprs {
            let expr = Expr::from_bytes(&pb_expr).box_err().context(DecodeExpr)?;
            exprs.push(expr);
        }
        Ok(Self {
            exprs,
            time_range: TimeRange::new(
                Timestamp::new(time_range.start),
                Timestamp::new(time_range.end),
            )
            .context(InvalidTimeRange)?,
        })
    }
}

/// Builder for [Predicate]
#[derive(Debug, Clone, Default)]
#[must_use]
pub struct PredicateBuilder {
    time_range: Option<TimeRange>,
    exprs: Vec<Expr>,
}

impl PredicateBuilder {
    /// Adds expressions.
    pub fn add_pushdown_exprs(mut self, filter_exprs: &[Expr]) -> Self {
        self.exprs.extend_from_slice(filter_exprs);
        self
    }

    pub fn set_time_range(mut self, time_range: TimeRange) -> Self {
        self.time_range = Some(time_range);
        self
    }

    /// Extract the time range from the `filter_exprs` and set it as
    /// [`TimeRange::min_to_max`] if no timestamp predicate is found.
    pub fn extract_time_range(mut self, schema: &Schema, filter_exprs: &[Expr]) -> Self {
        let time_range_extractor = TimeRangeExtractor {
            timestamp_column_name: schema.timestamp_name(),
            filters: filter_exprs,
        };

        let time_range = time_range_extractor.extract();
        debug!(
            "finish extract time range from the filters, time_range:{:?}, filters:{:?}",
            time_range, filter_exprs
        );

        self.time_range = Some(time_range);

        self
    }

    pub fn build(self) -> PredicateRef {
        Arc::new(Predicate {
            exprs: self.exprs,
            time_range: self.time_range.unwrap_or_else(TimeRange::min_to_max),
        })
    }
}

/// Extract the time range requirement from expressions.
struct TimeRangeExtractor<'a> {
    timestamp_column_name: &'a str,
    filters: &'a [Expr],
}

impl<'a> TimeRangeExtractor<'a> {
    /// Do extraction from the `self.filters` for TimeRange.
    ///
    /// Returns `TimeRange::zero_to_max()` if no timestamp predicate is found.
    fn extract(&self) -> TimeRange {
        let mut time_range = TimeRange::min_to_max();
        for expr in self.filters {
            let sub_time_range = self.extract_time_range_from_expr(expr);
            let new_time_range = Self::and_time_ranges(&time_range, &sub_time_range);

            debug!(
                "do and logic for time range, left:{:?}, right:{:?}, output:{:?}, expr:{:?}",
                time_range, sub_time_range, new_time_range, expr
            );
            time_range = new_time_range
        }

        time_range
    }

    /// Extract timestamp from the literal scalar expression.
    fn timestamp_from_scalar_expr(expr: &Expr) -> Option<Timestamp> {
        if let Expr::Literal(ScalarValue::TimestampMillisecond(v, _)) = expr {
            return v.map(Timestamp::new);
        }

        None
    }

    /// Compute the intersection of the two time ranges.
    fn and_time_ranges(left: &TimeRange, right: &TimeRange) -> TimeRange {
        let start = left.inclusive_start().max(right.inclusive_start());
        let end = left.exclusive_end().min(right.exclusive_end());
        TimeRange::new(start, end).unwrap_or_else(TimeRange::empty)
    }

    /// Compute the union of the two time ranges and the union is defined as the
    /// [min(left.start(), right.start()), max(left.end(), right.end())).
    fn or_time_ranges(left: &TimeRange, right: &TimeRange) -> TimeRange {
        let start = left.inclusive_start().min(right.inclusive_start());
        let end = left.exclusive_end().max(right.exclusive_end());
        TimeRange::new_unchecked(start, end)
    }

    /// Extract the timestamp from the column expression and its corresponding
    /// literal expression. Returns `None` if the expression pair is not
    /// involved with timestamp column. No assumption on the order of the
    /// `left` and `right`.
    fn timestamp_from_column_and_value_expr(&self, left: &Expr, right: &Expr) -> Option<Timestamp> {
        let (column, val) = match (left, right) {
            (Expr::Column(column), Expr::Literal(_)) => (column, right),
            (Expr::Literal(_), Expr::Column(column)) => (column, left),
            _ => return None,
        };

        if column.name == self.timestamp_column_name {
            Self::timestamp_from_scalar_expr(val)
        } else {
            None
        }
    }

    /// Extract time range from the binary expression.
    fn extract_time_range_from_binary_expr(
        &self,
        left: &Expr,
        right: &Expr,
        op: &Operator,
    ) -> TimeRange {
        match op {
            Operator::And => {
                let time_range_left = self.extract_time_range_from_expr(left);
                let time_range_right = self.extract_time_range_from_expr(right);
                Self::and_time_ranges(&time_range_left, &time_range_right)
            }
            Operator::Or => {
                let time_range_left = self.extract_time_range_from_expr(left);
                let time_range_right = self.extract_time_range_from_expr(right);
                Self::or_time_ranges(&time_range_left, &time_range_right)
            }
            Operator::Eq => self
                .timestamp_from_column_and_value_expr(left, right)
                .map(TimeRange::from_timestamp)
                .unwrap_or_else(TimeRange::min_to_max),
            Operator::NotEq => TimeRange::min_to_max(),
            Operator::Lt => self
                .timestamp_from_column_and_value_expr(left, right)
                .map(|right_t| TimeRange::new_unchecked(Timestamp::MIN, right_t))
                .unwrap_or_else(TimeRange::min_to_max),
            Operator::LtEq => self
                .timestamp_from_column_and_value_expr(left, right)
                .map(|right_t| {
                    let right_t = right_t.checked_add_i64(1).unwrap_or(right_t);
                    TimeRange::new_unchecked(Timestamp::MIN, right_t)
                })
                .unwrap_or_else(TimeRange::min_to_max),
            Operator::Gt => self
                .timestamp_from_column_and_value_expr(left, right)
                .map(|left_t| {
                    let left_t = left_t.checked_add_i64(1).unwrap_or(left_t);
                    TimeRange::new_unchecked(left_t, Timestamp::MAX)
                })
                .unwrap_or_else(TimeRange::min_to_max),
            Operator::GtEq => self
                .timestamp_from_column_and_value_expr(left, right)
                .map(|left_t| TimeRange::new_unchecked(left_t, Timestamp::MAX))
                .unwrap_or_else(TimeRange::min_to_max),
            Operator::Plus
            | Operator::Minus
            | Operator::Multiply
            | Operator::Divide
            | Operator::Modulo
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
            | Operator::RegexMatch
            | Operator::RegexNotMatch
            | Operator::RegexIMatch
            | Operator::RegexNotIMatch
            | Operator::BitwiseAnd
            | Operator::BitwiseOr
            | Operator::BitwiseXor
            | Operator::BitwiseShiftRight
            | Operator::BitwiseShiftLeft
            | Operator::StringConcat => TimeRange::min_to_max(),
        }
    }

    /// Extract time range from the between expression.
    fn time_range_from_between_expr(low: &Expr, high: &Expr, negated: bool) -> TimeRange {
        if negated {
            return TimeRange::min_to_max();
        }

        let low_t = Self::timestamp_from_scalar_expr(low).unwrap_or(Timestamp::MIN);
        // the two operands are inclusive in the `between` expression.
        let high_t = {
            let t = Self::timestamp_from_scalar_expr(high).unwrap_or(Timestamp::MAX);
            t.checked_add_i64(1).unwrap_or(Timestamp::MAX)
        };
        TimeRange::new(low_t, high_t).unwrap_or_else(TimeRange::empty)
    }

    /// Extract time range from the list expressions.
    fn time_range_from_list_expr(list: &[Expr], negated: bool) -> TimeRange {
        if negated {
            return TimeRange::min_to_max();
        }

        if list.is_empty() {
            return TimeRange::empty();
        }

        let (mut inclusive_start, mut inclusive_end) = (Timestamp::MAX, Timestamp::MIN);
        for expr in list {
            match Self::timestamp_from_scalar_expr(expr) {
                Some(t) => {
                    inclusive_start = inclusive_start.min(t);
                    inclusive_end = inclusive_end.max(t);
                }
                None => return TimeRange::min_to_max(),
            }
        }

        TimeRange::new(inclusive_start, inclusive_end).unwrap_or_else(TimeRange::empty)
    }

    /// Extract the time range recursively from the `expr`.
    ///
    /// Now the strategy is conservative: for the sub-expr which we are not sure
    /// how to handle it, returns `TimeRange::zero_to_max()`.
    fn extract_time_range_from_expr(&self, expr: &Expr) -> TimeRange {
        match expr {
            Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr { left, op, right }) => {
                self.extract_time_range_from_binary_expr(left, right, op)
            }
            Expr::Between(datafusion::logical_expr::Between {
                expr,
                negated,
                low,
                high,
            }) => {
                if let Expr::Column(column) = expr.as_ref() {
                    if column.name == self.timestamp_column_name {
                        return Self::time_range_from_between_expr(low, high, *negated);
                    }
                }

                TimeRange::min_to_max()
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                if let Expr::Column(column) = expr.as_ref() {
                    if column.name == self.timestamp_column_name {
                        return Self::time_range_from_list_expr(list, *negated);
                    }
                }

                TimeRange::min_to_max()
            }

            Expr::Alias(_, _)
            | Expr::ScalarVariable(_, _)
            | Expr::Column(_)
            | Expr::Literal(_)
            | Expr::Not(_)
            | Expr::Like { .. }
            | Expr::ILike { .. }
            | Expr::SimilarTo { .. }
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotUnknown(_)
            | Expr::Negative(_)
            | Expr::Case { .. }
            | Expr::Cast { .. }
            | Expr::TryCast { .. }
            | Expr::Sort { .. }
            | Expr::ScalarFunction { .. }
            | Expr::ScalarUDF { .. }
            | Expr::AggregateFunction { .. }
            | Expr::WindowFunction { .. }
            | Expr::AggregateUDF { .. }
            | Expr::Wildcard { .. }
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::ScalarSubquery(_)
            | Expr::QualifiedWildcard { .. }
            | Expr::GroupingSet(_)
            | Expr::GetIndexedField { .. }
            | Expr::OuterReferenceColumn { .. }
            | Expr::Placeholder { .. } => TimeRange::min_to_max(),
        }
    }
}

#[cfg(test)]
mod tests {
    use common_types::{
        tests::build_schema_with_dictionary,
        time::{TimeRange, Timestamp},
    };
    use datafusion::{
        prelude::{col, Expr},
        scalar::ScalarValue,
    };

    use crate::predicate::PredicateBuilder;

    fn set_timestamp(ts: i64) -> Expr {
        Expr::Literal(ScalarValue::TimestampMillisecond(Some(ts), None))
    }
    #[test]
    fn test_extract_range() {
        // The actual range needs to be a subset of the predicate range
        let schema = build_schema_with_dictionary();
        let cases = vec![
            (
                // key2 > 10000
                col("key2").gt(set_timestamp(10000)),
                TimeRange::new(Timestamp::new(10001), Timestamp::MAX).unwrap(),
            ),
            (
                // key2 > 10000 and key2 > 500
                col("key2")
                    .gt(set_timestamp(10000))
                    .and(col("key2").gt(set_timestamp(500))),
                TimeRange::new(Timestamp::new(10001), Timestamp::MAX).unwrap(),
            ),
            (
                // key2 > 10000 or key2 > 500
                col("key2")
                    .gt(set_timestamp(10000))
                    .or(col("key2").gt(set_timestamp(500))),
                TimeRange::new(Timestamp::new(501), Timestamp::MAX).unwrap(),
            ),
            (
                // key2 > 10000 or (key2 > 500 and key2 > 300)
                col("key2").gt(set_timestamp(10000)).or(col("key2")
                    .gt(set_timestamp(500))
                    .and(col("key2").gt(set_timestamp(300)))),
                TimeRange::new(Timestamp::new(501), Timestamp::MAX).unwrap(),
            ),
            (
                // key2 > 10000 or (key2 > 500 and key2 < 600)
                col("key2").gt(set_timestamp(10000)).or(col("key2")
                    .gt(set_timestamp(500))
                    .and(col("key2").lt(set_timestamp(600)))),
                TimeRange::new(Timestamp::new(501), Timestamp::MAX).unwrap(),
            ),
            (
                // key2 > 500 and key2 < 600
                col("key2")
                    .gt(set_timestamp(500))
                    .and(col("key2").lt(set_timestamp(600))),
                TimeRange::new(Timestamp::new(501), Timestamp::new(600)).unwrap(),
            ),
            (
                // key2 > 10000 and (key2 > 500 and key2 < 600)
                col("key2").gt(set_timestamp(10000)).and(
                    col("key2")
                        .gt(set_timestamp(500))
                        .and(col("key2").lt(set_timestamp(600))),
                ),
                TimeRange::new(Timestamp::new(0), Timestamp::new(0)).unwrap(),
            ),
            (
                // key2 > 10000 and (key2 > 500 and key2 > 600)
                col("key2").gt(set_timestamp(10000)).and(
                    col("key2")
                        .gt(set_timestamp(500))
                        .and(col("key2").gt(set_timestamp(600))),
                ),
                TimeRange::new(Timestamp::new(10001), Timestamp::MAX).unwrap(),
            ),
            (
                // key2 > 10 and (key2 < 500 and key2 > 1)
                col("key2").gt(set_timestamp(10)).and(
                    col("key2")
                        .lt(set_timestamp(500))
                        .and(col("key2").gt(set_timestamp(1))),
                ),
                TimeRange::new(Timestamp::new(11), Timestamp::new(500)).unwrap(),
            ),
            (
                // key2 < 10 and key2 > 11
                col("key2")
                    .lt(set_timestamp(10))
                    .and(col("key2").gt(set_timestamp(11))),
                TimeRange::new(Timestamp::new(0), Timestamp::new(0)).unwrap(),
            ),
            (
                // key2 < 10 or key2 > 11
                col("key2")
                    .lt(set_timestamp(10))
                    .or(col("key2").gt(set_timestamp(11))),
                TimeRange::new(Timestamp::MIN, Timestamp::MAX).unwrap(),
            ),
            (
                // (key2 < 10 or key2 > 11) and key2 > 1000
                (col("key2")
                    .lt(set_timestamp(10))
                    .or(col("key2").gt(set_timestamp(11))))
                .and(col("key2").gt(set_timestamp(1000))),
                TimeRange::new(Timestamp::new(1001), Timestamp::MAX).unwrap(),
            ),
            (
                // (key2 > 10 or key2 > 100) and key2 > 5
                (col("key2")
                    .lt(set_timestamp(10))
                    .or(col("key2").gt(set_timestamp(100))))
                .and(col("key2").gt(set_timestamp(5))),
                TimeRange::new(Timestamp::new(6), Timestamp::MAX).unwrap(),
            ),
            (
                // (key2 > 1 or key2 > 100) and key2 > 5
                (col("key2")
                    .lt(set_timestamp(10))
                    .or(col("key2").gt(set_timestamp(100))))
                .and(col("key2").gt(set_timestamp(5))),
                TimeRange::new(Timestamp::new(6), Timestamp::MAX).unwrap(),
            ),
        ];
        for (expr, expcted) in cases {
            let predict = PredicateBuilder::default()
                .add_pushdown_exprs(&vec![expr.clone()])
                .extract_time_range(&schema, &vec![expr])
                .build();
            assert_eq!(predict.time_range(), expcted);
        }
    }
}
