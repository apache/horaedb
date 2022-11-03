// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Predict for query table.
//! Reference to: https://github.com/influxdata/influxdb_iox/blob/29b10413051f8c4a2193e8633aa133e45b0e505a/query/src/predicate.rs

use std::sync::Arc;

use common_types::{
    schema::Schema,
    time::{TimeRange, Timestamp},
};
use datafusion::{
    logical_plan::{Expr, Operator},
    scalar::ScalarValue,
};
use log::debug;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(display("Failed ot do pruning, err:{}", source))]
    Prune {
        source: datafusion::error::DataFusionError,
    },
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
            | Operator::Like
            | Operator::NotLike
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
            Expr::BinaryExpr { left, op, right } => {
                self.extract_time_range_from_binary_expr(left, right, op)
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
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
            | Expr::GetIndexedField { .. } => TimeRange::min_to_max(),
        }
    }
}

#[cfg(test)]
mod test {

    use arrow::{
        array::ArrayRef,
        datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema},
    };
    use datafusion::logical_expr::{expr_fn::col, lit};
    use parquet::{
        basic::Type,
        file::{
            metadata::{ColumnChunkMetaData, RowGroupMetaData},
            statistics::Statistics,
        },
        schema::types::{SchemaDescPtr, SchemaDescriptor, Type as SchemaType},
    };

    use super::*;

    fn convert_data_type(data_type: &ArrowDataType) -> Type {
        match data_type {
            ArrowDataType::Boolean => Type::BOOLEAN,
            ArrowDataType::Int32 => Type::INT32,
            ArrowDataType::Int64 => Type::INT64,
            ArrowDataType::Utf8 => Type::BYTE_ARRAY,
            _ => unimplemented!(),
        }
    }

    fn prepare_arrow_schema(fields: Vec<(&str, ArrowDataType)>) -> Arc<ArrowSchema> {
        let fields = fields
            .into_iter()
            .map(|(name, data_type)| ArrowField::new(name, data_type, false))
            .collect();
        Arc::new(ArrowSchema::new(fields))
    }

    fn prepare_parquet_schema_descr(schema: &ArrowSchema) -> SchemaDescPtr {
        let mut fields = schema
            .fields()
            .iter()
            .map(|field| {
                Arc::new(
                    SchemaType::primitive_type_builder(
                        field.name(),
                        convert_data_type(field.data_type()),
                    )
                    .build()
                    .unwrap(),
                )
            })
            .collect();
        let schema = SchemaType::group_type_builder("schema")
            .with_fields(&mut fields)
            .build()
            .unwrap();

        Arc::new(SchemaDescriptor::new(Arc::new(schema)))
    }

    fn prepare_metadata(schema: &ArrowSchema, statistics: Statistics) -> RowGroupMetaData {
        let schema_descr = prepare_parquet_schema_descr(schema);
        let column_metadata = schema_descr
            .columns()
            .iter()
            .cloned()
            .map(|col_descr| {
                ColumnChunkMetaData::builder(col_descr)
                    .set_statistics(statistics.clone())
                    .build()
                    .unwrap()
            })
            .collect();
        RowGroupMetaData::builder(schema_descr)
            .set_column_metadata(column_metadata)
            .build()
            .unwrap()
    }

    fn check_predicate_filter(
        expr: Expr,
        stat: Statistics,
        arrow_schema: Arc<ArrowSchema>,
        expected: Vec<bool>,
    ) {
        let metadata = prepare_metadata(&arrow_schema, stat);
        // let schema = arrow_schema.try_into().unwrap();
        let predicate = PredicateBuilder::default()
            .add_pushdown_exprs(&[expr])
            .build();
        let result = prune::filter_row_groups(arrow_schema, predicate.exprs(), &[metadata]);
        assert_eq!(result, expected);
    }

    #[test]
    fn filter_i32_by_equal_operator() {
        let expr = col("a").eq(lit(5i32));
        let stat = Statistics::int32(Some(10), Some(20), None, 0, false);
        let arrow_schema = prepare_arrow_schema(vec![("a", ArrowDataType::Int32)]);
        let expected = vec![false];

        check_predicate_filter(expr, stat, arrow_schema, expected);
    }

    #[test]
    fn filter_i64_by_greater_operator() {
        let expr = col("a").eq(lit(14i64));
        let stat = Statistics::int32(Some(10), Some(20), None, 0, false);
        let arrow_schema = prepare_arrow_schema(vec![("a", ArrowDataType::Int64)]);
        let expected = vec![true];

        check_predicate_filter(expr, stat, arrow_schema, expected);
    }

    #[test]
    fn filter_two_i32_cols_by_lesser_operator() {
        let expr = col("a").lt(col("b"));
        let stat = Statistics::int32(Some(10), Some(20), None, 0, false);
        let arrow_schema = prepare_arrow_schema(vec![
            ("a", ArrowDataType::Int32),
            ("b", ArrowDataType::Int32),
        ]);
        // nothing actually gets calculated.
        let expected = vec![true];

        check_predicate_filter(expr, stat, arrow_schema, expected);
    }

    #[test]
    fn filter_i64_by_in_list_operator() {
        let expr = col("a").in_list(vec![lit(17i64), lit(100i64)], false);
        let stat = Statistics::int32(Some(101), Some(200), None, 0, false);
        let arrow_schema = prepare_arrow_schema(vec![("a", ArrowDataType::Int64)]);
        let expected = vec![false];

        check_predicate_filter(expr, stat, arrow_schema, expected);
    }
}
