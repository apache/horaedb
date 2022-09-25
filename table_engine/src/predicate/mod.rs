// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Predict for query table.
//! Reference to: https://github.com/influxdata/influxdb_iox/blob/29b10413051f8c4a2193e8633aa133e45b0e505a/query/src/predicate.rs

use std::{convert::TryInto, sync::Arc};

use arrow_deps::{
    arrow::{
        array::ArrayRef,
        datatypes::{Schema as ArrowSchema, SchemaRef},
    },
    datafusion::{
        logical_plan::{Column, Expr, Operator},
        parquet::file::metadata::RowGroupMetaData,
        physical_optimizer::pruning::{PruningPredicate, PruningStatistics},
        scalar::ScalarValue,
    },
    parquet::file::statistics::Statistics as ParquetStatistics,
};
use common_types::{
    schema::Schema,
    time::{TimeRange, Timestamp},
};
use log::{debug, error, trace};
use snafu::{ResultExt, Snafu};

pub mod filter_record_batch;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(display("Failed ot do pruning, err:{}", source))]
    Prune {
        source: arrow_deps::datafusion::error::DataFusionError,
    },
}

define_result!(Error);

/// port from datafusion.
/// Extract the min/max statistics from a `ParquetStatistics` object
macro_rules! get_statistic {
    ($column_statistics:expr, $func:ident, $bytes_func:ident) => {{
        if !$column_statistics.has_min_max_set() {
            return None;
        }
        match $column_statistics {
            ParquetStatistics::Boolean(s) => Some(ScalarValue::Boolean(Some(*s.$func()))),
            ParquetStatistics::Int32(s) => Some(ScalarValue::Int32(Some(*s.$func()))),
            ParquetStatistics::Int64(s) => Some(ScalarValue::Int64(Some(*s.$func()))),
            // 96 bit ints not supported
            ParquetStatistics::Int96(_) => None,
            ParquetStatistics::Float(s) => Some(ScalarValue::Float32(Some(*s.$func()))),
            ParquetStatistics::Double(s) => Some(ScalarValue::Float64(Some(*s.$func()))),
            ParquetStatistics::ByteArray(s) => {
                let s = std::str::from_utf8(s.$bytes_func())
                    .map(|s| s.to_string())
                    .ok();
                Some(ScalarValue::Utf8(s))
            }
            // type not supported yet
            ParquetStatistics::FixedLenByteArray(_) => None,
        }
    }};
}

/// port from datafusion.
// Extract the min or max value calling `func` or `bytes_func` on the
// ParquetStatistics as appropriate
macro_rules! get_min_max_values {
    ($self:expr, $column:expr, $func:ident, $bytes_func:ident) => {{
        let (column_index, field) =
            if let Some((v, f)) = $self.parquet_schema.column_with_name(&$column.name) {
                (v, f)
            } else {
                // Named column was not present
                return None;
            };

        let data_type = field.data_type();
        let null_scalar: ScalarValue = if let Ok(v) = data_type.try_into() {
            v
        } else {
            // DataFusion doesn't have support for ScalarValues of the column type
            return None;
        };

        let scalar_values: Vec<ScalarValue> = $self
            .row_group_metadata
            .iter()
            .flat_map(|meta| meta.column(column_index).statistics())
            .map(|stats| get_statistic!(stats, $func, $bytes_func))
            .map(|maybe_scalar| {
                // column either did't have statistics at all or didn't have min/max values
                maybe_scalar.unwrap_or_else(|| null_scalar.clone())
            })
            .collect();

        // ignore errors converting to arrays (e.g. different types)
        ScalarValue::iter_to_array(scalar_values).ok()
    }};
}

/// Wraps parquet statistics in a way
/// that implements [`PruningStatistics`]
struct RowGroupPruningStatistics<'a> {
    row_group_metadata: &'a [RowGroupMetaData],
    parquet_schema: &'a ArrowSchema,
}

impl<'a> PruningStatistics for RowGroupPruningStatistics<'a> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        get_min_max_values!(self, column, min, min_bytes)
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        get_min_max_values!(self, column, max, max_bytes)
    }

    fn num_containers(&self) -> usize {
        self.row_group_metadata.len()
    }

    // TODO: support this.
    fn null_counts(&self, _column: &Column) -> Option<ArrayRef> {
        None
    }
}

fn build_row_group_predicate(
    predicate_builder: &PruningPredicate,
    row_group_metadata: &[RowGroupMetaData],
) -> Result<Vec<bool>> {
    let parquet_schema = predicate_builder.schema().as_ref();

    let pruning_stats = RowGroupPruningStatistics {
        row_group_metadata,
        parquet_schema,
    };

    predicate_builder
        .prune(&pruning_stats)
        .map_err(|e| {
            error!("Error evaluating row group predicate values {}", e);
            e
        })
        .context(Prune)
}

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

    /// Determine whether a row group should be read according to the meta data
    /// in the `row_groups`.
    ///
    /// The boolean value in the returned vector denotes the corresponding row
    /// group in the `row_groups` whether should be read.
    pub fn filter_row_groups(&self, schema: &Schema, row_groups: &[RowGroupMetaData]) -> Vec<bool> {
        let mut results = vec![true; row_groups.len()];
        let arrow_schema: SchemaRef = schema.clone().into_arrow_schema_ref();
        for expr in &self.exprs {
            match PruningPredicate::try_new(expr.clone(), arrow_schema.clone()) {
                Ok(pruning_predicate) => {
                    trace!("pruning_predicate is:{:?}", pruning_predicate);

                    if let Ok(values) = build_row_group_predicate(&pruning_predicate, row_groups) {
                        for (curr_val, result_val) in values.into_iter().zip(results.iter_mut()) {
                            *result_val = curr_val && *result_val
                        }
                    };
                    // if fail to build, just ignore this filter so that all the
                    // row groups should be read for this
                    // filter.
                }
                Err(e) => {
                    // for any error just ignore it and that is to say, for this filter all the row
                    // groups should be read.
                    error!("fail to build pruning predicate, err:{}", e);
                }
            }
        }

        results
    }

    pub fn exprs(&self) -> &[Expr] {
        &self.exprs
    }

    pub fn time_range(&self) -> TimeRange {
        self.time_range
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

    use arrow_deps::{
        arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField},
        datafusion::logical_expr::{expr_fn::col, lit},
        parquet::{
            basic::Type,
            file::{metadata::ColumnChunkMetaData, statistics::Statistics},
            schema::types::{SchemaDescPtr, SchemaDescriptor, Type as SchemaType},
        },
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
        let schema = arrow_schema.try_into().unwrap();
        let predicate = PredicateBuilder::default()
            .add_pushdown_exprs(&[expr])
            .build();
        let result = predicate.filter_row_groups(&schema, &[metadata]);
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
