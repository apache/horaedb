// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{mem, sync::Arc};

use arrow::{compute, compute::kernels::cast_utils::string_to_timestamp_nanos, error::ArrowError};
use chrono::{Local, LocalResult, NaiveDateTime, TimeZone, Utc};
use datafusion::{
    arrow::datatypes::DataType,
    common::{
        tree_node::{TreeNode, TreeNodeRewriter},
        DFSchemaRef,
    },
    config::ConfigOptions,
    error::{DataFusionError, Result},
    logical_expr::{
        expr::{Expr, InList},
        logical_plan::{Filter, LogicalPlan, TableScan},
        utils, Between, BinaryExpr, ExprSchemable, Operator,
    },
    optimizer::analyzer::AnalyzerRule,
    scalar::ScalarValue,
};
use logger::debug;

/// Optimizer that cast literal value to target column's type
///
/// Example transformations that are applied:
/// * `expr > '5'` to `expr > 5` when `expr` is of numeric type
/// * `expr > '2021-12-02 15:00:34'` to `expr > 1638428434000(ms)` when `expr`
///   is of timestamp type
/// * `expr > 10` to `expr > '10'` when `expr` is of string type
/// * `expr = 'true'` to `expr = true` when `expr` is of boolean type
pub struct TypeConversion;

impl AnalyzerRule for TypeConversion {
    #[allow(clippy::only_used_in_recursion)]
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        #[allow(deprecated)]
        let mut rewriter = TypeRewriter {
            schemas: plan.all_schemas(),
        };

        match &plan {
            LogicalPlan::Filter(Filter {
                predicate, input, ..
            }) => {
                let input: &LogicalPlan = input;
                let predicate = predicate.clone().rewrite(&mut rewriter)?;
                let input = self.analyze(input.clone(), config)?;
                Ok(LogicalPlan::Filter(Filter::try_new(
                    predicate,
                    Arc::new(input),
                )?))
            }
            LogicalPlan::TableScan(TableScan {
                table_name,
                source,
                projection,
                projected_schema,
                filters,
                fetch,
            }) => {
                let rewrite_filters = filters
                    .clone()
                    .into_iter()
                    .map(|e| e.rewrite(&mut rewriter))
                    .collect::<Result<Vec<_>>>()?;
                Ok(LogicalPlan::TableScan(TableScan {
                    table_name: table_name.clone(),
                    source: source.clone(),
                    projection: projection.clone(),
                    projected_schema: projected_schema.clone(),
                    filters: rewrite_filters,
                    fetch: *fetch,
                }))
            }
            LogicalPlan::Projection { .. }
            | LogicalPlan::Window { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::Repartition { .. }
            | LogicalPlan::Extension { .. }
            | LogicalPlan::Sort { .. }
            | LogicalPlan::Explain { .. }
            | LogicalPlan::Limit { .. }
            | LogicalPlan::Union { .. }
            | LogicalPlan::Join { .. }
            | LogicalPlan::CrossJoin { .. }
            | LogicalPlan::Values { .. }
            | LogicalPlan::Analyze { .. }
            | LogicalPlan::Distinct { .. }
            | LogicalPlan::Prepare { .. }
            | LogicalPlan::DescribeTable { .. }
            | LogicalPlan::Ddl { .. }
            | LogicalPlan::Dml { .. } => {
                let inputs = plan.inputs();
                let new_inputs = inputs
                    .into_iter()
                    .map(|plan| self.analyze(plan.clone(), config))
                    .collect::<Result<Vec<_>>>()?;

                let expr = plan
                    .expressions()
                    .into_iter()
                    .map(|e| e.rewrite(&mut rewriter))
                    .collect::<Result<Vec<_>>>()?;

                Ok(utils::from_plan(&plan, &expr, &new_inputs)?)
            }
            LogicalPlan::Subquery(_)
            | LogicalPlan::Statement { .. }
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Unnest(_)
            | LogicalPlan::EmptyRelation { .. } => Ok(plan.clone()),
        }
    }

    fn name(&self) -> &str {
        "ceresdb_type_conversion"
    }
}

struct TypeRewriter<'a> {
    /// input schemas
    schemas: Vec<&'a DFSchemaRef>,
}

impl<'a> TypeRewriter<'a> {
    fn column_data_type(&self, expr: &Expr) -> Option<DataType> {
        if let Expr::Column(_) = expr {
            for schema in &self.schemas {
                if let Ok(v) = expr.get_type(schema) {
                    return Some(v);
                }
            }
        }

        None
    }

    fn convert_type<'b>(&self, mut left: &'b Expr, mut right: &'b Expr) -> Result<(Expr, Expr)> {
        let left_type = self.column_data_type(left);
        let right_type = self.column_data_type(right);

        let mut reverse = false;
        let left_type = match (&left_type, &right_type) {
            (Some(v), None) => v,
            (None, Some(v)) => {
                reverse = true;
                mem::swap(&mut left, &mut right);
                v
            }
            _ => return Ok((left.clone(), right.clone())),
        };

        match (left, right) {
            (Expr::Column(col), Expr::Literal(value)) => {
                let casted_right = Self::cast_scalar_value(value, left_type)?;
                debug!(
                    "TypeRewriter convert type, origin_left:{:?}, type:{}, right:{:?}, casted_right:{:?}",
                    col, left_type, value, casted_right
                );
                if casted_right.is_null() {
                    return Err(DataFusionError::Plan(format!(
                        "column:{col:?} value:{value:?} is invalid"
                    )));
                }
                if reverse {
                    Ok((Expr::Literal(casted_right), left.clone()))
                } else {
                    Ok((left.clone(), Expr::Literal(casted_right)))
                }
            }
            _ => Ok((left.clone(), right.clone())),
        }
    }

    fn cast_scalar_value(value: &ScalarValue, data_type: &DataType) -> Result<ScalarValue> {
        if let DataType::Timestamp(_, _) = data_type {
            if let ScalarValue::Utf8(Some(v)) = value {
                return match string_to_timestamp_ms_workaround(v) {
                    Ok(v) => Ok(v),
                    _ => string_to_timestamp_ms(v),
                };
            }
        }

        if let DataType::Boolean = data_type {
            if let ScalarValue::Utf8(Some(v)) = value {
                return match v.to_lowercase().as_str() {
                    "true" => Ok(ScalarValue::Boolean(Some(true))),
                    "false" => Ok(ScalarValue::Boolean(Some(false))),
                    _ => Ok(ScalarValue::Boolean(None)),
                };
            }
        }

        let array = value.to_array();
        ScalarValue::try_from_array(
            &compute::cast(&array, data_type).map_err(DataFusionError::ArrowError)?,
            // index: Converts a value in `array` at `index` into a ScalarValue
            0,
        )
    }
}

impl<'a> TreeNodeRewriter for TypeRewriter<'a> {
    type N = Expr;

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        let new_expr = match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
                Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq => {
                    let (left, right) = self.convert_type(&left, &right)?;
                    Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(left),
                        op,
                        right: Box::new(right),
                    })
                }
                _ => Expr::BinaryExpr(BinaryExpr { left, op, right }),
            },
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => {
                let (expr, low) = self.convert_type(&expr, &low)?;
                let (expr, high) = self.convert_type(&expr, &high)?;
                Expr::Between(Between {
                    expr: Box::new(expr),
                    negated,
                    low: Box::new(low),
                    high: Box::new(high),
                })
            }
            Expr::InList(InList {
                expr,
                list,
                negated,
            }) => {
                let mut list_expr = Vec::with_capacity(list.len());
                for e in list {
                    let (_, expr_conversion) = self.convert_type(&expr, &e)?;
                    list_expr.push(expr_conversion);
                }
                Expr::InList(InList {
                    expr,
                    list: list_expr,
                    negated,
                })
            }
            Expr::Literal(value) => match value {
                ScalarValue::TimestampSecond(Some(i), _) => {
                    timestamp_to_timestamp_ms_expr(TimestampType::Second, i)
                }
                ScalarValue::TimestampMicrosecond(Some(i), _) => {
                    timestamp_to_timestamp_ms_expr(TimestampType::Microsecond, i)
                }
                ScalarValue::TimestampNanosecond(Some(i), _) => {
                    timestamp_to_timestamp_ms_expr(TimestampType::Nanosecond, i)
                }
                _ => Expr::Literal(value),
            },
            expr => {
                // no rewrite possible
                expr
            }
        };
        Ok(new_expr)
    }
}

fn string_to_timestamp_ms(string: &str) -> Result<ScalarValue> {
    let ts = string_to_timestamp_nanos(string)
        .map(|t| t / 1_000_000)
        .map_err(DataFusionError::from)?;
    Ok(ScalarValue::TimestampMillisecond(Some(ts), None))
}

// TODO(lee): remove following codes after PR(https://github.com/apache/arrow-rs/pull/3787) merged
fn string_to_timestamp_ms_workaround(string: &str) -> Result<ScalarValue> {
    // Because function `string_to_timestamp_nanos` returns a NaiveDateTime's
    // nanoseconds from a string without a specify time zone, We need to convert
    // it to local timestamp.

    // without a timezone specifier as a local time, using 'T' as a separator
    // Example: 2020-09-08T13:42:29.190855
    if let Ok(ts) = NaiveDateTime::parse_from_str(string, "%Y-%m-%dT%H:%M:%S%.f") {
        let mills = naive_datetime_to_timestamp(string, ts).map_err(DataFusionError::from)?;
        return Ok(ScalarValue::TimestampMillisecond(Some(mills), None));
    }

    // without a timezone specifier as a local time, using ' ' as a separator
    // Example: 2020-09-08 13:42:29.190855
    if let Ok(ts) = NaiveDateTime::parse_from_str(string, "%Y-%m-%d %H:%M:%S%.f") {
        let mills = naive_datetime_to_timestamp(string, ts).map_err(DataFusionError::from)?;
        return Ok(ScalarValue::TimestampMillisecond(Some(mills), None));
    }

    Err(ArrowError::CastError(format!(
        "Error parsing '{string}' as timestamp: local time representation is invalid"
    )))
    .map_err(DataFusionError::from)
}

/// Converts the naive datetime (which has no specific timezone) to a
/// nanosecond epoch timestamp relative to UTC.
/// copy from:https://github.com/apache/arrow-rs/blob/6a6e7f72331aa6589aa676577571ffed98d52394/arrow/src/compute/kernels/cast_utils.rs#L208
fn naive_datetime_to_timestamp(s: &str, datetime: NaiveDateTime) -> Result<i64, ArrowError> {
    let l = Local {};

    match l.from_local_datetime(&datetime) {
        LocalResult::None => Err(ArrowError::CastError(format!(
            "Error parsing '{s}' as timestamp: local time representation is invalid"
        ))),
        LocalResult::Single(local_datetime) => {
            Ok(local_datetime.with_timezone(&Utc).timestamp_nanos() / 1_000_000)
        }

        LocalResult::Ambiguous(local_datetime, _) => {
            Ok(local_datetime.with_timezone(&Utc).timestamp_nanos() / 1_000_000)
        }
    }
}

enum TimestampType {
    Second,
    #[allow(dead_code)]
    Millisecond,
    Microsecond,
    Nanosecond,
}

fn timestamp_to_timestamp_ms_expr(typ: TimestampType, timestamp: i64) -> Expr {
    let timestamp = match typ {
        TimestampType::Second => timestamp * 1_000,
        TimestampType::Millisecond => timestamp,
        TimestampType::Microsecond => timestamp / 1_000,
        TimestampType::Nanosecond => timestamp / 1_000 / 1_000,
    };

    Expr::Literal(ScalarValue::TimestampMillisecond(Some(timestamp), None))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::datatypes::TimeUnit;
    use chrono::{NaiveDate, NaiveTime};
    use datafusion::{
        common::{DFField, DFSchema},
        prelude::col,
    };

    use super::*;
    use crate::datafusion_impl::logical_optimizer::type_conversion;

    fn expr_test_schema() -> DFSchemaRef {
        Arc::new(
            DFSchema::new_with_metadata(
                vec![
                    DFField::new_unqualified("c1", DataType::Utf8, true),
                    DFField::new_unqualified("c2", DataType::Int64, true),
                    DFField::new_unqualified("c3", DataType::Float64, true),
                    DFField::new_unqualified("c4", DataType::Float32, true),
                    DFField::new_unqualified("c5", DataType::Boolean, true),
                    DFField::new_unqualified(
                        "c6",
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        false,
                    ),
                ],
                HashMap::new(),
            )
            .unwrap(),
        )
    }

    #[test]
    fn test_type_conversion_int64() {
        let int_value = 100;
        let int_str = int_value.to_string();
        let not_int_str = "100ss".to_string();
        let schema = expr_test_schema();
        let mut rewriter = TypeRewriter {
            schemas: vec![&schema],
        };

        // Int64 c2 > "100" success
        let exp = col("c2").gt(Expr::Literal(ScalarValue::Utf8(Some(int_str.clone()))));
        let rewrite_exp = exp.rewrite(&mut rewriter).unwrap();
        assert_eq!(
            rewrite_exp,
            col("c2").gt(Expr::Literal(ScalarValue::Int64(Some(int_value)),))
        );

        // Int64 "100" > c2 success
        let exp = Expr::Literal(ScalarValue::Utf8(Some(int_str))).gt(col("c2"));
        let rewrite_exp = exp.rewrite(&mut rewriter).unwrap();
        assert_eq!(
            rewrite_exp,
            Expr::Literal(ScalarValue::Int64(Some(int_value))).gt(col("c2"))
        );

        // Int64 c2 > "100ss" fail
        let exp = col("c2").gt(Expr::Literal(ScalarValue::Utf8(Some(not_int_str))));
        assert!(exp.rewrite(&mut rewriter).is_err());
    }

    #[test]
    fn test_type_conversion_float() {
        let double_value = 100.1;
        let double_str = double_value.to_string();
        let not_int_str = "100ss".to_string();
        let schema = expr_test_schema();
        let mut rewriter = TypeRewriter {
            schemas: vec![&schema],
        };

        // Float64 c3 > "100" success
        let exp = col("c3").gt(Expr::Literal(ScalarValue::Utf8(Some(double_str.clone()))));
        let rewrite_exp = exp.rewrite(&mut rewriter).unwrap();
        assert_eq!(
            rewrite_exp,
            col("c3").gt(Expr::Literal(ScalarValue::Float64(Some(double_value)),))
        );

        // Float64 c3 > "100ss" fail
        let exp = col("c3").gt(Expr::Literal(ScalarValue::Utf8(Some(not_int_str.clone()))));
        assert!(exp.rewrite(&mut rewriter).is_err());

        // Float32 c4 > "100" success
        let exp = col("c4").gt(Expr::Literal(ScalarValue::Utf8(Some(double_str))));
        let rewrite_exp = exp.rewrite(&mut rewriter).unwrap();
        assert_eq!(
            rewrite_exp,
            col("c4").gt(Expr::Literal(ScalarValue::Float32(Some(
                double_value as f32
            )),))
        );

        // Float32 c4 > "100ss" fail
        let exp = col("c4").gt(Expr::Literal(ScalarValue::Utf8(Some(not_int_str))));
        assert!(exp.rewrite(&mut rewriter).is_err());
    }

    #[test]
    fn test_type_conversion_boolean() {
        let bool_value = true;
        let bool_str = bool_value.to_string();
        let not_int_str = "100ss".to_string();
        let schema = expr_test_schema();
        let mut rewriter = TypeRewriter {
            schemas: vec![&schema],
        };

        // Boolean c5 > "100ss" fail
        let exp = col("c5").gt(Expr::Literal(ScalarValue::Utf8(Some(not_int_str))));
        assert!(exp.rewrite(&mut rewriter).is_err());

        // Boolean c5 > "true" success
        let exp = col("c5").gt(Expr::Literal(ScalarValue::Utf8(Some(bool_str))));
        let rewrite_exp = exp.rewrite(&mut rewriter).unwrap();
        assert_eq!(
            rewrite_exp,
            col("c5").gt(Expr::Literal(ScalarValue::Boolean(Some(bool_value)),))
        );

        // Boolean c5 > true success
        let exp = col("c5").gt(Expr::Literal(ScalarValue::Boolean(Some(bool_value))));
        let rewrite_exp = exp.rewrite(&mut rewriter).unwrap();
        assert_eq!(
            rewrite_exp,
            col("c5").gt(Expr::Literal(ScalarValue::Boolean(Some(bool_value)),))
        );
    }

    #[test]
    fn test_type_conversion_timestamp() {
        let date_string = "2021-09-07T16:00:00Z".to_string();
        let schema = expr_test_schema();
        let mut rewriter = TypeRewriter {
            schemas: vec![&schema],
        };

        // Timestamp c6 > "2021-09-07 16:00:00"
        let exp = col("c6").gt(Expr::Literal(ScalarValue::Utf8(Some(date_string.clone()))));
        let rewrite_exp = exp.rewrite(&mut rewriter).unwrap();
        assert_eq!(
            rewrite_exp,
            col("c6").gt(Expr::Literal(ScalarValue::TimestampMillisecond(
                Some(
                    string_to_timestamp_nanos(&date_string)
                        .map(|t| t / 1_000_000)
                        .unwrap(),
                ),
                None
            ),))
        );

        // "2021-09-07 16:00:00" > Timestamp c6
        let exp = Expr::Literal(ScalarValue::Utf8(Some(date_string.clone()))).gt(col("c6"));
        let rewrite_exp = exp.rewrite(&mut rewriter).unwrap();
        assert_eq!(
            rewrite_exp,
            Expr::Literal(ScalarValue::TimestampMillisecond(
                Some(
                    string_to_timestamp_nanos(&date_string)
                        .map(|t| t / 1_000_000)
                        .unwrap(),
                ),
                None
            ),)
            .gt(col("c6"))
        );

        // Timestamp c6 > 1642141472
        let timestamp_int = 1642141472;
        let exp = col("c6").gt(Expr::Literal(ScalarValue::TimestampSecond(
            Some(timestamp_int),
            None,
        )));
        let rewrite_exp = exp.rewrite(&mut rewriter).unwrap();
        assert_eq!(
            rewrite_exp,
            col("c6").gt(Expr::Literal(ScalarValue::TimestampMillisecond(
                Some(timestamp_int * 1000),
                None
            )))
        );

        // Timestamp c6 between "2021-09-07 16:00:00" and "2021-09-07 17:00:00"
        let date_string2 = "2021-09-07T17:00:00Z".to_string();
        let exp = Expr::Between(Between {
            expr: Box::new(col("c6")),
            negated: false,
            low: Box::new(Expr::Literal(ScalarValue::Utf8(Some(date_string.clone())))),
            high: Box::new(Expr::Literal(ScalarValue::Utf8(Some(date_string2.clone())))),
        });
        let rewrite_exp = exp.rewrite(&mut rewriter).unwrap();
        assert_eq!(
            rewrite_exp,
            Expr::Between(Between {
                expr: Box::new(col("c6")),
                negated: false,
                low: Box::new(Expr::Literal(ScalarValue::TimestampMillisecond(
                    Some(
                        string_to_timestamp_nanos(&date_string)
                            .map(|t| t / 1_000_000)
                            .unwrap(),
                    ),
                    None
                ),)),
                high: Box::new(Expr::Literal(ScalarValue::TimestampMillisecond(
                    Some(
                        string_to_timestamp_nanos(&date_string2)
                            .map(|t| t / 1_000_000)
                            .unwrap(),
                    ),
                    None
                ),))
            })
        );
    }

    #[test]
    fn test_string_to_timestamp_ms_workaround() {
        let date_string = [
            "2021-09-07T16:00:00+08:00",
            "2021-09-07 16:00:00+08:00",
            "2021-09-07T16:00:00Z",
            "2021-09-07 16:00:00Z",
        ];
        for string in date_string {
            let result = type_conversion::string_to_timestamp_ms_workaround(string);
            assert!(result.is_err());
        }

        let date_string = "2021-09-07 16:00:00".to_string();
        let d = NaiveDate::from_ymd_opt(2021, 9, 7).unwrap();
        let t = NaiveTime::from_hms_milli_opt(16, 0, 0, 0).unwrap();
        let dt = NaiveDateTime::new(d, t);
        let expect = naive_datetime_to_timestamp(&date_string, dt).unwrap();
        let result = type_conversion::string_to_timestamp_ms_workaround(&date_string);
        if let Ok(ScalarValue::TimestampMillisecond(Some(mills), _)) = result {
            assert_eq!(mills, expect)
        }
    }
}
