// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use influxdb_influxql_parser::{
    common::{MeasurementName, QualifiedMeasurementName},
    expression::{
        BinaryOperator as InfluxqlBinaryOperator, ConditionalExpression, ConditionalOperator,
        Expr as InfluxqlExpr,
    },
    literal::Literal,
    select::{Dimension, MeasurementSelection, SelectStatement},
};
use snafu::ensure;
use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, Ident, ObjectName, Offset,
    OffsetRows, Query, Select, SelectItem, SetExpr, TableFactor, TableWithJoins, Value,
};

use super::is_scalar_math_function;
use crate::influxql::error::*;

/// Used to convert influxql select statement to sql's.
#[allow(dead_code)]
pub struct StmtConverter;

impl StmtConverter {
    #[allow(dead_code)]
    pub fn convert(stmt: SelectStatement) -> Result<Query> {
        // Fields in `Query` needed to be converted.
        //  - limit
        //  - order by
        //  - limit
        //  - offset
        //  - select body
        let limit = stmt.limit.map(|limit| {
            let limit_n: u64 = *limit;
            Expr::Value(Value::Number(limit_n.to_string(), false))
        });

        let offset = stmt.offset.map(|offset| {
            let offset_n = *offset;
            let offset_val = Expr::Value(Value::Number(offset_n.to_string(), false));

            Offset {
                value: offset_val,
                rows: OffsetRows::None,
            }
        });

        // For select body:
        //  - projection
        //  - from
        //  - selection
        //  - group_by
        let projection_exprs = stmt
            .fields
            .iter()
            .map(|field| expr_to_sql_expr(ExprScope::Projection, &field.expr))
            .collect::<Result<Vec<_>>>()?;
        let projection = stmt
            .fields
            .iter()
            .zip(projection_exprs.into_iter())
            .map(|(field, expr)| match &field.alias {
                Some(alias) => SelectItem::ExprWithAlias {
                    expr,
                    alias: Ident::new(alias.to_string()),
                },
                None => SelectItem::UnnamedExpr(expr),
            })
            .collect();

        ensure!(
            stmt.from.len() == 1,
            Unimplemented {
                msg: "from multiple measurements",
            }
        );
        let measurement_name = match &stmt.from[0] {
            MeasurementSelection::Name(QualifiedMeasurementName { name, .. }) => match name {
                MeasurementName::Name(name) => name.to_string(),
                MeasurementName::Regex(re) => {
                    return Convert {
                        msg: format!("convert from to sql statement encounter regex, regex:{re}"),
                    }
                    .fail()
                }
            },
            MeasurementSelection::Subquery(_) => {
                return Unimplemented {
                    msg: "from subquery",
                }
                .fail()
            }
        };
        let table_factor = TableFactor::Table {
            name: ObjectName(vec![Ident::with_quote('`', measurement_name)]),
            alias: None,
            args: None,
            with_hints: Vec::default(),
        };
        let from = vec![TableWithJoins {
            relation: table_factor,
            joins: Vec::default(),
        }];

        let selection = match stmt.condition {
            Some(condition) => Some(conditional_to_sql_expr(&condition)?),
            None => None,
        };

        let group_by = match stmt.group_by {
            Some(keys) => keys
                .iter()
                .map(|key| match key {
                    Dimension::Time { .. } => Unimplemented {
                        msg: "group by time interval",
                    }
                    .fail(),
                    Dimension::Tag(tag) => Ok(Expr::Identifier(Ident::new(tag.to_string()))),
                    Dimension::Regex(re) => Convert {
                        msg: format!(
                            "convert group by to sql statement encounter regex, regex:{re}"
                        ),
                    }
                    .fail(),
                    Dimension::Wildcard => Convert {
                        msg: "convert group by to sql statement encounter wildcard",
                    }
                    .fail(),
                })
                .collect::<Result<Vec<_>>>()?,
            None => Vec::default(),
        };

        let body = Select {
            distinct: false,
            top: None,
            projection,
            into: None,
            from,
            lateral_views: Vec::default(),
            selection,
            group_by,
            cluster_by: Vec::default(),
            sort_by: Vec::default(),
            having: None,
            qualify: None,
            distribute_by: Vec::default(),
        };

        Ok(Query {
            with: None,
            body: Box::new(SetExpr::Select(Box::new(body))),
            order_by: Vec::default(),
            limit,
            offset,
            fetch: None,
            locks: Vec::default(),
        })
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
enum ExprScope {
    Projection,
    Where,
}

/// Map an InfluxQL [`InfluxqlExpr`] to a sql [`Expr`].
fn expr_to_sql_expr(scope: ExprScope, iql: &InfluxqlExpr) -> Result<Expr> {
    match iql {
        // rewriter is expected to expand wildcard expressions
        InfluxqlExpr::Wildcard(_) => Convert {
            msg: "unexpected wildcard in projection",
        }
        .fail(),

        InfluxqlExpr::VarRef {
            name,
            data_type: opt_dst_type,
        } => {
            if let Some(dst_type) = opt_dst_type {
                return Unimplemented {
                    msg: format!("cast to dst type, column:{name}, dst type:{dst_type}"),
                }
                .fail();
            };

            Ok(Expr::Identifier(Ident::new(name.to_string())))
        }

        InfluxqlExpr::BindParameter(_) => Unimplemented {
            msg: "bind parameter",
        }
        .fail(),

        InfluxqlExpr::Literal(val) => Ok(match val {
            Literal::Integer(v) => Expr::Value(Value::Number(v.to_string(), false)),
            Literal::Unsigned(v) => Expr::Value(Value::Number(v.to_string(), false)),
            Literal::Float(v) => Expr::Value(Value::Number(v.to_string(), false)),
            Literal::String(v) => Expr::Value(Value::SingleQuotedString(v.clone())),
            Literal::Timestamp(v) => Expr::Value(Value::SingleQuotedString(v.to_rfc3339())),
            Literal::Duration(_) => {
                return Unimplemented {
                    msg: "duration literal",
                }
                .fail()
            }
            Literal::Regex(re) => match scope {
                // a regular expression in a projection list is unexpected,
                // as it should have been expanded by the rewriter.
                ExprScope::Projection => {
                    return Convert {
                        msg: format!(
                            "convert projection to sql statement encounter regex, regex:{re}"
                        ),
                    }
                    .fail()
                }
                ExprScope::Where => {
                    return Unimplemented {
                        msg: "regex in where clause",
                    }
                    .fail()
                }
            },
            Literal::Boolean(v) => Expr::Value(Value::Boolean(*v)),
        }),

        InfluxqlExpr::Distinct(_) => Unimplemented { msg: "DISTINCT" }.fail(),

        InfluxqlExpr::Call { name, args } => call_to_sql_expr(scope, name, args),

        InfluxqlExpr::Binary { lhs, op, rhs } => binary_expr_to_sql_expr(scope, lhs, op, rhs),

        InfluxqlExpr::Nested(e) => expr_to_sql_expr(scope, e),
    }
}

fn call_to_sql_expr(scope: ExprScope, name: &str, args: &[InfluxqlExpr]) -> Result<Expr> {
    if is_scalar_math_function(name) {
        let name = ObjectName(vec![Ident::new(name.to_string())]);
        // TODO: Support `FunctionArg::Named`.
        let args = args
            .iter()
            .map(|arg| {
                let sql_expr_res = expr_to_sql_expr(scope, arg);
                sql_expr_res.map(|sql_expr| FunctionArg::Unnamed(FunctionArgExpr::Expr(sql_expr)))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Expr::Function(Function {
            name,
            args,
            over: None,
            distinct: false,
            special: false,
        }))
    } else {
        match scope {
            ExprScope::Projection => Unimplemented {
                msg: "aggregate and selector functions in projection list",
            }
            .fail(),

            ExprScope::Where => {
                if name.eq_ignore_ascii_case("now") {
                    Unimplemented {
                        msg: "now() in where clause",
                    }
                    .fail()
                } else {
                    Convert {
                        msg: format!("invalid function call in condition: {name}"),
                    }
                    .fail()
                }
            }
        }
    }
}

fn binary_expr_to_sql_expr(
    scope: ExprScope,
    lhs: &InfluxqlExpr,
    op: &InfluxqlBinaryOperator,
    rhs: &InfluxqlExpr,
) -> Result<Expr> {
    let left = Box::new(expr_to_sql_expr(scope, lhs)?);
    let right = Box::new(expr_to_sql_expr(scope, rhs)?);

    let op = match op {
        InfluxqlBinaryOperator::Add => BinaryOperator::Plus,
        InfluxqlBinaryOperator::Sub => BinaryOperator::Minus,
        InfluxqlBinaryOperator::Mul => BinaryOperator::Multiply,
        InfluxqlBinaryOperator::Div => BinaryOperator::Divide,
        InfluxqlBinaryOperator::Mod => BinaryOperator::Modulo,
        InfluxqlBinaryOperator::BitwiseAnd => BinaryOperator::BitwiseAnd,
        InfluxqlBinaryOperator::BitwiseOr => BinaryOperator::BitwiseOr,
        InfluxqlBinaryOperator::BitwiseXor => BinaryOperator::BitwiseXor,
    };

    Ok(Expr::BinaryOp { left, op, right })
}

/// Map an InfluxQL [`ConditionalExpression`] to a sql [`Expr`].
fn conditional_to_sql_expr(iql: &ConditionalExpression) -> Result<Expr> {
    match iql {
        ConditionalExpression::Expr(expr) => expr_to_sql_expr(ExprScope::Where, expr),
        ConditionalExpression::Binary { lhs, op, rhs } => {
            let op = conditional_op_to_operator(*op)?;
            let (lhs, rhs) = (conditional_to_sql_expr(lhs)?, conditional_to_sql_expr(rhs)?);

            Ok(Expr::BinaryOp {
                left: Box::new(lhs),
                op,
                right: Box::new(rhs),
            })
        }
        ConditionalExpression::Grouped(e) => conditional_to_sql_expr(e),
    }
}

fn conditional_op_to_operator(op: ConditionalOperator) -> Result<BinaryOperator> {
    match op {
        ConditionalOperator::Eq => Ok(BinaryOperator::Eq),
        ConditionalOperator::NotEq => Ok(BinaryOperator::NotEq),
        ConditionalOperator::EqRegex => Unimplemented {
            msg: "eq regex in where clause",
        }
        .fail(),
        ConditionalOperator::NotEqRegex => Unimplemented {
            msg: "not eq regex in where clause",
        }
        .fail(),
        ConditionalOperator::Lt => Ok(BinaryOperator::Lt),
        ConditionalOperator::LtEq => Ok(BinaryOperator::LtEq),
        ConditionalOperator::Gt => Ok(BinaryOperator::Gt),
        ConditionalOperator::GtEq => Ok(BinaryOperator::GtEq),
        ConditionalOperator::And => Ok(BinaryOperator::And),
        ConditionalOperator::Or => Ok(BinaryOperator::Or),
        // NOTE: This is not supported by InfluxQL SELECT expressions, so it is unexpected
        ConditionalOperator::In => Convert {
            msg: "unexpected binary operator: IN",
        }
        .fail(),
    }
}

#[cfg(test)]
mod test {
    use sqlparser::ast::Statement as SqlStatement;

    use crate::{
        ast::Statement,
        influxql::{stmt_converter::StmtConverter, test_util::parse_select},
        parser::Parser,
    };

    #[test]
    fn test_basic_convert() {
        // Common parts between influxql and sql, include:
        //  - limit
        //  - offset
        //  - projection
        //  - from(single table)
        //  - selection
        //  - group_by
        let stmt = parse_select(
            "SELECT a, sin(b), c 
            FROM influxql_test WHERE a < 4 and b > 4.5 GROUP BY c LIMIT 1 OFFSET 0",
        );
        let converted_sql_stmt = Statement::Standard(Box::new(SqlStatement::Query(Box::new(
            StmtConverter::convert(stmt).unwrap(),
        ))));

        let sql_stmts = Parser::parse_sql(
            "SELECT a, sin(b), c 
            FROM influxql_test WHERE a < 4 and b > 4.5 GROUP BY c LIMIT 1 OFFSET 0",
        )
        .unwrap();
        let expected_sql_stmt = sql_stmts.first().unwrap();
        assert_eq!(expected_sql_stmt, &converted_sql_stmt);
    }
}
