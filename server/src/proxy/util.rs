// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::io::Cursor;

use arrow::{ipc::reader::StreamReader, record_batch::RecordBatch as ArrowRecordBatch};
use ceresdbproto::{
    prometheus::{
        expr::{Node, Node::Operand},
        operand::Value::Selector,
        sub_expr::OperatorType,
        Expr, SubExpr,
    },
    storage::{
        arrow_payload::Compression, sql_query_response::Output as OutputPb, ArrowPayload,
        SqlQueryResponse,
    },
};
use common_types::record_batch::RecordBatch;
use common_util::error::BoxError;
use datafusion::sql::sqlparser::ast::{SetExpr, TableFactor};
use interpreters::interpreter::Output;
use snafu::{OptionExt, ResultExt};
use sql::{
    ast::{Statement, TableName},
    parser::Parser,
};
use sqlparser::ast::Statement as SqlStatement;

use crate::proxy::error::{Internal, InternalNoCause, Result};

pub fn convert_sql_response_to_output(sql_query_response: SqlQueryResponse) -> Result<Output> {
    let output_pb = sql_query_response.output.context(InternalNoCause {
        msg: "Output is empty in sql query response".to_string(),
    })?;
    let output = match output_pb {
        OutputPb::AffectedRows(affected) => Output::AffectedRows(affected as usize),
        OutputPb::Arrow(arrow_payload) => {
            let arrow_record_batches = decode_arrow_payload(arrow_payload)?;
            let rows_group: Vec<RecordBatch> = arrow_record_batches
                .into_iter()
                .map(TryInto::<RecordBatch>::try_into)
                .map(|v| {
                    v.box_err().context(Internal {
                        msg: "decode arrow payload",
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            // let rows = rows_group.into_iter().flatten().collect::<Vec<_>>();

            Output::Records(rows_group)
        }
    };

    Ok(output)
}

fn decode_arrow_payload(arrow_payload: ArrowPayload) -> Result<Vec<ArrowRecordBatch>> {
    let compression = arrow_payload.compression();
    let byte_batches = arrow_payload.record_batches;

    // Maybe unzip payload bytes firstly.
    let unzip_byte_batches = byte_batches
        .into_iter()
        .map(|bytes_batch| match compression {
            Compression::None => Ok(bytes_batch),
            Compression::Zstd => zstd::stream::decode_all(Cursor::new(bytes_batch))
                .box_err()
                .context(Internal {
                    msg: "decode arrow payload",
                }),
        })
        .collect::<Result<Vec<Vec<u8>>>>()?;

    // Decode the byte batches to record batches, multiple record batches may be
    // included in one byte batch.
    let record_batches_group = unzip_byte_batches
        .into_iter()
        .map(|byte_batch| {
            // Decode bytes to `RecordBatch`.
            let stream_reader = match StreamReader::try_new(Cursor::new(byte_batch), None)
                .box_err()
                .context(Internal {
                    msg: "decode arrow payload",
                }) {
                Ok(reader) => reader,
                Err(e) => return Err(e),
            };

            stream_reader
                .into_iter()
                .map(|decode_result| {
                    decode_result.box_err().context(Internal {
                        msg: "decode arrow payload",
                    })
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<Vec<_>>>>()?;

    let record_batches = record_batches_group
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    Ok(record_batches)
}

// TODO: use parse_table_name in sql module and remove this function, after PR
// #802 merged.
pub fn parse_table_name_with_sql(sql: &str) -> Option<String> {
    let statements = if let Ok(v) = Parser::parse_sql(sql) {
        v
    } else {
        return None;
    };
    match &statements[0] {
        Statement::Standard(s) => match *s.clone() {
            SqlStatement::Insert { table_name, .. } => {
                Some(TableName::from(table_name).to_string())
            }
            SqlStatement::Explain { statement, .. } => {
                if let SqlStatement::Query(q) = *statement {
                    match *q.body {
                        SetExpr::Select(select) => {
                            if select.from.len() != 1 {
                                None
                            } else if let TableFactor::Table { name, .. } = &select.from[0].relation
                            {
                                Some(TableName::from(name.clone()).to_string())
                            } else {
                                None
                            }
                        }
                        _ => None,
                    }
                } else {
                    None
                }
            }
            SqlStatement::Query(q) => match *q.body {
                SetExpr::Select(select) => {
                    if select.from.len() != 1 {
                        None
                    } else if let TableFactor::Table { name, .. } = &select.from[0].relation {
                        Some(TableName::from(name.clone()).to_string())
                    } else {
                        None
                    }
                }
                _ => None,
            },
            _ => None,
        },
        Statement::Create(s) => Some(s.table_name.to_string()),
        Statement::Drop(s) => Some(s.table_name.to_string()),
        Statement::Describe(s) => Some(s.table_name.to_string()),
        Statement::AlterModifySetting(s) => Some(s.table_name.to_string()),
        Statement::AlterAddColumn(s) => Some(s.table_name.to_string()),
        Statement::ShowCreate(s) => Some(s.table_name.to_string()),
        Statement::ShowTables(_s) => None,
        Statement::ShowDatabases => None,
        Statement::Exists(s) => Some(s.table_name.to_string()),
    }
}

fn table_from_sub_expr(expr: &SubExpr) -> Option<String> {
    if expr.op_type == OperatorType::Aggr as i32 || expr.op_type == OperatorType::Func as i32 {
        return table_from_expr(&expr.operands[0]);
    }

    None
}

pub fn table_from_expr(expr: &Expr) -> Option<String> {
    if let Some(node) = &expr.node {
        match node {
            Operand(operand) => {
                if let Some(op_value) = &operand.value {
                    match op_value {
                        Selector(sel) => return Some(sel.measurement.to_string()),
                        _ => return None,
                    }
                }
            }
            Node::SubExpr(sub_expr) => return table_from_sub_expr(sub_expr),
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use std::{assert_eq, vec};

    use ceresdbproto::prometheus::{expr, operand::Value::Selector, Expr, Operand};

    use crate::proxy::util::table_from_expr;

    #[test]
    fn test_measurement_from_expr() {
        let expr = {
            let selector = ceresdbproto::prometheus::Selector {
                measurement: "aaa".to_string(),
                filters: vec![],
                start: 0,
                end: 12345678,
                align_start: 0,
                align_end: 12345678,
                step: 1,
                range: 1,
                offset: 1,
                field: "value".to_string(),
            };

            let oprand = Operand {
                value: Some(Selector(selector)),
            };

            Expr {
                node: Some(expr::Node::Operand(oprand)),
            }
        };

        let measurement = table_from_expr(&expr);
        assert_eq!(measurement, Some("aaa".to_owned()));
    }
}
