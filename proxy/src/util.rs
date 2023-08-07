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

use ceresdbproto::prometheus::{
    expr::{Node, Node::Operand},
    operand::Value::Selector,
    sub_expr::OperatorType,
    Expr, SubExpr,
};
use table_engine::partition::{format_sub_partition_table_name, PartitionInfo};

pub fn get_sub_partition_name(
    table_name: &str,
    partition_info: &PartitionInfo,
    id: usize,
) -> String {
    let partition_name = partition_info.get_definitions()[id].name.clone();
    format_sub_partition_table_name(table_name, &partition_name)
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

    use crate::util::table_from_expr;

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
