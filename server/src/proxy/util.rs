use ceresdbproto::prometheus::{
    expr::{Node, Node::Operand},
    operand::Value::Selector,
    sub_expr::OperatorType,
    Expr, SubExpr,
};

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
