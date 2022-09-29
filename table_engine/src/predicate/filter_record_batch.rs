// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use common_types::{datum::DatumView, record_batch::RecordBatchWithKey};
use datafusion::{
    logical_plan::{Expr, Operator},
    scalar::ScalarValue,
};

#[derive(Debug)]
struct ColumnFilter {
    name: String,
    op: Operator,
    literal: ScalarValue,
}

fn evaluate_by_operator<T: PartialOrd>(lhs: &T, rhs: &T, op: &Operator) -> Option<bool> {
    let cmp_res = lhs.partial_cmp(rhs)?;
    let v = match op {
        Operator::Lt => cmp_res.is_lt(),
        Operator::LtEq => cmp_res.is_le(),
        Operator::Gt => cmp_res.is_gt(),
        Operator::GtEq => cmp_res.is_ge(),
        Operator::NotEq => cmp_res.is_ne(),
        Operator::Eq => cmp_res.is_eq(),
        _ => return None,
    };
    Some(v)
}

fn evaluate_datums_by_operator<'a>(
    lhs: &DatumView<'a>,
    rhs: &DatumView<'a>,
    op: &Operator,
) -> Option<bool> {
    macro_rules! impl_evaluate {
        ($($Kind: ident), *) => {
           match (lhs, rhs){
               (DatumView::Null, DatumView::Null) => Some(true),
               $((DatumView::$Kind(v1), DatumView::$Kind(v2)) => evaluate_by_operator(v1, v2, op),)*
               _ => None,
           }
        };
    }

    impl_evaluate!(
        Timestamp, Double, Float, Varbinary, String, UInt64, UInt32, UInt16, UInt8, Int64, Int32,
        Int16, Int8, Boolean
    )
}

impl ColumnFilter {
    fn filter(&self, record_batch: &RecordBatchWithKey, selected_buf: &mut [bool]) -> Option<()> {
        let filter_datum_view = DatumView::from_scalar_value(&self.literal)?;

        let column_idx = record_batch.schema_with_key().index_of(&self.name)?;
        let column_data = record_batch.column(column_idx);

        assert!(selected_buf.len() >= column_data.num_rows());
        for (i, selected) in selected_buf
            .iter_mut()
            .enumerate()
            .take(column_data.num_rows())
        {
            if *selected {
                let datum_view = column_data.datum_view(i);
                *selected = evaluate_datums_by_operator(&datum_view, &filter_datum_view, &self.op)
                    .unwrap_or(true);
            }
        }

        Some(())
    }
}

/// Filter record batch by applying the `column_filters`.
pub struct RecordBatchFilter {
    column_filters: Vec<ColumnFilter>,
}

impl RecordBatchFilter {
    /// Create filter according to the `exprs` whose logical relationship is
    /// `AND` between each other. Note that the created filter is not
    /// equivalent to the original `exprs` and actually only a subset of the
    /// exprs is chosen to create the [`RecordBatchFilter`].
    pub fn new(exprs: &[Expr]) -> Self {
        let mut filters = Vec::with_capacity(exprs.len());
        for expr in exprs {
            if let Expr::BinaryExpr { left, op, right } = expr {
                let (column_name, literal) = match (left.as_ref(), right.as_ref()) {
                    (Expr::Column(col), Expr::Literal(v))
                    | (Expr::Literal(v), Expr::Column(col)) => (col.name.to_string(), v.clone()),
                    _ => continue,
                };

                if matches!(
                    op,
                    Operator::NotEq
                        | Operator::Eq
                        | Operator::Gt
                        | Operator::GtEq
                        | Operator::Lt
                        | Operator::LtEq
                ) {
                    filters.push(ColumnFilter {
                        name: column_name,
                        op: *op,
                        literal,
                    })
                }
            }
        }

        RecordBatchFilter {
            column_filters: filters,
        }
    }

    /// Filter `record_batch` and save the filtering results into the
    /// `selected_rows_buf`.
    ///
    /// Requires: `selected_rows_buf.len() == record_batch.num_rows()`.
    pub fn filter(
        &self,
        record_batch: &RecordBatchWithKey,
        selected_rows_buf: &mut [bool],
    ) -> usize {
        assert_eq!(record_batch.num_rows(), selected_rows_buf.len());

        for selected in &mut *selected_rows_buf {
            *selected = true;
        }

        for column_filter in &self.column_filters {
            column_filter.filter(record_batch, selected_rows_buf.as_mut());
        }

        selected_rows_buf
            .iter()
            .map(|selected| if *selected { 1 } else { 0 })
            .sum()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.column_filters.is_empty()
    }
}

impl From<&[Expr]> for RecordBatchFilter {
    fn from(exprs: &[Expr]) -> Self {
        Self::new(exprs)
    }
}

#[cfg(test)]
mod test {
    use common_types::{
        row::Row,
        tests::{build_record_batch_with_key_by_rows, build_row},
    };
    use datafusion::prelude::Column;

    use super::*;

    fn build_record_batch(rows: Vec<Row>) -> RecordBatchWithKey {
        build_record_batch_with_key_by_rows(rows)
    }

    fn build_filter_expr(column_name: &str, literal: ScalarValue, op: Operator) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name(column_name.to_string()))),
            op,
            right: Box::new(Expr::Literal(literal)),
        }
    }

    #[test]
    fn test_empty_filter() {
        let rows = vec![
            build_row(b"aaaa", 1, 11.0, "AAAA"),
            build_row(b"aaaa", 1, 21.0, "BBBB"),
        ];
        let batch = build_record_batch(rows);

        let filter = RecordBatchFilter::new(&[]);
        let mut selected_rows = vec![false; batch.num_rows()];
        let selected_num = filter.filter(&batch, &mut selected_rows);

        assert_eq!(selected_num, selected_rows.len());
        assert!(selected_rows.iter().all(|v| *v));
    }

    #[test]
    fn test_all_filter() {
        let rows = vec![
            build_row(b"aaaa", 1, 11.0, "AAAA"),
            build_row(b"aaaa", 1, 21.0, "BBBB"),
            build_row(b"aaaa", 2, 21.0, "CCCC"),
            build_row(b"bbbb", 2, 31.0, "DDDD"),
            build_row(b"bbbb", 2, 31.0, "DDDD"),
        ];
        let batch = build_record_batch(rows);

        let expr = build_filter_expr("key2", ScalarValue::Int64(Some(2)), Operator::LtEq);
        let filter = RecordBatchFilter::new(&[expr]);
        let mut selected_rows = vec![false; batch.num_rows()];
        let selected_num = filter.filter(&batch, &mut selected_rows);

        assert_eq!(selected_num, selected_rows.len());
        assert!(selected_rows.iter().all(|v| *v));
    }

    #[test]
    fn test_partial_filter() {
        let rows = vec![
            build_row(b"aaaa", 1, 11.0, "AAAA"),
            build_row(b"aaaa", 1, 21.0, "BBBB"),
            build_row(b"aaaa", 2, 21.0, "CCCC"),
            build_row(b"bbbb", 2, 31.0, "DDDD"),
            build_row(b"bbbb", 2, 31.0, "DDDD"),
        ];
        let batch = build_record_batch(rows);

        let expr1 = build_filter_expr("key2", ScalarValue::Int64(Some(2)), Operator::LtEq);
        let expr2 = build_filter_expr(
            "key1",
            ScalarValue::Binary(Some(b"aabb".to_vec())),
            Operator::GtEq,
        );
        let filter = RecordBatchFilter::new(&[expr1, expr2]);
        let mut selected_rows = vec![false; batch.num_rows()];
        let selected_num = filter.filter(&batch, &mut selected_rows);
        let expect_selected_rows = vec![false, false, false, true, true];

        assert_eq!(selected_num, 2);
        assert_eq!(selected_rows, expect_selected_rows);
    }

    #[test]
    fn test_filter_empty_batch() {
        let batch = build_record_batch(vec![]);
        let expr1 = build_filter_expr("key2", ScalarValue::Int64(Some(2)), Operator::LtEq);
        let filter = RecordBatchFilter::new(&[expr1]);
        let mut selected_rows = vec![false; batch.num_rows()];
        filter.filter(&batch, &mut selected_rows);

        assert!(selected_rows.is_empty());
    }
}
