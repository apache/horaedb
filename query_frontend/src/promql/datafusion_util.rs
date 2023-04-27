// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    any::Any,
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};

use common_types::{schema::TSID_COLUMN, time::TimeRange};
use datafusion::{
    common::DFSchemaRef,
    logical_expr::{
        col, lit, Between, Expr as DataFusionExpr, Expr, LogicalPlan, UserDefinedLogicalNode,
    },
};

use crate::promql::pushdown::{AlignParameter, Func};

/// ColumnNames represents meaning of columns in one table.
#[derive(Debug, Hash, PartialEq)]
pub struct ColumnNames {
    pub timestamp: String,
    pub tag_keys: Vec<String>,
    pub field: String,
}

/// Translate to `column_name BETWEEN start AND end` expr
pub fn timerange_to_expr(query_range: TimeRange, column_name: &str) -> DataFusionExpr {
    DataFusionExpr::Between(Between {
        expr: Box::new(col(column_name)),
        negated: false,
        low: Box::new(lit(query_range.inclusive_start().as_i64())),
        high: Box::new(lit(query_range.exclusive_end().as_i64() - 1)),
    })
}

pub fn default_sort_exprs(timestamp_column: &str) -> Vec<DataFusionExpr> {
    vec![
        col(TSID_COLUMN).sort(true, true),
        col(timestamp_column).sort(true, true),
    ]
}

#[derive(Hash, PartialEq)]
pub struct PromAlignNode {
    pub input: LogicalPlan,
    pub column_name: Arc<ColumnNames>,
    pub table_name: String,
    pub func: Func,
    pub align_param: AlignParameter,
    pub read_parallelism: usize,
}

impl fmt::Debug for PromAlignNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for PromAlignNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "PromAlignNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        let qualified_name = |n| col(format!("{}.{}", self.table_name, n));

        let mut exprs = self
            .column_name
            .tag_keys
            .iter()
            .map(qualified_name)
            .collect::<Vec<_>>();

        exprs.extend(vec![
            qualified_name(&self.column_name.timestamp),
            qualified_name(&self.column_name.field),
        ]);

        exprs
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PromAlign: align_param={:?}, column_name={:?}, read_parallelism={}",
            self.align_param, self.column_name, self.read_parallelism
        )
    }

    fn from_template(
        &self,
        _exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> std::sync::Arc<dyn UserDefinedLogicalNode> {
        Arc::new(PromAlignNode {
            input: inputs[0].clone(),
            func: self.func,
            table_name: self.table_name.clone(),
            column_name: self.column_name.clone(),
            align_param: self.align_param,
            read_parallelism: self.read_parallelism,
        })
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
    }

    fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool {
        match other.as_any().downcast_ref::<Self>() {
            Some(o) => self == o,
            None => false,
        }
    }
}
