// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use datafusion::sql::TableReference;
use influxdb_influxql_parser::{parse_statements, select::SelectStatement, statement::Statement};

use super::{planner::MeasurementProvider, select::rewriter::Rewriter};
use crate::{provider::MetaProvider, tests::MockMetaProvider};

impl MeasurementProvider for MockMetaProvider {
    fn measurement(
        &self,
        measurement_name: &str,
    ) -> crate::influxql::error::Result<Option<table_engine::table::TableRef>> {
        let table_ref = TableReference::Bare {
            table: std::borrow::Cow::Borrowed(measurement_name),
        };
        Ok(self.table(table_ref).unwrap())
    }
}

pub fn rewrite_statement(provider: &dyn MeasurementProvider, stmt: &mut SelectStatement) {
    let rewriter = Rewriter::new(provider);
    rewriter.rewrite(stmt).unwrap();
}

/// Returns the InfluxQL [`SelectStatement`] for the specified SQL, `s`.
pub fn parse_select(s: &str) -> SelectStatement {
    let statements = parse_statements(s).unwrap();
    match statements.first() {
        Some(Statement::Select(sel)) => *sel.clone(),
        _ => panic!("expected SELECT statement"),
    }
}
