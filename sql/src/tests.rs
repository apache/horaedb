// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use catalog::consts::{DEFAULT_CATALOG, DEFAULT_SCHEMA};
use common_types::{
    column_schema,
    datum::DatumKind,
    schema::{Builder, Schema, TSID_COLUMN},
    tests::{build_default_value_schema, build_schema},
};
use common_util::error::GenericResult;
use datafusion::catalog::TableReference;
use df_operator::{scalar::ScalarUdf, udaf::AggregateUdf};
use influxdb_influxql_parser::{parse_statements, select::SelectStatement, statement::Statement};
use table_engine::{
    memory::MemoryTable,
    table::{Table, TableId, TableRef},
    ANALYTIC_ENGINE_TYPE,
};

use crate::{
    influxql::{planner::MeasurementProvider, select::rewriter::Rewriter},
    provider::MetaProvider,
};

pub struct MockMetaProvider {
    tables: Vec<Arc<MemoryTable>>,
}

impl Default for MockMetaProvider {
    fn default() -> Self {
        Self {
            tables: vec![
                Arc::new(MemoryTable::new(
                    "test_table".to_string(),
                    TableId::from(100),
                    build_schema(),
                    ANALYTIC_ENGINE_TYPE.to_string(),
                )),
                Arc::new(MemoryTable::new(
                    "test_table2".to_string(),
                    TableId::from(101),
                    build_schema(),
                    ANALYTIC_ENGINE_TYPE.to_string(),
                )),
                Arc::new(MemoryTable::new(
                    "test_missing_columns_table".to_string(),
                    TableId::from(102),
                    build_default_value_schema(),
                    ANALYTIC_ENGINE_TYPE.to_string(),
                )),
                Arc::new(MemoryTable::new(
                    "__test_table".to_string(),
                    TableId::from(103),
                    build_schema(),
                    ANALYTIC_ENGINE_TYPE.to_string(),
                )),
                Arc::new(MemoryTable::new(
                    "influxql_test".to_string(),
                    TableId::from(144),
                    build_influxql_test_schema(),
                    ANALYTIC_ENGINE_TYPE.to_string(),
                )),
            ],
        }
    }
}

impl MetaProvider for MockMetaProvider {
    fn default_catalog_name(&self) -> &str {
        DEFAULT_CATALOG
    }

    fn default_schema_name(&self) -> &str {
        DEFAULT_SCHEMA
    }

    fn table(&self, name: TableReference) -> crate::provider::Result<Option<TableRef>> {
        let resolved = name.resolve(self.default_catalog_name(), self.default_schema_name());
        for table in &self.tables {
            if resolved.table == table.name() {
                return Ok(Some(table.clone()));
            }
        }

        Ok(None)
    }

    fn scalar_udf(&self, _name: &str) -> crate::provider::Result<Option<ScalarUdf>> {
        todo!()
    }

    fn aggregate_udf(&self, _name: &str) -> crate::provider::Result<Option<AggregateUdf>> {
        todo!()
    }
}

impl MeasurementProvider for MockMetaProvider {
    fn measurement(
        &self,
        measurement_name: &str,
    ) -> GenericResult<Option<table_engine::table::TableRef>> {
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

fn build_influxql_test_schema() -> Schema {
    Builder::new()
        .auto_increment_column_id(true)
        .add_key_column(
            column_schema::Builder::new(TSID_COLUMN.to_string(), DatumKind::UInt64)
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
        .add_key_column(
            column_schema::Builder::new("timestamp".to_string(), DatumKind::Timestamp)
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
        .add_normal_column(
            column_schema::Builder::new("col1".to_string(), DatumKind::String)
                .is_tag(true)
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
        .add_normal_column(
            column_schema::Builder::new("col2".to_string(), DatumKind::String)
                .is_tag(true)
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
        .add_normal_column(
            column_schema::Builder::new("col3".to_string(), DatumKind::Int64)
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
        .build()
        .expect("should succeed to build schema")
}
