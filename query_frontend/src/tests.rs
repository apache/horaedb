// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use catalog::consts::{DEFAULT_CATALOG, DEFAULT_SCHEMA};
use common_types::tests::{build_default_value_schema, build_schema, build_schema_for_cpu};
use datafusion::catalog::TableReference;
use df_operator::{scalar::ScalarUdf, udaf::AggregateUdf};
use table_engine::{
    memory::MemoryTable,
    table::{Table, TableId, TableRef},
    ANALYTIC_ENGINE_TYPE,
};

use crate::provider::MetaProvider;

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
                // Used in `test_remote_query_to_plan`
                Arc::new(MemoryTable::new(
                    "cpu".to_string(),
                    TableId::from(104),
                    build_schema_for_cpu(),
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

    fn all_tables(&self) -> crate::provider::Result<Vec<TableRef>> {
        todo!()
    }
}
