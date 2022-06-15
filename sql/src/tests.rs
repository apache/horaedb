// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use arrow_deps::datafusion::catalog::TableReference;
use catalog::consts::{DEFAULT_CATALOG, DEFAULT_SCHEMA};
use common_types::tests::build_schema;
use table_engine::{
    memory::MemoryTable,
    table::{Table, TableId, TableRef},
    ANALYTIC_ENGINE_TYPE,
};
use df_operator::{scalar::ScalarUdf, udaf::AggregateUdf};

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
