// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Table container

use std::{collections::HashMap, sync::Arc};

pub use arrow_deps::datafusion::catalog::{ResolvedTableReference, TableReference};
use arrow_deps::datafusion::datasource::DefaultTableSource;

// Rust has poor support of using tuple as map key, so we use a 3 level
// map to store catalog -> schema -> table mapping
type CatalogMap = HashMap<String, SchemaMap>;
type SchemaMap = HashMap<String, TableMap>;
type TableMap = HashMap<String, Arc<DefaultTableSource>>;

/// Container to hold table adapters
///
/// Optimized for default catalog and schema
#[derive(Default)]
pub struct TableContainer {
    default_catalog: String,
    default_schema: String,
    default_tables: HashMap<String, Arc<DefaultTableSource>>,
    other_tables: CatalogMap,
}

impl TableContainer {
    pub fn new(default_catalog: String, default_schema: String) -> Self {
        Self {
            default_catalog,
            default_schema,
            default_tables: HashMap::new(),
            other_tables: CatalogMap::new(),
        }
    }

    /// Catalog num
    pub fn num_catalogs(&self) -> usize {
        if self.other_tables.is_empty() {
            1
        } else {
            self.other_tables.len() + 1
        }
    }

    pub fn get(&self, name: TableReference) -> Option<Arc<DefaultTableSource>> {
        match name {
            TableReference::Bare { table } => self.get_default(table),
            TableReference::Partial { schema, table } => {
                if schema == self.default_schema {
                    self.get_default(table)
                } else {
                    self.get_other(&self.default_catalog, schema, table)
                }
            }
            TableReference::Full {
                catalog,
                schema,
                table,
            } => {
                if catalog == self.default_catalog && schema == self.default_schema {
                    self.get_default(table)
                } else {
                    self.get_other(catalog, schema, table)
                }
            }
        }
    }

    fn get_default(&self, table: &str) -> Option<Arc<DefaultTableSource>> {
        self.default_tables.get(table).cloned()
    }

    fn get_other(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Option<Arc<DefaultTableSource>> {
        self.other_tables
            .get(catalog)
            .and_then(|schemas| schemas.get(schema))
            .and_then(|tables| tables.get(table))
            .cloned()
        // .and_then(|provider| {
        //     Some(Arc::new(DefaultTableSource {
        //         table_provider: provider.clone(),
        //     }))
        // })
    }

    pub fn insert(&mut self, name: TableReference, table_adapter: Arc<DefaultTableSource>) {
        match name {
            TableReference::Bare { table } => self.insert_default(table, table_adapter),
            TableReference::Partial { schema, table } => {
                if schema == self.default_schema {
                    self.insert_default(table, table_adapter)
                } else {
                    self.insert_other(
                        self.default_catalog.clone(),
                        schema.to_string(),
                        table.to_string(),
                        table_adapter,
                    )
                }
            }
            TableReference::Full {
                catalog,
                schema,
                table,
            } => {
                if catalog == self.default_catalog && schema == self.default_schema {
                    self.insert_default(table, table_adapter)
                } else {
                    self.insert_other(
                        catalog.to_string(),
                        schema.to_string(),
                        table.to_string(),
                        table_adapter,
                    )
                }
            }
        }
    }

    fn insert_default(&mut self, table: &str, table_adapter: Arc<DefaultTableSource>) {
        self.default_tables.insert(table.to_string(), table_adapter);
    }

    fn insert_other(
        &mut self,
        catalog: String,
        schema: String,
        table: String,
        table_adapter: Arc<DefaultTableSource>,
    ) {
        self.other_tables
            .entry(catalog)
            .or_insert_with(HashMap::new)
            .entry(schema)
            .or_insert_with(HashMap::new)
            .insert(table, table_adapter);
    }

    /// Visit all tables
    ///
    /// If f returns error, stop iteration and return the error
    pub fn visit<F, E>(&self, mut f: F) -> Result<(), E>
    where
        F: FnMut(ResolvedTableReference, &Arc<DefaultTableSource>) -> Result<(), E>,
    {
        // Visit default tables first
        for (table, adapter) in &self.default_tables {
            // default_catalog/default_schema can be empty string, but that's
            // ok since we have table under them
            let table_ref = ResolvedTableReference {
                catalog: &self.default_catalog,
                schema: &self.default_schema,
                table,
            };
            f(table_ref, adapter)?;
        }

        // Visit other tables
        for (catalog, schemas) in &self.other_tables {
            for (schema, tables) in schemas {
                for (table, adapter) in tables {
                    let table_ref = ResolvedTableReference {
                        catalog,
                        schema,
                        table,
                    };
                    f(table_ref, adapter)?;
                }
            }
        }

        Ok(())
    }
}
