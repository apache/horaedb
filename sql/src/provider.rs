// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Adapter to providers in datafusion

use std::{any::Any, cell::RefCell, collections::HashMap, sync::Arc};

use arrow_deps::{
    datafusion::{
        catalog::{catalog::CatalogProvider, schema::SchemaProvider},
        common::DataFusionError,
        datasource::{DefaultTableSource, TableProvider},
        physical_plan::{udaf::AggregateUDF, udf::ScalarUDF},
        sql::planner::ContextProvider,
    },
    datafusion_expr::TableSource,
};
use catalog::manager::ManagerRef;
use common_types::request_id::RequestId;
use df_operator::{registry::FunctionRegistry, scalar::ScalarUdf, udaf::AggregateUdf};
use snafu::{ResultExt, Snafu};
use table_engine::{provider::TableProviderAdapter, table::TableRef};

use crate::container::{TableContainer, TableReference};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to find catalog, name:{}, err:{}", name, source))]
    FindCatalog {
        name: String,
        source: catalog::manager::Error,
    },

    #[snafu(display("Failed to find schema, name:{}, err:{}", name, source))]
    FindSchema {
        name: String,
        source: catalog::Error,
    },

    #[snafu(display("Failed to find table, name:{}, err:{}", name, source))]
    FindTable {
        name: String,
        source: catalog::schema::Error,
    },

    #[snafu(display("Failed to find udf, err:{}", source))]
    FindUdf {
        source: df_operator::registry::Error,
    },
}

define_result!(Error);

/// MetaProvider provides meta info needed by Frontend
pub trait MetaProvider {
    /// Default catalog name
    fn default_catalog_name(&self) -> &str;

    /// Default schema name
    fn default_schema_name(&self) -> &str;

    /// Get table meta by table reference
    ///
    /// Note that this function may block current thread. We can't make this
    /// function async as the underlying (aka. datafusion) planner needs a
    /// sync provider.
    fn table(&self, name: TableReference) -> Result<Option<TableRef>>;

    /// Get udf by name.
    fn scalar_udf(&self, name: &str) -> Result<Option<ScalarUdf>>;

    /// Get udaf by name.
    fn aggregate_udf(&self, name: &str) -> Result<Option<AggregateUdf>>;
}

/// We use an adapter instead of using [catalog::Manager] directly, because
/// - MetaProvider provides blocking method, but catalog::Manager may provide
/// async method
/// - Other meta data like default catalog and schema are needed
// TODO(yingwen): Maybe support schema searching instead of using a fixed
// default schema
pub struct CatalogMetaProvider<'a> {
    pub manager: ManagerRef,
    pub default_catalog: &'a str,
    pub default_schema: &'a str,
    pub function_registry: &'a (dyn FunctionRegistry + Send + Sync),
}

impl<'a> MetaProvider for CatalogMetaProvider<'a> {
    fn default_catalog_name(&self) -> &str {
        self.default_catalog
    }

    fn default_schema_name(&self) -> &str {
        self.default_schema
    }

    fn table(&self, name: TableReference) -> Result<Option<TableRef>> {
        let resolved = name.resolve(self.default_catalog, self.default_schema);

        let catalog = match self
            .manager
            .catalog_by_name(resolved.catalog)
            .context(FindCatalog {
                name: resolved.catalog,
            })? {
            Some(c) => c,
            None => return Ok(None),
        };

        let schema = match catalog
            .schema_by_name(resolved.schema)
            .context(FindSchema {
                name: resolved.schema,
            })? {
            Some(s) => s,
            None => return Ok(None),
        };

        schema.table_by_name(resolved.table).context(FindTable {
            name: resolved.table,
        })
    }

    fn scalar_udf(&self, name: &str) -> Result<Option<ScalarUdf>> {
        self.function_registry.find_udf(name).context(FindUdf)
    }

    fn aggregate_udf(&self, name: &str) -> Result<Option<AggregateUdf>> {
        self.function_registry.find_udaf(name).context(FindUdf)
    }
}

/// An adapter to ContextProvider, not thread safe
pub struct ContextProviderAdapter<'a, P> {
    /// Local cache for TableProvider to avoid create multiple adapter for the
    /// same table, also save all the table needed during planning
    table_cache: RefCell<TableContainer>,
    /// Store the first error MetaProvider returns
    err: RefCell<Option<Error>>,
    meta_provider: &'a P,
    request_id: RequestId,
    /// Read parallelism for each table.
    read_parallelism: usize,
}

impl<'a, P: MetaProvider> ContextProviderAdapter<'a, P> {
    /// Create a adapter from meta provider
    pub fn new(meta_provider: &'a P, request_id: RequestId, read_parallelism: usize) -> Self {
        let default_catalog = meta_provider.default_catalog_name().to_string();
        let default_schema = meta_provider.default_schema_name().to_string();

        Self {
            table_cache: RefCell::new(TableContainer::new(default_catalog, default_schema)),
            err: RefCell::new(None),
            meta_provider,
            request_id,
            read_parallelism,
        }
    }

    /// Consumes the adapter, returning the tables used during planning if no
    /// error occurs, otherwise returning the error
    pub fn try_into_container(self) -> Result<TableContainer> {
        if let Some(e) = self.err.into_inner() {
            return Err(e);
        }

        Ok(self.table_cache.into_inner())
    }

    /// Save error if there is no existing error.
    ///
    /// The datafusion's ContextProvider can't return error, so here we save the
    /// error in the adapter and return None, also let datafusion
    /// return a provider not found error and abort the planning
    /// procedure.
    fn maybe_set_err(&self, err: Error) {
        if self.err.borrow().is_none() {
            *self.err.borrow_mut() = Some(err);
        }
    }
}

impl<'a, P: MetaProvider> MetaProvider for ContextProviderAdapter<'a, P> {
    fn default_catalog_name(&self) -> &str {
        self.meta_provider.default_catalog_name()
    }

    fn default_schema_name(&self) -> &str {
        self.meta_provider.default_schema_name()
    }

    fn table(&self, name: TableReference) -> Result<Option<TableRef>> {
        self.meta_provider.table(name)
    }

    fn scalar_udf(&self, name: &str) -> Result<Option<ScalarUdf>> {
        self.meta_provider.scalar_udf(name)
    }

    fn aggregate_udf(&self, name: &str) -> Result<Option<AggregateUdf>> {
        self.meta_provider.aggregate_udf(name)
    }
}

impl<'a, P: MetaProvider> ContextProvider for ContextProviderAdapter<'a, P> {
    fn get_table_provider(
        &self,
        name: TableReference,
    ) -> std::result::Result<Arc<(dyn TableSource + 'static)>, DataFusionError> {
        // Find in local cache
        if let Some(p) = self.table_cache.borrow().get(name) {
            return Ok(p);
        }

        // Find in meta provider
        match self.meta_provider.table(name) {
            Ok(Some(table)) => {
                let table_adapter = Arc::new(TableProviderAdapter::new(
                    table,
                    self.request_id,
                    self.read_parallelism,
                ));
                let table_source = Arc::new(DefaultTableSource {
                    table_provider: table_adapter,
                });
                // Put into cache
                self.table_cache
                    .borrow_mut()
                    .insert(name, table_source.clone());

                Ok(table_source)
            }
            Ok(None) => Err(DataFusionError::Execution(
                "MetaProvider not found".to_string(),
            )),
            Err(e) => {
                self.maybe_set_err(e);
                Err(DataFusionError::Execution(
                    "MetaProvider not found".to_string(),
                ))
            }
        }
    }

    // ScalarUDF is not supported now
    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        // We don't cache udf used by the query because now we will register all udf to
        // datafusion's context.
        match self.meta_provider.scalar_udf(name) {
            Ok(Some(udf)) => Some(udf.to_datafusion_udf()),
            Ok(None) => None,
            Err(e) => {
                self.maybe_set_err(e);
                None
            }
        }
    }

    // AggregateUDF is not supported now
    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        match self.meta_provider.aggregate_udf(name) {
            Ok(Some(udaf)) => Some(udaf.to_datafusion_udaf()),
            Ok(None) => None,
            Err(e) => {
                self.maybe_set_err(e);
                None
            }
        }
    }

    // TODO: Variable Type is not supported now
    fn get_variable_type(
        &self,
        _variable_names: &[String],
    ) -> Option<common_types::schema::DataType> {
        None
    }
}

struct SchemaProviderAdapter {
    catalog: String,
    schema: String,
    tables: Arc<TableContainer>,
}

impl SchemaProvider for SchemaProviderAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        let _ = self.tables.visit::<_, ()>(|name, table| {
            if name.catalog == self.catalog && name.schema == self.schema {
                let provider = table
                    .table_provider
                    .as_any()
                    .downcast_ref::<Arc<TableProviderAdapter>>()
                    .unwrap();
                names.push(provider.as_table_ref().name().to_string());
            }
            Ok(())
        });
        names
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let name_ref = TableReference::Full {
            catalog: &self.catalog,
            schema: &self.schema,
            table: name,
        };
        self.tables.get(name_ref).map(|v| {
            v.as_any()
                .downcast_ref::<Arc<TableProviderAdapter>>()
                .unwrap()
                .clone() as _
        })
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table(name).is_some()
    }
}

#[derive(Default)]
pub struct CatalogProviderAdapter {
    schemas: HashMap<String, Arc<SchemaProviderAdapter>>,
}

impl CatalogProviderAdapter {
    pub fn new_adapters(tables: Arc<TableContainer>) -> HashMap<String, CatalogProviderAdapter> {
        let mut catalog_adapters = HashMap::with_capacity(tables.num_catalogs());
        let _ = tables.visit::<_, ()>(|name, _| {
            // Get or create catalog
            let catalog = match catalog_adapters.get_mut(name.catalog) {
                Some(v) => v,
                None => catalog_adapters
                    .entry(name.catalog.to_string())
                    .or_insert_with(CatalogProviderAdapter::default),
            };
            // Get or create schema
            if catalog.schemas.get(name.schema).is_none() {
                catalog.schemas.insert(
                    name.schema.to_string(),
                    Arc::new(SchemaProviderAdapter {
                        catalog: name.catalog.to_string(),
                        schema: name.schema.to_string(),
                        tables: tables.clone(),
                    }),
                );
            }

            Ok(())
        });

        catalog_adapters
    }
}

impl CatalogProvider for CatalogProviderAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas
            .get(name)
            .cloned()
            .map(|v| v as Arc<dyn SchemaProvider>)
    }
}
