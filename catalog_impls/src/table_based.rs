// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Table based catalog implementation

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use catalog::{
    self, consts,
    manager::{self, Manager},
    schema::{
        self, AllocateTableId, CatalogMismatch, CloseOptions, CloseTableRequest, CreateExistTable,
        CreateOptions, CreateTableRequest, CreateTableWithCause, DropOptions, DropTableRequest,
        DropTableWithCause, NameRef, OpenOptions, OpenTableRequest, Schema, SchemaMismatch,
        SchemaRef, TooManyTable, WriteTableMeta,
    },
    Catalog, CatalogRef,
};
use common_util::{define_result, error::BoxError};
use log::{debug, error, info};
use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};
use system_catalog::sys_catalog_table::{
    self, CreateCatalogRequest, CreateSchemaRequest, SysCatalogTable, VisitOptions,
    VisitOptionsBuilder, VisitorCatalogNotFound, VisitorInner, VisitorSchemaNotFound,
};
use table_engine::{
    engine::{TableEngineRef, TableState},
    table::{
        ReadOptions, SchemaId, SchemaIdGenerator, TableId, TableInfo, TableRef, TableSeq,
        TableSeqGenerator,
    },
};
use tokio::sync::Mutex;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build sys catalog table, err:{}", source))]
    BuildSysCatalog {
        source: system_catalog::sys_catalog_table::Error,
    },

    #[snafu(display("Failed to visit sys catalog table, err:{}", source))]
    VisitSysCatalog {
        source: system_catalog::sys_catalog_table::Error,
    },

    #[snafu(display(
        "Failed to find table to update, name:{}.\nBacktrace:\n{}",
        name,
        backtrace
    ))]
    UpdateTableNotFound { name: String, backtrace: Backtrace },

    #[snafu(display("Failed to create catalog, catalog:{}, err:{}", catalog, source))]
    CreateCatalog {
        catalog: String,
        source: system_catalog::sys_catalog_table::Error,
    },

    #[snafu(display(
        "Failed to create schema, catalog:{}, schema:{}, err:{}",
        catalog,
        schema,
        source
    ))]
    CreateSchema {
        catalog: String,
        schema: String,
        source: system_catalog::sys_catalog_table::Error,
    },

    #[snafu(display(
        "Invalid schema id and table seq, schema_id:{:?}, table_seq:{:?}.\nBacktrace:\n{}",
        schema_id,
        table_seq,
        backtrace,
    ))]
    InvalidSchemaIdAndTableSeq {
        schema_id: SchemaId,
        table_seq: TableSeq,
        backtrace: Backtrace,
    },
}

define_result!(Error);

/// Table based catalog manager
pub struct TableBasedManager {
    /// Sys catalog table
    catalog_table: Arc<SysCatalogTable>,
    catalogs: CatalogMap,
    /// Global schema id generator, Each schema has a unique schema id.
    schema_id_generator: Arc<SchemaIdGenerator>,
}

impl Manager for TableBasedManager {
    fn default_catalog_name(&self) -> NameRef {
        consts::DEFAULT_CATALOG
    }

    fn default_schema_name(&self) -> NameRef {
        consts::DEFAULT_SCHEMA
    }

    fn catalog_by_name(&self, name: NameRef) -> manager::Result<Option<CatalogRef>> {
        let catalog = self.catalogs.get(name).cloned().map(|v| v as _);
        Ok(catalog)
    }

    fn all_catalogs(&self) -> manager::Result<Vec<CatalogRef>> {
        Ok(self.catalogs.values().map(|v| v.clone() as _).collect())
    }
}

impl TableBasedManager {
    /// Create and init the TableBasedManager.
    // TODO(yingwen): Define all constants in catalog crate.
    pub async fn new(backend: TableEngineRef) -> Result<Self> {
        // Create or open sys_catalog table, will also create a space (catalog + schema)
        // for system catalog.
        let catalog_table = SysCatalogTable::new(backend)
            .await
            .context(BuildSysCatalog)?;

        let mut manager = Self {
            catalog_table: Arc::new(catalog_table),
            catalogs: HashMap::new(),
            schema_id_generator: Arc::new(SchemaIdGenerator::default()),
        };

        manager.init().await?;

        Ok(manager)
    }

    pub async fn fetch_table_infos(&mut self) -> Result<Vec<TableInfo>> {
        let catalog_table = self.catalog_table.clone();

        let mut table_infos = Vec::default();
        let visitor_inner = VisitorInnerImpl {
            catalog_table: catalog_table.clone(),
            catalogs: &mut self.catalogs,
            schema_id_generator: self.schema_id_generator.clone(),
            table_infos: &mut table_infos,
        };

        let visit_opts = VisitOptionsBuilder::default().visit_table().build();

        Self::visit_catalog_table_with_options(catalog_table, visitor_inner, visit_opts).await?;

        Ok(table_infos)
    }

    /// Load all data from sys catalog table.
    async fn init(&mut self) -> Result<()> {
        // The system catalog and schema in it is not persisted, so we add it manually.
        self.load_system_catalog();

        // Load all existent catalog/schema from catalog_table
        let catalog_table = self.catalog_table.clone();

        let visitor_inner = VisitorInnerImpl {
            catalog_table: self.catalog_table.clone(),
            catalogs: &mut self.catalogs,
            schema_id_generator: self.schema_id_generator.clone(),
            table_infos: &mut Vec::default(),
        };

        let visit_opts = VisitOptionsBuilder::default()
            .visit_catalog()
            .visit_schema()
            .build();

        Self::visit_catalog_table_with_options(catalog_table, visitor_inner, visit_opts).await?;

        // Create default catalog if it is not exists.
        self.maybe_create_default_catalog().await?;

        Ok(())
    }

    async fn visit_catalog_table_with_options(
        catalog_table: Arc<SysCatalogTable>,
        mut visitor_inner: VisitorInnerImpl<'_>,
        visit_opts: VisitOptions,
    ) -> Result<()> {
        let opts = ReadOptions::default();

        catalog_table
            .visit(opts, &mut visitor_inner, visit_opts)
            .await
            .context(VisitSysCatalog)
    }

    fn load_system_catalog(&mut self) {
        // Get the `sys_catalog` table and add it to tables.
        let table = self.catalog_table.inner_table();
        let mut tables = SchemaTables::default();
        tables.insert(self.catalog_table.table_id(), table);

        // Use schema id of schema `system/public` as last schema id.
        let schema_id = system_catalog::SYSTEM_SCHEMA_ID;
        self.schema_id_generator.set_last_schema_id(schema_id);

        // Create the default schema in system catalog.
        let schema = Arc::new(SchemaImpl {
            catalog_name: consts::SYSTEM_CATALOG.to_string(),
            schema_name: consts::SYSTEM_CATALOG_SCHEMA.to_string(),
            schema_id,
            tables: RwLock::new(tables),
            mutex: Mutex::new(()),
            catalog_table: self.catalog_table.clone(),
            table_seq_generator: TableSeqGenerator::default(),
        });
        // Use table seq of `sys_catalog` table as last table seq.
        schema
            .table_seq_generator
            .set_last_table_seq(system_catalog::MAX_SYSTEM_TABLE_SEQ);

        let mut schemas = HashMap::new();
        schemas.insert(schema.name().to_string(), schema);

        let schema_id_generator = self.schema_id_generator.clone();
        let catalog_table = self.catalog_table.clone();
        // Create the system catalog.
        let catalog = Arc::new(CatalogImpl {
            name: consts::SYSTEM_CATALOG.to_string(),
            schemas: RwLock::new(schemas),
            schema_id_generator,
            catalog_table,
            mutex: Mutex::new(()),
        });

        self.catalogs.insert(catalog.name().to_string(), catalog);
    }

    async fn maybe_create_default_catalog(&mut self) -> Result<()> {
        // Try to get default catalog, create it if not exists.
        let catalog = match self.catalogs.get(consts::DEFAULT_CATALOG) {
            Some(v) => v.clone(),
            None => {
                // Only system catalog should exists.
                assert_eq!(1, self.catalogs.len());

                // Default catalog is not exists, create and store it.
                self.create_catalog(CreateCatalogRequest {
                    catalog_name: consts::DEFAULT_CATALOG.to_string(),
                })
                .await?
            }
        };

        // Create default schema if not exists.
        if catalog.find_schema(consts::DEFAULT_SCHEMA).is_none() {
            // Allocate schema id.
            let schema_id = self
                .schema_id_generator
                .alloc_schema_id()
                .expect("Schema id of default catalog should be valid");

            self.add_schema_to_catalog(
                CreateSchemaRequest {
                    catalog_name: consts::DEFAULT_CATALOG.to_string(),
                    schema_name: consts::DEFAULT_SCHEMA.to_string(),
                    schema_id,
                },
                &catalog,
            )
            .await?;
        }

        Ok(())
    }

    async fn create_catalog(&mut self, request: CreateCatalogRequest) -> Result<Arc<CatalogImpl>> {
        let catalog_name = request.catalog_name.clone();

        self.catalog_table
            .create_catalog(request)
            .await
            .context(CreateCatalog {
                catalog: &catalog_name,
            })?;

        let schema_id_generator = self.schema_id_generator.clone();
        let catalog_table = self.catalog_table.clone();
        let catalog = Arc::new(CatalogImpl {
            name: catalog_name.clone(),
            schemas: RwLock::new(HashMap::new()),
            schema_id_generator,
            catalog_table,
            mutex: Mutex::new(()),
        });

        self.catalogs.insert(catalog_name, catalog.clone());

        Ok(catalog)
    }

    async fn add_schema_to_catalog(
        &mut self,
        request: CreateSchemaRequest,
        catalog: &CatalogImpl,
    ) -> Result<Arc<SchemaImpl>> {
        let schema_name = request.schema_name.clone();
        let schema_id = request.schema_id;

        self.catalog_table
            .create_schema(request)
            .await
            .context(CreateSchema {
                catalog: &catalog.name,
                schema: &schema_name,
            })?;

        let schema = Arc::new(SchemaImpl::new(
            &catalog.name,
            &schema_name,
            schema_id,
            self.catalog_table.clone(),
        ));

        catalog.insert_schema_into_memory(schema.clone());

        Ok(schema)
    }
}

type CatalogMap = HashMap<String, Arc<CatalogImpl>>;

/// Sys catalog visitor implementation, used to load catalog info
struct VisitorInnerImpl<'a> {
    catalog_table: Arc<SysCatalogTable>,
    catalogs: &'a mut CatalogMap,
    schema_id_generator: Arc<SchemaIdGenerator>,
    table_infos: &'a mut Vec<TableInfo>,
}

#[async_trait]
impl<'a> VisitorInner for VisitorInnerImpl<'a> {
    fn visit_catalog(&mut self, request: CreateCatalogRequest) -> sys_catalog_table::Result<()> {
        debug!("Visitor visit catalog, request:{:?}", request);
        let schema_id_generator = self.schema_id_generator.clone();
        let catalog_table = self.catalog_table.clone();

        let catalog = CatalogImpl {
            name: request.catalog_name.to_string(),
            schemas: RwLock::new(HashMap::new()),
            schema_id_generator,
            catalog_table,
            mutex: Mutex::new(()),
        };

        // Register catalog.
        self.catalogs
            .insert(request.catalog_name, Arc::new(catalog));

        Ok(())
    }

    fn visit_schema(&mut self, request: CreateSchemaRequest) -> sys_catalog_table::Result<()> {
        debug!("Visitor visit schema, request:{:?}", request);

        let catalog =
            self.catalogs
                .get_mut(&request.catalog_name)
                .context(VisitorCatalogNotFound {
                    catalog: &request.catalog_name,
                })?;

        let schema_id = request.schema_id;
        let schema = Arc::new(SchemaImpl::new(
            &request.catalog_name,
            &request.schema_name,
            schema_id,
            self.catalog_table.clone(),
        ));

        // If schema exists, we overwrite it.
        catalog.insert_schema_into_memory(schema);

        // Update last schema id.
        if self.schema_id_generator.last_schema_id_u32() < schema_id.as_u32() {
            self.schema_id_generator.set_last_schema_id(schema_id);
        }

        Ok(())
    }

    fn visit_tables(&mut self, table_info: TableInfo) -> sys_catalog_table::Result<()> {
        debug!("Visitor visit tables, table_info:{:?}", table_info);

        let catalog =
            self.catalogs
                .get_mut(&table_info.catalog_name)
                .context(VisitorCatalogNotFound {
                    catalog: &table_info.catalog_name,
                })?;
        let schema =
            catalog
                .find_schema(&table_info.schema_name)
                .context(VisitorSchemaNotFound {
                    catalog: &table_info.catalog_name,
                    schema: &table_info.schema_name,
                })?;

        // Update max table sequence of the schema.
        let table_id = table_info.table_id;
        let table_seq = TableSeq::from(table_id);
        if table_seq.as_u64() >= schema.table_seq_generator.last_table_seq_u64() {
            schema.table_seq_generator.set_last_table_seq(table_seq);
        }

        // Only the stable/altering table can be opened.
        if !matches!(table_info.state, TableState::Stable) {
            debug!(
                "Visitor visit a unstable table, table_info:{:?}",
                table_info
            );
            return Ok(());
        }

        // Collect table infos for later opening.
        self.table_infos.push(table_info);

        Ok(())
    }
}

type SchemaMap = HashMap<String, Arc<SchemaImpl>>;

/// Table based catalog
struct CatalogImpl {
    /// Catalog name
    name: String,
    /// Schemas of catalog
    // Now the Schema trait does not support create schema, so we use impl type here
    schemas: RwLock<SchemaMap>,
    /// Global schema id generator, Each schema has a unique schema id.
    schema_id_generator: Arc<SchemaIdGenerator>,
    /// Sys catalog table
    catalog_table: Arc<SysCatalogTable>,
    /// Mutex
    ///
    /// Protects:
    /// - create schema
    /// - persist to default catalog
    mutex: Mutex<()>,
}

impl CatalogImpl {
    /// Insert schema
    fn insert_schema_into_memory(&self, schema: Arc<SchemaImpl>) {
        let mut schemas = self.schemas.write().unwrap();
        schemas.insert(schema.name().to_string(), schema);
    }

    fn find_schema(&self, schema_name: &str) -> Option<Arc<SchemaImpl>> {
        let schemas = self.schemas.read().unwrap();
        schemas.get(schema_name).cloned()
    }
}

// TODO(yingwen): Support add schema (with options to control schema
// persistence)
#[async_trait]
impl Catalog for CatalogImpl {
    fn name(&self) -> NameRef {
        &self.name
    }

    fn schema_by_name(&self, name: NameRef) -> catalog::Result<Option<SchemaRef>> {
        let schemas = self.schemas.read().unwrap();
        let schema = schemas.get(name).cloned().map(|v| v as _);
        Ok(schema)
    }

    async fn create_schema<'a>(&'a self, name: NameRef<'a>) -> catalog::Result<()> {
        // Check schema existence
        if self.schema_by_name(name)?.is_some() {
            return Ok(());
        }

        // Lock schema and persist schema to default catalog
        let _lock = self.mutex.lock().await;
        // Check again
        if self.schema_by_name(name)?.is_some() {
            return Ok(());
        }

        // Allocate schema id.
        let schema_id = self
            .schema_id_generator
            .alloc_schema_id()
            .expect("Schema id of default catalog should be valid");

        let request = CreateSchemaRequest {
            catalog_name: self.name.to_string(),
            schema_name: name.to_string(),
            schema_id,
        };

        let schema_id = request.schema_id;

        self.catalog_table
            .create_schema(request)
            .await
            .box_err()
            .context(catalog::CreateSchemaWithCause {
                catalog: &self.name,
                schema: &name.to_string(),
            })?;

        let schema = Arc::new(SchemaImpl::new(
            &self.name,
            name,
            schema_id,
            self.catalog_table.clone(),
        ));

        self.insert_schema_into_memory(schema);
        info!(
            "create schema success, catalog:{}, schema:{}",
            &self.name, name
        );
        Ok(())
    }

    fn all_schemas(&self) -> catalog::Result<Vec<SchemaRef>> {
        Ok(self
            .schemas
            .read()
            .unwrap()
            .iter()
            .map(|(_, v)| v.clone() as _)
            .collect())
    }
}

/// Table based schema
struct SchemaImpl {
    /// Catalog name
    catalog_name: String,
    /// Schema name
    schema_name: String,
    /// Schema id
    schema_id: SchemaId,
    /// Tables of schema
    tables: RwLock<SchemaTables>,
    /// Mutex
    ///
    /// Protects:
    /// - add/drop/alter table
    /// - persist to sys catalog table
    mutex: Mutex<()>,
    /// Sys catalog table
    catalog_table: Arc<SysCatalogTable>,
    table_seq_generator: TableSeqGenerator,
}

impl SchemaImpl {
    fn new(
        catalog_name: &str,
        schema_name: &str,
        schema_id: SchemaId,
        catalog_table: Arc<SysCatalogTable>,
    ) -> Self {
        Self {
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
            schema_id,
            tables: RwLock::new(SchemaTables::default()),
            mutex: Mutex::new(()),
            catalog_table,
            table_seq_generator: TableSeqGenerator::default(),
        }
    }

    fn validate_schema_info(&self, catalog_name: &str, schema_name: &str) -> schema::Result<()> {
        ensure!(
            self.catalog_name == catalog_name,
            CatalogMismatch {
                expect: &self.catalog_name,
                given: catalog_name,
            }
        );
        ensure!(
            self.schema_name == schema_name,
            SchemaMismatch {
                expect: &self.schema_name,
                given: schema_name,
            }
        );

        Ok(())
    }

    /// Insert table into memory, wont check existence
    fn insert_table_into_memory(&self, table_id: TableId, table: TableRef) {
        let mut tables = self.tables.write().unwrap();
        tables.insert(table_id, table);
    }

    /// Check table existence in read lock
    ///
    /// If table exists:
    /// - if create_if_not_exists is true, return Ok
    /// - if create_if_not_exists is false, return Error
    fn check_create_table_read(
        &self,
        table_name: &str,
        create_if_not_exists: bool,
    ) -> schema::Result<Option<TableRef>> {
        let tables = self.tables.read().unwrap();
        if let Some(table) = tables.tables_by_name.get(table_name) {
            // Already exists
            if create_if_not_exists {
                // Create if not exists is set
                return Ok(Some(table.clone()));
            }
            // Create if not exists is not set, need to return error
            return CreateExistTable { table: table_name }.fail();
        }

        Ok(None)
    }

    fn find_table_by_name(&self, name: NameRef) -> Option<TableRef> {
        self.tables
            .read()
            .unwrap()
            .tables_by_name
            .get(name)
            .cloned()
    }

    async fn alloc_table_id<'a>(&self, name: NameRef<'a>) -> schema::Result<TableId> {
        let table_seq = self
            .table_seq_generator
            .alloc_table_seq()
            .context(TooManyTable {
                schema: &self.schema_name,
                table: name,
            })?;

        TableId::with_seq(self.schema_id, table_seq)
            .context(InvalidSchemaIdAndTableSeq {
                schema_id: self.schema_id,
                table_seq,
            })
            .box_err()
            .context(AllocateTableId {
                schema: &self.schema_name,
                table: name,
            })
    }
}

#[derive(Default)]
struct SchemaTables {
    tables_by_name: HashMap<String, TableRef>,
    tables_by_id: HashMap<TableId, TableRef>,
}

impl SchemaTables {
    fn insert(&mut self, table_id: TableId, table: TableRef) {
        self.tables_by_name
            .insert(table.name().to_string(), table.clone());
        self.tables_by_id.insert(table_id, table);
    }

    fn remove(&mut self, name: NameRef) {
        if let Some(table) = self.tables_by_name.remove(name) {
            self.tables_by_id.remove(&table.id());
        }
    }
}

#[async_trait]
impl Schema for SchemaImpl {
    fn name(&self) -> NameRef {
        &self.schema_name
    }

    fn id(&self) -> SchemaId {
        self.schema_id
    }

    fn table_by_name(&self, name: NameRef) -> schema::Result<Option<TableRef>> {
        let table = self
            .tables
            .read()
            .unwrap()
            .tables_by_name
            .get(name)
            .cloned();
        Ok(table)
    }

    // TODO(yingwen): Do not persist if engine is memory engine.
    async fn create_table(
        &self,
        request: CreateTableRequest,
        opts: CreateOptions,
    ) -> schema::Result<TableRef> {
        info!(
            "Table based catalog manager create table, request:{:?}",
            request
        );

        self.validate_schema_info(&request.catalog_name, &request.schema_name)?;

        // TODO(yingwen): Validate table id is unique.

        // Check table existence
        if let Some(table) =
            self.check_create_table_read(&request.table_name, opts.create_if_not_exists)?
        {
            return Ok(table);
        }

        // Lock schema and persist table to sys catalog table
        let _lock = self.mutex.lock().await;
        // Check again
        if let Some(table) =
            self.check_create_table_read(&request.table_name, opts.create_if_not_exists)?
        {
            return Ok(table);
        }

        // Create table
        let table_id = self.alloc_table_id(&request.table_name).await?;
        let request = request.into_engine_create_request(table_id);
        let table_name = request.table_name.clone();
        let table = opts
            .table_engine
            .create_table(request.clone())
            .await
            .box_err()
            .context(CreateTableWithCause)?;
        assert_eq!(table_name, table.name());

        self.catalog_table
            .create_table(request.clone().into())
            .await
            .box_err()
            .context(WriteTableMeta {
                table: &request.table_name,
            })?;

        {
            // Insert into memory
            let mut tables = self.tables.write().unwrap();
            tables.insert(request.table_id, table.clone());
        }

        Ok(table)
    }

    async fn drop_table(
        &self,
        mut request: DropTableRequest,
        opts: DropOptions,
    ) -> schema::Result<bool> {
        info!(
            "Table based catalog manager drop table, request:{:?}",
            request
        );

        self.validate_schema_info(&request.catalog_name, &request.schema_name)?;

        if self.find_table_by_name(&request.table_name).is_none() {
            return Ok(false);
        };

        let _lock = self.mutex.lock().await;
        // double check whether the table to drop exists.
        let table = match self.find_table_by_name(&request.table_name) {
            Some(v) => v,
            None => return Ok(false),
        };

        // Determine the real engine type of the table to drop.
        // FIXME(xikai): the engine should not be part of the DropRequest.
        request.engine = table.engine_type().to_string();

        // Prepare to drop table info in the sys_catalog.
        self.catalog_table
            .prepare_drop_table(request.clone())
            .await
            .box_err()
            .context(WriteTableMeta {
                table: &request.table_name,
            })?;

        let dropped = opts
            .table_engine
            .drop_table(request.clone())
            .await
            .box_err()
            .context(DropTableWithCause)?;

        info!(
            "Table engine drop table successfully, request:{:?}, dropped:{}",
            request, dropped
        );

        // Update the drop table record into the sys_catalog_table.
        self.catalog_table
            .drop_table(request.clone())
            .await
            .box_err()
            .context(WriteTableMeta {
                table: &request.table_name,
            })?;

        {
            let mut tables = self.tables.write().unwrap();
            tables.remove(&request.table_name);
        };

        info!(
            "Table based catalog manager drop table successfully, request:{:?}",
            request
        );

        return Ok(true);
    }

    async fn open_table(
        &self,
        request: OpenTableRequest,
        opts: OpenOptions,
    ) -> schema::Result<Option<TableRef>> {
        debug!(
            "Table based catalog manager open table, request:{:?}",
            request
        );

        self.validate_schema_info(&request.catalog_name, &request.schema_name)?;

        // Do opening work.
        let table_name = request.table_name.clone();
        let table_id = request.table_id;
        let table_opt = opts
            .table_engine
            .open_table(request.clone())
            .await
            .box_err()
            .context(schema::OpenTableWithCause)?;

        match table_opt {
            Some(table) => {
                self.insert_table_into_memory(table_id, table.clone());

                Ok(Some(table))
            }

            None => {
                // Now we ignore the error that table not in engine but in catalog.
                error!(
                    "Visitor found table not in engine, table_name:{:?}, table_id:{}",
                    table_name, table_id,
                );

                Ok(None)
            }
        }
    }

    async fn close_table(
        &self,
        request: CloseTableRequest,
        _opts: CloseOptions,
    ) -> schema::Result<()> {
        debug!(
            "Table based catalog manager close table, request:{:?}",
            request
        );

        self.validate_schema_info(&request.catalog_name, &request.schema_name)?;

        schema::UnSupported {
            msg: "close table is not supported",
        }
        .fail()
    }

    fn all_tables(&self) -> schema::Result<Vec<TableRef>> {
        Ok(self
            .tables
            .read()
            .unwrap()
            .tables_by_name
            .values()
            .cloned()
            .collect())
    }
}

#[cfg(any(test, feature = "test"))]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use analytic_engine::tests::util::{EngineBuildContext, RocksDBEngineBuildContext, TestEnv};
    use catalog::{
        consts::DEFAULT_CATALOG,
        manager::Manager,
        schema::{CreateOptions, CreateTableRequest, DropOptions, DropTableRequest, SchemaRef},
    };
    use common_types::table::{DEFAULT_CLUSTER_VERSION, DEFAULT_SHARD_ID};
    use table_engine::{
        engine::{TableEngineRef, TableState},
        memory::MemoryTableEngine,
        proxy::TableEngineProxy,
        ANALYTIC_ENGINE_TYPE,
    };

    use crate::table_based::TableBasedManager;

    async fn build_catalog_manager(analytic: TableEngineRef) -> TableBasedManager {
        // Create catalog manager, use analytic table as backend
        TableBasedManager::new(analytic.clone())
            .await
            .expect("Failed to create catalog manager")
    }

    async fn build_default_schema_with_catalog(catalog_manager: &TableBasedManager) -> SchemaRef {
        let catalog_name = catalog_manager.default_catalog_name();
        let schema_name = catalog_manager.default_schema_name();
        let catalog = catalog_manager.catalog_by_name(catalog_name);
        assert!(catalog.is_ok());
        assert!(catalog.as_ref().unwrap().is_some());
        catalog
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap()
            .schema_by_name(schema_name)
            .unwrap()
            .unwrap()
    }

    async fn build_create_table_req(table_name: &str, schema: SchemaRef) -> CreateTableRequest {
        CreateTableRequest {
            catalog_name: DEFAULT_CATALOG.to_string(),
            schema_name: schema.name().to_string(),
            schema_id: schema.id(),
            table_name: table_name.to_string(),
            table_schema: common_types::tests::build_schema(),
            engine: ANALYTIC_ENGINE_TYPE.to_string(),
            options: HashMap::new(),
            state: TableState::Stable,
            shard_id: DEFAULT_SHARD_ID,
            cluster_version: DEFAULT_CLUSTER_VERSION,
            partition_info: None,
        }
    }

    #[tokio::test]
    async fn test_catalog_by_name_schema_by_name_rocks() {
        let rocksdb_ctx = RocksDBEngineBuildContext::default();
        test_catalog_by_name_schema_by_name(rocksdb_ctx).await;
    }

    async fn test_catalog_by_name_schema_by_name<T>(engine_context: T)
    where
        T: EngineBuildContext,
    {
        let env = TestEnv::builder().build();
        let mut test_ctx = env.new_context(engine_context);
        test_ctx.open().await;

        let catalog_manager = build_catalog_manager(test_ctx.clone_engine()).await;
        let catalog_name = catalog_manager.default_catalog_name();
        let schema_name = catalog_manager.default_schema_name();
        let catalog = catalog_manager.catalog_by_name(catalog_name);
        assert!(catalog.is_ok());
        assert!(catalog.as_ref().unwrap().is_some());

        let schema = catalog
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap()
            .schema_by_name(schema_name);
        assert!(schema.is_ok());
        assert!(schema.as_ref().unwrap().is_some());

        let schema_name2 = "test";
        let schema = catalog
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap()
            .schema_by_name(schema_name2);
        assert!(schema.is_ok());
        assert!(schema.as_ref().unwrap().is_none());

        let catalog_name2 = "test";
        let catalog = catalog_manager.catalog_by_name(catalog_name2);
        assert!(catalog.is_ok());
        assert!(catalog.as_ref().unwrap().is_none());
    }

    #[tokio::test]
    async fn test_maybe_create_schema_by_name_rocks() {
        let rocksdb_ctx = RocksDBEngineBuildContext::default();
        test_maybe_create_schema_by_name(rocksdb_ctx).await;
    }

    async fn test_maybe_create_schema_by_name<T>(engine_context: T)
    where
        T: EngineBuildContext,
    {
        let env = TestEnv::builder().build();
        let mut test_ctx = env.new_context(engine_context);
        test_ctx.open().await;

        let catalog_manager = build_catalog_manager(test_ctx.clone_engine()).await;
        let catalog_name = catalog_manager.default_catalog_name();
        let catalog = catalog_manager.catalog_by_name(catalog_name);
        assert!(catalog.is_ok());
        assert!(catalog.as_ref().unwrap().is_some());

        let schema_name = "test";
        let catalog_ref = catalog.as_ref().unwrap().as_ref().unwrap();
        let mut schema = catalog_ref.schema_by_name(schema_name);
        assert!(schema.is_ok());
        assert!(schema.as_ref().unwrap().is_none());

        catalog_ref.create_schema(schema_name).await.unwrap();
        schema = catalog_ref.schema_by_name(schema_name);
        assert!(schema.is_ok());
        assert!(schema.as_ref().unwrap().is_some());
    }

    #[tokio::test]
    async fn test_create_table_rocks() {
        let rocksdb_ctx = RocksDBEngineBuildContext::default();
        test_create_table(rocksdb_ctx).await;
    }

    async fn test_create_table<T: EngineBuildContext>(engine_context: T) {
        let env = TestEnv::builder().build();
        let mut test_ctx = env.new_context(engine_context);
        test_ctx.open().await;

        let engine = test_ctx.engine().clone();
        let memory = MemoryTableEngine;
        let engine_proxy = Arc::new(TableEngineProxy {
            memory,
            analytic: engine.clone(),
        });

        let catalog_manager = build_catalog_manager(engine.clone()).await;
        let schema = build_default_schema_with_catalog(&catalog_manager).await;

        let table_name = "test";
        let request = build_create_table_req(table_name, schema.clone()).await;

        let opts = CreateOptions {
            table_engine: engine_proxy.clone(),
            create_if_not_exists: true,
        };

        schema
            .create_table(request.clone(), opts.clone())
            .await
            .unwrap();
        assert!(schema.table_by_name(table_name).unwrap().is_some());

        // create again
        schema.create_table(request.clone(), opts).await.unwrap();
        assert!(schema.table_by_name(table_name).unwrap().is_some());

        let opts2 = CreateOptions {
            table_engine: engine_proxy,
            create_if_not_exists: false,
        };
        assert!(schema.create_table(request.clone(), opts2).await.is_err());
    }

    #[tokio::test]
    async fn test_drop_table_rocks() {
        let rocksdb_ctx = RocksDBEngineBuildContext::default();
        test_drop_table(rocksdb_ctx).await;
    }

    async fn test_drop_table<T: EngineBuildContext>(engine_context: T) {
        let env = TestEnv::builder().build();
        let mut test_ctx = env.new_context(engine_context);
        test_ctx.open().await;

        let engine = test_ctx.engine().clone();
        let memory = MemoryTableEngine;
        let engine_proxy = Arc::new(TableEngineProxy {
            memory,
            analytic: engine.clone(),
        });

        let catalog_manager = build_catalog_manager(engine.clone()).await;
        let schema = build_default_schema_with_catalog(&catalog_manager).await;

        let table_name = "test";
        let engine_name = "test_engine";
        let drop_table_request = DropTableRequest {
            catalog_name: DEFAULT_CATALOG.to_string(),
            schema_name: schema.name().to_string(),
            schema_id: schema.id(),
            table_name: table_name.to_string(),
            engine: engine_name.to_string(),
        };
        let drop_table_opts = DropOptions {
            table_engine: engine_proxy.clone(),
        };

        assert!(!schema
            .drop_table(drop_table_request.clone(), drop_table_opts.clone())
            .await
            .unwrap());

        let create_table_request = build_create_table_req(table_name, schema.clone()).await;
        let create_table_opts = CreateOptions {
            table_engine: engine_proxy,
            create_if_not_exists: true,
        };

        // create table
        {
            schema
                .create_table(create_table_request.clone(), create_table_opts.clone())
                .await
                .unwrap();
            // check table exists
            assert!(schema.table_by_name(table_name).unwrap().is_some());
        }

        // drop table
        {
            assert!(schema
                .drop_table(drop_table_request.clone(), drop_table_opts.clone())
                .await
                .unwrap());
            // check table not exists
            assert!(schema.table_by_name(table_name).unwrap().is_none());
        }

        // create table again
        {
            schema
                .create_table(create_table_request.clone(), create_table_opts.clone())
                .await
                .unwrap();
            // check table exists
            assert!(schema.table_by_name(table_name).unwrap().is_some());
        }

        // drop table again
        {
            assert!(schema
                .drop_table(drop_table_request.clone(), drop_table_opts.clone())
                .await
                .unwrap());
            // check table not exists
            assert!(schema.table_by_name(table_name).unwrap().is_none());
        }

        // create two tables
        {
            let table_name2 = "test2";
            let create_table_request2 = build_create_table_req(table_name2, schema.clone()).await;
            schema
                .create_table(create_table_request2.clone(), create_table_opts.clone())
                .await
                .unwrap();
            // check table exists
            assert!(schema.table_by_name(table_name2).unwrap().is_some());

            schema
                .create_table(create_table_request, create_table_opts)
                .await
                .unwrap();
            // check table exists
            assert!(schema.table_by_name(table_name).unwrap().is_some());
        }

        // drop table again
        {
            assert!(schema
                .drop_table(drop_table_request, drop_table_opts)
                .await
                .unwrap());
            // check table not exists
            assert!(schema.table_by_name(table_name).unwrap().is_none());
        }
    }
}
