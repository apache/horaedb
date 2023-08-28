// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use table_engine::table::{SchemaId, TableRef};

use crate::{
    manager::{Manager, ManagerRef},
    schema::{
        CreateOptions, CreateTableRequest, DropOptions, DropTableRequest, NameRef,
        Result as SchemaResult, Schema, SchemaRef,
    },
    Catalog, CatalogRef, Result,
};

/// Mock catalog builder
pub struct MockCatalogManagerBuilder {
    catalog: String,
    schema: String,
    tables: Vec<TableRef>,
}

impl MockCatalogManagerBuilder {
    pub fn new(catalog: String, schema: String, tables: Vec<TableRef>) -> Self {
        Self {
            catalog,
            schema,
            tables,
        }
    }

    pub fn build(self) -> ManagerRef {
        let schema = Arc::new(MockSchema {
            name: self.schema.clone(),
            tables: self
                .tables
                .into_iter()
                .map(|t| (t.name().to_string(), t))
                .collect(),
        });

        let catalog = Arc::new(MockCatalog {
            name: self.catalog.clone(),
            schemas: HashMap::from([(self.schema.clone(), schema as _)]),
        });

        Arc::new(MockCatalogManager {
            catalogs: HashMap::from([(self.catalog.clone(), catalog as _)]),
            default_catalog: self.catalog,
            default_schema: self.schema,
        })
    }
}

/// Mock catalog manager which only support default catalog and schema
///
/// You can set the default catalog and schema when initializing.
struct MockCatalogManager {
    catalogs: HashMap<String, CatalogRef>,
    default_catalog: String,
    default_schema: String,
}

impl Manager for MockCatalogManager {
    fn default_catalog_name(&self) -> crate::schema::NameRef {
        &self.default_catalog
    }

    fn default_schema_name(&self) -> crate::schema::NameRef {
        &self.default_schema
    }

    fn catalog_by_name(
        &self,
        name: crate::schema::NameRef,
    ) -> crate::manager::Result<Option<CatalogRef>> {
        Ok(self.catalogs.get(name).cloned())
    }

    fn all_catalogs(&self) -> crate::manager::Result<Vec<CatalogRef>> {
        Ok(self.catalogs.clone().into_values().collect())
    }
}

struct MockCatalog {
    name: String,
    schemas: HashMap<String, SchemaRef>,
}

#[async_trait::async_trait]
impl Catalog for MockCatalog {
    fn name(&self) -> NameRef {
        &self.name
    }

    fn schema_by_name(&self, name: NameRef) -> Result<Option<SchemaRef>> {
        Ok(self.schemas.get(name).cloned())
    }

    async fn create_schema<'a>(&'a self, _name: NameRef<'a>) -> Result<()> {
        unimplemented!()
    }

    /// All schemas
    fn all_schemas(&self) -> Result<Vec<SchemaRef>> {
        Ok(self.schemas.clone().into_values().collect())
    }
}

struct MockSchema {
    name: String,
    tables: HashMap<String, TableRef>,
}

#[async_trait]
impl Schema for MockSchema {
    fn name(&self) -> NameRef {
        &self.name
    }

    fn id(&self) -> SchemaId {
        SchemaId::from_u32(42)
    }

    fn table_by_name(&self, name: NameRef) -> SchemaResult<Option<TableRef>> {
        Ok(self.tables.get(name).cloned())
    }

    async fn create_table(
        &self,
        _request: CreateTableRequest,
        _opts: CreateOptions,
    ) -> SchemaResult<TableRef> {
        unimplemented!()
    }

    async fn drop_table(
        &self,
        _request: DropTableRequest,
        _opts: DropOptions,
    ) -> SchemaResult<bool> {
        unimplemented!()
    }

    fn all_tables(&self) -> SchemaResult<Vec<TableRef>> {
        Ok(self.tables.clone().into_values().collect())
    }

    fn register_table(&self, _table: TableRef) {
        unimplemented!()
    }

    fn unregister_table(&self, _table_name: &str) {
        unimplemented!()
    }
}
