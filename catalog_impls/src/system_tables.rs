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

//! Contains System tables, such as system.public.tables

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use catalog::{
    consts::{SYSTEM_CATALOG, SYSTEM_CATALOG_SCHEMA},
    schema::{
        CreateOptions, CreateTableRequest, DropOptions, DropTableRequest, NameRef, Schema,
        SchemaRef,
    },
    Catalog,
};
use log::warn;
use system_catalog::SystemTableAdapter;
use table_engine::{
    self,
    table::{SchemaId, Table, TableRef},
};

const UNSUPPORTED_MSG: &str = "system tables not supported";

pub struct SystemTablesBuilder {
    tables: HashMap<String, Arc<SystemTableAdapter>>,
}

impl SystemTablesBuilder {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn insert_table(mut self, table: SystemTableAdapter) -> Self {
        self.tables
            .insert(table.name().to_string(), Arc::new(table));
        self
    }

    pub fn build(self) -> SystemTables {
        SystemTables::new(self.tables)
    }
}

#[derive(Clone)]
pub struct SystemTables {
    tables: Arc<HashMap<String, Arc<SystemTableAdapter>>>,
}

impl SystemTables {
    pub fn new(tables: HashMap<String, Arc<SystemTableAdapter>>) -> Self {
        Self {
            tables: Arc::new(tables),
        }
    }
}

#[async_trait]
impl Schema for SystemTables {
    fn name(&self) -> NameRef {
        SYSTEM_CATALOG_SCHEMA
    }

    fn id(&self) -> SchemaId {
        system_catalog::SYSTEM_SCHEMA_ID
    }

    fn table_by_name(&self, name: NameRef) -> catalog::schema::Result<Option<TableRef>> {
        Ok(self.tables.get(name).map(|v| v.clone() as TableRef))
    }

    async fn create_table(
        &self,
        _request: CreateTableRequest,
        _opts: CreateOptions,
    ) -> catalog::schema::Result<TableRef> {
        catalog::schema::UnSupported {
            msg: UNSUPPORTED_MSG,
        }
        .fail()
    }

    async fn drop_table(
        &self,
        _request: DropTableRequest,
        _opts: DropOptions,
    ) -> catalog::schema::Result<bool> {
        catalog::schema::UnSupported {
            msg: UNSUPPORTED_MSG,
        }
        .fail()
    }

    fn all_tables(&self) -> catalog::schema::Result<Vec<TableRef>> {
        Ok(self
            .tables
            .iter()
            .map(|(_, v)| v.clone() as TableRef)
            .collect())
    }

    fn register_table(&self, _table: TableRef) {
        warn!("Try to register table in the system tables");
    }

    fn unregister_table(&self, _table_name: &str) {
        warn!("Try to unregister table in the system tables");
    }
}

#[async_trait]
impl Catalog for SystemTables {
    fn name(&self) -> NameRef {
        SYSTEM_CATALOG
    }

    fn schema_by_name(&self, name: NameRef) -> catalog::Result<Option<SchemaRef>> {
        if name == SYSTEM_CATALOG_SCHEMA {
            Ok(Some(Arc::new(self.clone())))
        } else {
            Ok(None)
        }
    }

    async fn create_schema<'a>(&'a self, _name: NameRef<'a>) -> catalog::Result<()> {
        catalog::UnSupported {
            msg: UNSUPPORTED_MSG,
        }
        .fail()
    }

    fn all_schemas(&self) -> catalog::Result<Vec<SchemaRef>> {
        catalog::UnSupported {
            msg: UNSUPPORTED_MSG,
        }
        .fail()
    }
}
