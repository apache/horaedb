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

use std::sync::Arc;

use catalog::{
    consts::SYSTEM_CATALOG,
    manager::{Manager, ManagerRef},
    schema::NameRef,
    CatalogRef,
};
use system_catalog::{tables::Tables, SystemTableAdapter};

use crate::system_tables::{SystemTables, SystemTablesBuilder};

mod system_tables;
pub mod table_based;
pub mod volatile;

/// CatalogManagerImpl is a wrapper for system and user tables
#[derive(Clone)]
pub struct CatalogManagerImpl {
    system_tables: SystemTables,
    user_catalog_manager: ManagerRef,
}

impl CatalogManagerImpl {
    pub fn new(manager: ManagerRef) -> Self {
        let mut system_tables_builder = SystemTablesBuilder::new();
        system_tables_builder = system_tables_builder
            .insert_table(SystemTableAdapter::new(Tables::new(manager.clone())));
        Self {
            system_tables: system_tables_builder.build(),
            user_catalog_manager: manager,
        }
    }
}

impl Manager for CatalogManagerImpl {
    fn default_catalog_name(&self) -> NameRef {
        self.user_catalog_manager.default_catalog_name()
    }

    fn default_schema_name(&self) -> NameRef {
        self.user_catalog_manager.default_schema_name()
    }

    fn catalog_by_name(&self, name: NameRef) -> catalog::manager::Result<Option<CatalogRef>> {
        match name {
            SYSTEM_CATALOG => Ok(Some(Arc::new(self.system_tables.clone()))),
            _ => self.user_catalog_manager.catalog_by_name(name),
        }
    }

    fn all_catalogs(&self) -> catalog::manager::Result<Vec<CatalogRef>> {
        self.user_catalog_manager.all_catalogs()
    }
}
