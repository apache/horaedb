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

//! Catalog manager

use std::sync::Arc;

use macros::define_result;
use snafu::Snafu;

use crate::{schema::NameRef, CatalogRef};

#[derive(Debug, Snafu)]
pub struct Error;

define_result!(Error);

/// Catalog manager abstraction
///
/// Tracks meta data of databases/tables
// TODO(yingwen): Maybe use async trait?
// TODO(yingwen): Provide a context

pub trait Manager: Send + Sync {
    /// Get the default catalog name
    ///
    /// Default catalog is ensured created because no method to create catalog
    /// is provided.
    fn default_catalog_name(&self) -> NameRef;

    /// Get the default schema name
    ///
    /// Default schema may be not created by the implementation and the caller
    /// may need to create that by itself.
    fn default_schema_name(&self) -> NameRef;

    /// Find the catalog by name
    fn catalog_by_name(&self, name: NameRef) -> Result<Option<CatalogRef>>;

    /// All catalogs
    fn all_catalogs(&self) -> Result<Vec<CatalogRef>>;
}

pub type ManagerRef = Arc<dyn Manager>;
