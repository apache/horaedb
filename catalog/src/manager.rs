// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Catalog manager

use std::sync::Arc;

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
    fn default_catalog_name(&self) -> NameRef;

    /// Get the default schema name
    fn default_schema_name(&self) -> NameRef;

    /// Find the catalog by name
    fn catalog_by_name(&self, name: NameRef) -> Result<Option<CatalogRef>>;

    /// All catalogs
    fn all_catalogs(&self) -> Result<Vec<CatalogRef>>;
}

pub type ManagerRef = Arc<dyn Manager>;
