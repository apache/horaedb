// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Catalog manager

use std::sync::Arc;

use snafu::Snafu;

use crate::{schema::NameRef, CatalogRef};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to do initialization, msg:{}, err:{}", msg, source))]
    Init {
        msg: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

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
