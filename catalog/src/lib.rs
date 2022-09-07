// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Common traits and types about catalog (schema)

#[macro_use]
extern crate common_util;

pub mod consts;
pub mod manager;
pub mod schema;

use std::sync::Arc;

use async_trait::async_trait;
use snafu::{Backtrace, Snafu};

use crate::schema::{NameRef, SchemaRef};

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(display(
        "Failed to create schema, catalog:{}, schema:{}, err:{}",
        catalog,
        schema,
        source
    ))]
    CreateSchema {
        catalog: String,
        schema: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unsupported method, msg:{}.\nBacktrace:\n{}", msg, backtrace))]
    UnSupported { msg: String, backtrace: Backtrace },
}

define_result!(Error);

/// Catalog manage schemas
// TODO(yingwen): Provide a context
// TODO(yingwen): Catalog id?
#[async_trait]
pub trait Catalog {
    /// Get the catalog name
    fn name(&self) -> NameRef;

    /// Find schema by name
    fn schema_by_name(&self, name: NameRef) -> Result<Option<SchemaRef>>;

    async fn create_schema<'a>(&'a self, name: NameRef<'a>) -> Result<()>;

    /// All schemas
    fn all_schemas(&self) -> Result<Vec<SchemaRef>>;
}

/// A reference counted catalog pointer
pub type CatalogRef = Arc<dyn Catalog + Send + Sync>;
