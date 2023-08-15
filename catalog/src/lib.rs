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

//! Common traits and types about catalog (schema)

pub mod consts;
pub mod manager;
pub mod schema;
pub mod table_operator;
#[cfg(feature = "test")]
pub mod test_util;

use std::sync::Arc;

use async_trait::async_trait;
use generic_error::GenericError;
use macros::define_result;
use snafu::{Backtrace, Snafu};

use crate::schema::{NameRef, SchemaRef};

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(display(
        "Failed to create schema, catalog:{}, schema:{}, msg:{}.\nBacktrace:\nbacktrace:{}",
        catalog,
        schema,
        msg,
        backtrace,
    ))]
    CreateSchema {
        catalog: String,
        schema: String,
        msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to create schema, catalog:{}, schema:{}, err:{}",
        catalog,
        schema,
        source
    ))]
    CreateSchemaWithCause {
        catalog: String,
        schema: String,
        source: GenericError,
    },

    #[snafu(display("Unsupported method, msg:{}.\nBacktrace:\n{}", msg, backtrace))]
    UnSupported { msg: String, backtrace: Backtrace },

    #[snafu(display("Failed to operate table, msg:{:?}, err:{}", msg, source))]
    TableOperatorWithCause {
        msg: Option<String>,
        source: GenericError,
    },

    // Fixme: Temporarily remove the stack information, otherwise you will encounter a
    // segmentation fault.
    #[snafu(display("Failed to operate table, msg:{:?}.\n", msg))]
    TableOperatorNoCause { msg: Option<String> },
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
