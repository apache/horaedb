// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Server context

use std::{sync::Arc, time::Duration};

use common_util::runtime::Runtime;
use router::Router;
use snafu::{ensure, Backtrace, OptionExt, Snafu};

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing catalog.\nBacktrace:\n{}", backtrace))]
    MissingCatalog { backtrace: Backtrace },

    #[snafu(display("Missing schema.\nBacktrace:\n{}", backtrace))]
    MissingSchema { backtrace: Backtrace },

    #[snafu(display("Missing runtime.\nBacktrace:\n{}", backtrace))]
    MissingRuntime { backtrace: Backtrace },

    #[snafu(display("Missing router.\nBacktrace:\n{}", backtrace))]
    MissingRouter { backtrace: Backtrace },
}

define_result!(Error);

/// Server request context
///
/// Context for request, may contains
/// 1. Request context and options
/// 2. Info from http headers
pub struct RequestContext {
    /// Catalog of the request
    pub catalog: String,
    /// Schema of request
    pub schema: String,
    /// Runtime of this request
    pub runtime: Arc<Runtime>,
    /// Enable partition table_access flag
    pub enable_partition_table_access: bool,
    /// Request timeout
    pub timeout: Option<Duration>,
    /// router
    pub router: Arc<dyn Router + Send + Sync>,
}

impl RequestContext {
    pub fn builder() -> Builder {
        Builder::default()
    }
}

#[derive(Default)]
pub struct Builder {
    catalog: String,
    schema: String,
    runtime: Option<Arc<Runtime>>,
    enable_partition_table_access: bool,
    timeout: Option<Duration>,
    router: Option<Arc<dyn Router + Send + Sync>>,
}

impl Builder {
    pub fn catalog(mut self, catalog: String) -> Self {
        self.catalog = catalog;
        self
    }

    pub fn schema(mut self, schema: String) -> Self {
        self.schema = schema;
        self
    }

    pub fn runtime(mut self, runtime: Arc<Runtime>) -> Self {
        self.runtime = Some(runtime);
        self
    }

    pub fn enable_partition_table_access(mut self, enable_partition_table_access: bool) -> Self {
        self.enable_partition_table_access = enable_partition_table_access;
        self
    }

    pub fn timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn router(mut self, router: Arc<dyn Router + Send + Sync>) -> Self {
        self.router = Some(router);
        self
    }

    pub fn build(self) -> Result<RequestContext> {
        ensure!(!self.catalog.is_empty(), MissingCatalog);
        ensure!(!self.schema.is_empty(), MissingSchema);

        let runtime = self.runtime.context(MissingRuntime)?;
        let router = self.router.context(MissingRouter)?;

        Ok(RequestContext {
            catalog: self.catalog,
            schema: self.schema,
            runtime,
            enable_partition_table_access: self.enable_partition_table_access,
            timeout: self.timeout,
            router,
        })
    }
}
