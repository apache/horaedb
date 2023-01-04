// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Server context

use std::sync::Arc;

use common_util::runtime::Runtime;
use snafu::{ensure, Backtrace, OptionExt, Snafu};

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing catalog.\nBacktrace:\n{}", backtrace))]
    MissingCatalog { backtrace: Backtrace },

    #[snafu(display("Missing tenant.\nBacktrace:\n{}", backtrace))]
    MissingTenant { backtrace: Backtrace },

    #[snafu(display("Missing runtime.\nBacktrace:\n{}", backtrace))]
    MissingRuntime { backtrace: Backtrace },
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
    /// Tenant of request
    pub tenant: String,
    /// Runtime of this request
    pub runtime: Arc<Runtime>,
    /// Admin flag, some operation only can be done by admin
    pub is_admin: bool,
}

impl RequestContext {
    pub fn builder() -> Builder {
        Builder::default()
    }
}

#[derive(Default)]
pub struct Builder {
    catalog: String,
    tenant: String,
    runtime: Option<Arc<Runtime>>,
    is_admin: bool,
}

impl Builder {
    pub fn catalog(mut self, catalog: String) -> Self {
        self.catalog = catalog;
        self
    }

    pub fn tenant(mut self, tenant: String) -> Self {
        self.tenant = tenant;
        self
    }

    pub fn runtime(mut self, runtime: Arc<Runtime>) -> Self {
        self.runtime = Some(runtime);
        self
    }

    pub fn is_admin(mut self, is_admin: bool) -> Self {
        self.is_admin = is_admin;
        self
    }

    pub fn build(self) -> Result<RequestContext> {
        ensure!(!self.catalog.is_empty(), MissingCatalog);
        // We use tenant as schema, so we use default schema if tenant is not specific
        ensure!(!self.tenant.is_empty(), MissingTenant);

        let runtime = self.runtime.context(MissingRuntime)?;

        Ok(RequestContext {
            catalog: self.catalog,
            tenant: self.tenant,
            runtime,
            is_admin: self.is_admin,
        })
    }
}
