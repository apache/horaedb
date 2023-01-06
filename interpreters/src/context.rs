// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Interpreter context

use std::sync::Arc;

use common_types::request_id::RequestId;
use query_engine::context::{Context as QueryContext, ContextRef as QueryContextRef};
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {}

define_result!(Error);

/// Interpreter context
///
/// Contains information that all interpreters need
#[derive(Debug, Clone)]
pub struct Context {
    request_id: RequestId,
    default_catalog: String,
    default_schema: String,
    admin: bool,
}

impl Context {
    pub fn builder(request_id: RequestId) -> Builder {
        Builder {
            request_id,
            default_catalog: String::new(),
            default_schema: String::new(),
            admin: false,
        }
    }

    /// Create a new context of query executor
    pub fn new_query_context(&self) -> Result<QueryContextRef> {
        let ctx = QueryContext {
            request_id: self.request_id,
            default_catalog: self.default_catalog.clone(),
            default_schema: self.default_schema.clone(),
        };
        Ok(Arc::new(ctx))
    }

    #[inline]
    pub fn default_catalog(&self) -> &str {
        &self.default_catalog
    }

    #[inline]
    pub fn default_schema(&self) -> &str {
        &self.default_schema
    }

    #[inline]
    pub fn request_id(&self) -> RequestId {
        self.request_id
    }

    #[inline]
    pub fn admin(&self) -> bool {
        self.admin
    }
}

#[must_use]
pub struct Builder {
    request_id: RequestId,
    default_catalog: String,
    default_schema: String,
    admin: bool,
}

impl Builder {
    pub fn default_catalog_and_schema(mut self, catalog: String, schema: String) -> Self {
        self.default_catalog = catalog;
        self.default_schema = schema;
        self
    }

    pub fn admin(mut self, admin: bool) -> Self {
        self.admin = admin;
        self
    }

    pub fn build(self) -> Context {
        Context {
            request_id: self.request_id,
            default_catalog: self.default_catalog,
            default_schema: self.default_schema,
            admin: self.admin,
        }
    }
}
