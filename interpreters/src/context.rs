// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Interpreter context

use std::{sync::Arc, time::Instant};

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
    deadline: Option<Instant>,
    default_catalog: String,
    default_schema: String,
    enable_partition_table_access: bool,
}

impl Context {
    pub fn builder(request_id: RequestId, deadline: Option<Instant>) -> Builder {
        Builder {
            request_id,
            deadline,
            default_catalog: String::new(),
            default_schema: String::new(),
            enable_partition_table_access: false,
        }
    }

    /// Create a new context of query executor
    pub fn new_query_context(&self) -> Result<QueryContextRef> {
        let ctx = QueryContext {
            request_id: self.request_id,
            deadline: self.deadline,
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
    pub fn enable_partition_table_access(&self) -> bool {
        self.enable_partition_table_access
    }
}

#[must_use]
pub struct Builder {
    request_id: RequestId,
    deadline: Option<Instant>,
    default_catalog: String,
    default_schema: String,
    enable_partition_table_access: bool,
}

impl Builder {
    pub fn default_catalog_and_schema(mut self, catalog: String, schema: String) -> Self {
        self.default_catalog = catalog;
        self.default_schema = schema;
        self
    }

    pub fn enable_partition_table_access(mut self, enable_partition_table_access: bool) -> Self {
        self.enable_partition_table_access = enable_partition_table_access;
        self
    }

    pub fn build(self) -> Context {
        Context {
            request_id: self.request_id,
            deadline: self.deadline,
            default_catalog: self.default_catalog,
            default_schema: self.default_schema,
            enable_partition_table_access: self.enable_partition_table_access,
        }
    }
}
