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

//! Interpreter context

use std::{sync::Arc, time::Instant};

use common_types::request_id::RequestId;
use macros::define_result;
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
    /// If time range exceeds this threshold, the query will be marked as
    /// expensive
    expensive_query_threshold: u64,
}

impl Context {
    pub fn builder(request_id: RequestId, deadline: Option<Instant>) -> Builder {
        Builder {
            request_id,
            deadline,
            default_catalog: String::new(),
            default_schema: String::new(),
            enable_partition_table_access: false,
            expensive_query_threshold: 24 * 3600 * 1000, // default 24 hours
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

    #[inline]
    pub fn expensive_query_threshold(&self) -> u64 {
        self.expensive_query_threshold
    }
}

#[must_use]
pub struct Builder {
    request_id: RequestId,
    deadline: Option<Instant>,
    default_catalog: String,
    default_schema: String,
    enable_partition_table_access: bool,
    expensive_query_threshold: u64,
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

    pub fn expensive_query_threshold(mut self, threshold: u64) -> Self {
        self.expensive_query_threshold = threshold;
        self
    }

    pub fn build(self) -> Context {
        Context {
            request_id: self.request_id,
            deadline: self.deadline,
            default_catalog: self.default_catalog,
            default_schema: self.default_schema,
            enable_partition_table_access: self.enable_partition_table_access,
            expensive_query_threshold: self.expensive_query_threshold,
        }
    }
}
