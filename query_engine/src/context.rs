// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Query context

use std::{sync::Arc, time::Instant};

use common_types::request_id::RequestId;

pub type ContextRef = Arc<Context>;

/// Query context
pub struct Context {
    pub request_id: RequestId,
    pub deadline: Option<Instant>,
    pub default_catalog: String,
    pub default_schema: String,
}
