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

//! Query context

use std::{sync::Arc, time::Instant};

use common_types::request_id::RequestId;
use runtime::Priority;

pub type ContextRef = Arc<Context>;

/// Query context
#[derive(Debug, Clone)]
pub struct Context {
    pub request_id: RequestId,
    pub deadline: Option<Instant>,
    pub default_catalog: String,
    pub default_schema: String,
    pub priority: Priority,
}
