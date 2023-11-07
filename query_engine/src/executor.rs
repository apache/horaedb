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

//! Query executor

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use table_engine::stream::SendableRecordBatchStream;

use crate::{context::Context, error::*, physical_planner::PhysicalPlanRef};

/// Query executor
///
/// Executes the logical plan
#[async_trait]
pub trait Executor: fmt::Debug + Send + Sync + 'static {
    /// Execute the query, return the record batch stream
    ///
    /// REQUIRE: The meta data of tables in query should be found from
    /// ContextRef
    async fn execute(
        &self,
        ctx: &Context,
        physical_plan: PhysicalPlanRef,
    ) -> Result<SendableRecordBatchStream>;
}

pub type ExecutorRef = Arc<dyn Executor>;
