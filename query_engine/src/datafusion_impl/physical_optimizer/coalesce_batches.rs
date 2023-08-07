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

use std::sync::Arc;

use datafusion::{
    config::ConfigOptions,
    physical_optimizer::{coalesce_batches::CoalesceBatches, optimizer::PhysicalOptimizerRule},
    physical_plan::{limit::GlobalLimitExec, ExecutionPlan},
};

use crate::datafusion_impl::physical_optimizer::{Adapter, OptimizeRuleRef};

pub struct CoalesceBatchesAdapter {
    original_rule: OptimizeRuleRef,
}

impl Adapter for CoalesceBatchesAdapter {
    fn may_adapt(original_rule: OptimizeRuleRef) -> OptimizeRuleRef {
        if original_rule.name() == CoalesceBatches::default().name() {
            Arc::new(Self { original_rule })
        } else {
            original_rule
        }
    }
}

impl CoalesceBatchesAdapter {
    /// Detect the plan contains any limit plan with a small limit(smaller than
    /// `batch_size`).
    fn detect_small_limit_plan(plan: &dyn ExecutionPlan, batch_size: usize) -> bool {
        if let Some(limit_plan) = plan.as_any().downcast_ref::<GlobalLimitExec>() {
            return limit_plan.skip() + limit_plan.fetch().unwrap_or(0) < batch_size;
        }

        for child_plan in plan.children() {
            if Self::detect_small_limit_plan(&*child_plan, batch_size) {
                return true;
            }
        }

        // No small limit plan is found.
        false
    }
}

impl PhysicalOptimizerRule for CoalesceBatchesAdapter {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if Self::detect_small_limit_plan(&*plan, config.execution.batch_size) {
            Ok(plan)
        } else {
            self.original_rule.optimize(plan, config)
        }
    }

    fn name(&self) -> &str {
        "custom_coalesce_batches"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
