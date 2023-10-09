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

//! Adapter for the original datafusion repartiton optimization rule.

use std::sync::Arc;

use datafusion::{
    config::ConfigOptions,
    physical_optimizer::{optimizer::PhysicalOptimizerRule, repartition::Repartition},
    physical_plan::ExecutionPlan,
};
use logger::debug;

use crate::datafusion_impl::physical_optimizer::{Adapter, OptimizeRuleRef};

pub struct RepartitionAdapter {
    original_rule: OptimizeRuleRef,
}

impl Adapter for RepartitionAdapter {
    fn may_adapt(original_rule: OptimizeRuleRef) -> OptimizeRuleRef {
        if original_rule.name() == Repartition::new().name() {
            Arc::new(Self { original_rule })
        } else {
            original_rule
        }
    }
}

impl PhysicalOptimizerRule for RepartitionAdapter {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // the underlying plan maybe requires the order of the output.
        if plan.output_partitioning().partition_count() == 1 {
            debug!(
                "RepartitionAdapter avoid repartition optimization for plan:{:?}",
                plan
            );
            Ok(plan)
        } else {
            self.original_rule.optimize(plan, config)
        }
    }

    fn name(&self) -> &str {
        "custom-repartition"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
