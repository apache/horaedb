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

use datafusion::physical_optimizer::optimizer::PhysicalOptimizerRule;

use crate::datafusion_impl::physical_optimizer::{
    coalesce_batches::CoalesceBatchesAdapter, repartition::RepartitionAdapter,
};

pub mod coalesce_batches;
pub mod repartition;

pub type OptimizeRuleRef = Arc<dyn PhysicalOptimizerRule + Send + Sync>;

/// The default optimize rules of the datafusion is not all suitable for our
/// cases so the adapters may change the default rules(normally just decide
/// whether to apply the rule according to the specific plan).
pub trait Adapter {
    /// May change the original rule into the custom one.
    fn may_adapt(original_rule: OptimizeRuleRef) -> OptimizeRuleRef;
}

pub fn may_adapt_optimize_rule(
    original_rule: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
) -> Arc<dyn PhysicalOptimizerRule + Send + Sync> {
    CoalesceBatchesAdapter::may_adapt(RepartitionAdapter::may_adapt(original_rule))
}
