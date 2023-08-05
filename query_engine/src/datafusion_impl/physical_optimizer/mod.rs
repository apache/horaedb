// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Physical query optimizer

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    error::DataFusionError, physical_optimizer::optimizer::PhysicalOptimizerRule,
    prelude::SessionContext,
};
use macros::define_result;
use query_frontend::plan::QueryPlan;
use snafu::{Backtrace, ResultExt, Snafu};

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
