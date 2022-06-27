// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Query context

use std::sync::Arc;

use arrow_deps::datafusion::{
    execution::context::default_session_builder,
    optimizer::{
        common_subexpr_eliminate::CommonSubexprEliminate, eliminate_limit::EliminateLimit,
        filter_push_down::FilterPushDown, limit_push_down::LimitPushDown, optimizer::OptimizerRule,
        projection_push_down::ProjectionPushDown, simplify_expressions::SimplifyExpressions,
    },
    physical_optimizer::optimizer::PhysicalOptimizerRule,
    prelude::{SessionConfig, SessionContext},
};
use common_types::request_id::RequestId;

use crate::{
    df_planner_extension::QueryPlannerAdapter,
    logical_optimizer::{
        order_by_primary_key::OrderByPrimaryKeyRule, type_conversion::TypeConversion,
    },
    physical_optimizer,
};

/// Query context
pub struct Context {
    request_id: RequestId,
    df_session_ctx: SessionContext,
}

impl Context {
    // For datafusion, internal use only
    #[inline]
    pub(crate) fn df_session_ctx(&self) -> &SessionContext {
        &self.df_session_ctx
    }

    #[inline]
    pub fn request_id(&self) -> RequestId {
        self.request_id
    }

    pub fn builder(request_id: RequestId) -> Builder {
        Builder {
            request_id,
            df_session_config: SessionConfig::new(),
        }
    }
}

pub type ContextRef = Arc<Context>;

#[must_use]
pub struct Builder {
    request_id: RequestId,
    df_session_config: SessionConfig,
}

impl Builder {
    /// Set default catalog and schema of this query context
    pub fn default_catalog_and_schema(mut self, catalog: String, schema: String) -> Self {
        self.df_session_config = self
            .df_session_config
            .with_default_catalog_and_schema(catalog, schema);

        self
    }

    pub fn build(self) -> Context {
        // Always create default catalog and schema now

        let logical_optimize_rules = Self::logical_optimize_rules();
        let mut state = default_session_builder(self.df_session_config)
            .with_query_planner(Arc::new(QueryPlannerAdapter))
            .with_optimizer_rules(logical_optimize_rules);
        let physical_optimizer =
            Self::apply_adapters_for_physical_optimize_rules(&state.physical_optimizers);
        state.physical_optimizers = physical_optimizer;
        let df_session_ctx = SessionContext::with_state(state);

        Context {
            request_id: self.request_id,
            df_session_ctx,
        }
    }

    fn apply_adapters_for_physical_optimize_rules(
        default_rules: &[Arc<dyn PhysicalOptimizerRule + Send + Sync>],
    ) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
        let mut new_rules = Vec::with_capacity(default_rules.len());
        for rule in default_rules {
            new_rules.push(physical_optimizer::may_adapt_optimize_rule(rule.clone()))
        }

        new_rules
    }

    fn logical_optimize_rules() -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
        let mut optimizers: Vec<Arc<dyn OptimizerRule + Send + Sync>> = vec![
            Arc::new(TypeConversion),
            // These rules are the default settings of the datafusion.
            Arc::new(SimplifyExpressions::new()),
            Arc::new(CommonSubexprEliminate::new()),
            Arc::new(EliminateLimit::new()),
            Arc::new(ProjectionPushDown::new()),
            Arc::new(FilterPushDown::new()),
            Arc::new(LimitPushDown::new()),
            // TODO: Re-enable this. Issue: https://github.com/CeresDB/ceresdb/issues/59
            // Arc::new(SingleDistinctToGroupBy::new()),
        ];

        // FIXME(xikai): use config to control the optimize rule.
        if std::env::var("ENABLE_CUSTOM_OPTIMIZE").is_ok() {
            optimizers.push(Arc::new(OrderByPrimaryKeyRule));
        }

        optimizers
    }
}
