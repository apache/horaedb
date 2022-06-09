// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Query context

use std::sync::Arc;

use arrow_deps::datafusion::{
    execution::context::default_session_builder,
    optimizer::{
        common_subexpr_eliminate::CommonSubexprEliminate, eliminate_limit::EliminateLimit,
        filter_push_down::FilterPushDown, limit_push_down::LimitPushDown, optimizer::OptimizerRule,
        projection_push_down::ProjectionPushDown,
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
    df_exec_ctx: SessionContext,
}

impl Context {
    // For datafusion, internal use only
    #[inline]
    pub(crate) fn df_exec_ctx(&self) -> &SessionContext {
        &self.df_exec_ctx
    }

    #[inline]
    pub fn request_id(&self) -> RequestId {
        self.request_id
    }

    pub fn builder(request_id: RequestId) -> Builder {
        Builder {
            request_id,
            df_exec_config: SessionConfig::new(),
        }
    }
}

pub type ContextRef = Arc<Context>;

#[must_use]
pub struct Builder {
    request_id: RequestId,
    df_exec_config: SessionConfig,
}

impl Builder {
    /// Set default catalog and schema of this query context
    pub fn default_catalog_and_schema(mut self, catalog: String, schema: String) -> Self {
        self.df_exec_config = self
            .df_exec_config
            .with_default_catalog_and_schema(catalog, schema);

        self
    }

    pub fn build(self) -> Context {
        // Always create default catalog and schema now
        let df_exec_config = { self.df_exec_config };

        // todo: check this
        // let adapted_physical_optimize_rules =
        // Self::apply_adapters_for_physical_optimize_rules(
        //     &self.df_exec_config.physical_optimizers,
        // );
        let logical_optimize_rules = Self::logical_optimize_rules();

        let state = default_session_builder(df_exec_config)
            .with_query_planner(Arc::new(QueryPlannerAdapter))
            .with_optimizer_rules(logical_optimize_rules);
        // .with_physical_optimizer_rules(adapted_physical_optimize_rules);

        let context = SessionContext::with_state(state);

        Context {
            request_id: self.request_id,
            df_exec_ctx: context,
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

            // todo: enable this once  https://github.com/apache/arrow-datafusion/pull/2686 is available.
            // Arc::new(SimplifyExpressions::new()),
            Arc::new(CommonSubexprEliminate::new()),
            Arc::new(EliminateLimit::new()),
            Arc::new(ProjectionPushDown::new()),
            Arc::new(FilterPushDown::new()),
            Arc::new(LimitPushDown::new()),
            // TODO: restore this rule after the bug of df is fixed.
            // Arc::new(SingleDistinctToGroupBy::new()),
        ];

        // FIXME(xikai): use config to control the optimize rule.
        if std::env::var("ENABLE_CUSTOM_OPTIMIZE").is_ok() {
            optimizers.push(Arc::new(OrderByPrimaryKeyRule));
        }

        optimizers
    }
}
