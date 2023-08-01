// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Query context

use std::{sync::Arc, time::Instant};

use common_types::request_id::RequestId;
use datafusion::{
    execution::{context::SessionState, runtime_env::RuntimeEnv},
    optimizer::{
        analyzer::{
            count_wildcard_rule::CountWildcardRule, inline_table_scan::InlineTableScan,
            AnalyzerRule,
        },
        common_subexpr_eliminate::CommonSubexprEliminate,
        eliminate_limit::EliminateLimit,
        optimizer::OptimizerRule,
        push_down_filter::PushDownFilter,
        push_down_limit::PushDownLimit,
        push_down_projection::PushDownProjection,
        simplify_expressions::SimplifyExpressions,
        single_distinct_to_groupby::SingleDistinctToGroupBy,
    },
    physical_optimizer::optimizer::PhysicalOptimizerRule,
    prelude::{SessionConfig, SessionContext},
};
use table_engine::provider::CeresdbOptions;

use crate::{
    config::Config, df_planner_extension::QueryPlannerAdapter,
    logical_optimizer::type_conversion::TypeConversion, physical_optimizer,
};

pub type ContextRef = Arc<Context>;

/// Query context
pub struct Context {
    pub request_id: RequestId,
    pub deadline: Option<Instant>,
    pub default_catalog: String,
    pub default_schema: String,
}

impl Context {
    pub fn build_df_session_ctx(
        &self,
        config: &Config,
        request_id: RequestId,
        deadline: Option<Instant>,
    ) -> SessionContext {
        let timeout =
            deadline.map(|deadline| deadline.duration_since(Instant::now()).as_millis() as u64);
        let ceresdb_options = CeresdbOptions {
            request_id: request_id.as_u64(),
            request_timeout: timeout,
        };
        let mut df_session_config = SessionConfig::new()
            .with_default_catalog_and_schema(
                self.default_catalog.clone(),
                self.default_schema.clone(),
            )
            .with_target_partitions(config.read_parallelism);

        df_session_config
            .options_mut()
            .extensions
            .insert(ceresdb_options);

        let logical_optimize_rules = Self::logical_optimize_rules();
        let state =
            SessionState::with_config_rt(df_session_config, Arc::new(RuntimeEnv::default()))
                .with_query_planner(Arc::new(QueryPlannerAdapter))
                .with_analyzer_rules(Self::analyzer_rules())
                .with_optimizer_rules(logical_optimize_rules);
        let state = influxql_query::logical_optimizer::register_iox_logical_optimizers(state);
        let physical_optimizer =
            Self::apply_adapters_for_physical_optimize_rules(state.physical_optimizers());
        SessionContext::with_state(state.with_physical_optimizer_rules(physical_optimizer))
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
        vec![
            // These rules are the default settings of the datafusion.
            Arc::new(SimplifyExpressions::new()),
            Arc::new(CommonSubexprEliminate::new()),
            Arc::new(EliminateLimit::new()),
            Arc::new(PushDownProjection::new()),
            Arc::new(PushDownFilter::new()),
            Arc::new(PushDownLimit::new()),
            Arc::new(SingleDistinctToGroupBy::new()),
        ]
    }

    fn analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
        vec![
            Arc::new(InlineTableScan::new()),
            Arc::new(TypeConversion),
            Arc::new(datafusion::optimizer::analyzer::type_coercion::TypeCoercion::new()),
            Arc::new(CountWildcardRule::new()),
        ]
    }
}
