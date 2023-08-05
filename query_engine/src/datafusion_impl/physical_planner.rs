// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Instant};

use async_trait::async_trait;
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
        push_down_filter::PushDownFilter,
        push_down_limit::PushDownLimit,
        push_down_projection::PushDownProjection,
        simplify_expressions::SimplifyExpressions,
        single_distinct_to_groupby::SingleDistinctToGroupBy,
        OptimizerRule,
    },
    physical_optimizer::PhysicalOptimizerRule,
    prelude::{SessionConfig, SessionContext},
};
use generic_error::BoxError;
use query_frontend::{plan::QueryPlan, provider::CatalogProviderAdapter};
use snafu::ResultExt;
use table_engine::provider::CeresdbOptions;

use crate::{
    context::Context,
    datafusion_impl::{
        logical_optimizer::type_conversion::TypeConversion, physical_optimizer,
        physical_plan::DataFusionPhysicalPlanAdapter,
        physical_planner_extension::QueryPlannerAdapter,
    },
    error::*,
    physical_planner::{PhysicalPlanPtr, PhysicalPlanner},
    Config,
};

/// Physical query optimizer that converts a logical plan to a
/// physical plan suitable for execution
#[derive(Clone)]
pub struct PhysicalPlannerImpl {
    config: Config,
}

impl PhysicalPlannerImpl {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    pub fn build_df_session_ctx(&self, config: &Config, ctx: &Context) -> SessionContext {
        let timeout = ctx
            .deadline
            .map(|deadline| deadline.duration_since(Instant::now()).as_millis() as u64);
        let ceresdb_options = CeresdbOptions {
            request_id: ctx.request_id.as_u64(),
            request_timeout: timeout,
        };
        let mut df_session_config = SessionConfig::new()
            .with_default_catalog_and_schema(
                ctx.default_catalog.clone(),
                ctx.default_schema.clone(),
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

#[async_trait]
impl PhysicalPlanner for PhysicalPlannerImpl {
    async fn plan(&self, logical_plan: QueryPlan, ctx: &Context) -> Result<PhysicalPlanPtr> {
        // Register catalogs to datafusion execution context.
        let catalogs = CatalogProviderAdapter::new_adapters(logical_plan.tables.clone());
        let df_ctx = self.build_df_session_ctx(&self.config, ctx);
        for (name, catalog) in catalogs {
            df_ctx.register_catalog(&name, Arc::new(catalog));
        }

        // Generate physical plan.
        let exec_plan = df_ctx
            .state()
            .create_physical_plan(&logical_plan.df_plan)
            .await
            .box_err()
            .context(PhysicalPlannerWithCause { msg: None })?;
        let physical_plan = DataFusionPhysicalPlanAdapter::with_plan(df_ctx.clone(), exec_plan);

        Ok(Box::new(physical_plan))
    }
}
