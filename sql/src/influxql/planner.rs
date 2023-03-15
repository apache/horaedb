// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Influxql planner

use std::sync::Arc;

use common_util::error::BoxError;
use influxql_logical_planner::planner::InfluxQLToLogicalPlan;
use influxql_parser::statement::Statement as InfluxqlStatement;
use snafu::ResultExt;

use super::provider::InfluxSchemaProviderImpl;
use crate::{
    influxql::error::*,
    plan::{Plan, QueryPlan},
    provider::{ContextProviderAdapter, MetaProvider},
};

const CERESDB_MEASUREMENT_COLUMN_NAME: &str = "ceresdb::measurement";

pub(crate) struct Planner<'a, P: MetaProvider> {
    context_provider: ContextProviderAdapter<'a, P>,
}

impl<'a, P: MetaProvider> Planner<'a, P> {
    pub fn new(context_provider: ContextProviderAdapter<'a, P>) -> Self {
        Self { context_provider }
    }

    /// Build sql logical plan from [InfluxqlStatement].
    ///
    /// NOTICE: when building plan from influxql select statement,
    /// the [InfluxqlStatement] will be converted to [SqlStatement] first,
    /// and build plan then.
    pub fn statement_to_plan(self, stmt: InfluxqlStatement) -> Result<Plan> {
        match &stmt {
            InfluxqlStatement::Select(_) => self.select_to_plan(stmt),
            InfluxqlStatement::CreateDatabase(_)
            | InfluxqlStatement::ShowDatabases(_)
            | InfluxqlStatement::ShowRetentionPolicies(_)
            | InfluxqlStatement::ShowTagKeys(_)
            | InfluxqlStatement::ShowTagValues(_)
            | InfluxqlStatement::ShowFieldKeys(_)
            | InfluxqlStatement::ShowMeasurements(_)
            | InfluxqlStatement::Delete(_)
            | InfluxqlStatement::DropMeasurement(_)
            | InfluxqlStatement::Explain(_) => Unimplemented {
                stmt: stmt.to_string(),
            }
            .fail(),
        }
    }

    pub fn select_to_plan(self, stmt: InfluxqlStatement) -> Result<Plan> {
        let influx_schema_provider = InfluxSchemaProviderImpl {
            context_provider: &self.context_provider,
        };
        let influxql_logical_planner = InfluxQLToLogicalPlan::new(
            &influx_schema_provider,
            CERESDB_MEASUREMENT_COLUMN_NAME.to_string(),
        );

        let df_plan = influxql_logical_planner
            .statement_to_plan(stmt)
            .box_err()
            .context(BuildPlan {
                msg: "build df plan for influxql select statement",
            })?;
        let tables = Arc::new(
            self.context_provider
                .try_into_container()
                .box_err()
                .context(BuildPlan {
                    msg: "get tables from df plan of select",
                })?,
        );

        Ok(Plan::Query(QueryPlan { df_plan, tables }))
    }
}
