// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Influxql planner

use std::sync::Arc;

use common_util::error::BoxError;
use influxql_logical_planner::planner::InfluxQLToLogicalPlan;
use influxql_parser::{
    common::{MeasurementName, QualifiedMeasurementName},
    select::{MeasurementSelection, SelectStatement},
    statement::Statement as InfluxqlStatement,
};
use snafu::{ensure, ResultExt};

use crate::{
    influxql::{error::*, provider::InfluxSchemaProviderImpl},
    plan::{Plan, QueryPlan},
    provider::{ContextProviderAdapter, MetaProvider},
};

pub const CERESDB_MEASUREMENT_COLUMN_NAME: &str = "ceresdb::measurement";

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
                msg: stmt.to_string(),
            }
            .fail(),
        }
    }

    pub fn select_to_plan(self, stmt: InfluxqlStatement) -> Result<Plan> {
        if let InfluxqlStatement::Select(select_stmt) = &stmt {
            check_select_statement(select_stmt)?;
        } else {
            unreachable!("select statement here has been ensured by caller");
        }

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
            .context(BuildPlanWithCause {
                msg: "build df plan for influxql select statement",
            })?;
        let tables = Arc::new(
            self.context_provider
                .try_into_container()
                .box_err()
                .context(BuildPlanWithCause {
                    msg: "get tables from df plan of select",
                })?,
        );

        Ok(Plan::Query(QueryPlan { df_plan, tables }))
    }
}

pub fn check_select_statement(select_stmt: &SelectStatement) -> Result<()> {
    // Only support from single measurements now.
    ensure!(
        !select_stmt.from.is_empty(),
        BuildPlanNoCause {
            msg: format!("invalid influxql select statement with empty from, stmt:{select_stmt}"),
        }
    );
    ensure!(
        select_stmt.from.len() == 1,
        Unimplemented {
            msg: format!("select from multiple measurements, stmt:{select_stmt}"),
        }
    );

    let from = &select_stmt.from[0];
    match from {
        MeasurementSelection::Name(name) => {
            let QualifiedMeasurementName { name, .. } = name;

            match name {
                MeasurementName::Regex(_) => Unimplemented {
                    msg: format!("select from regex, stmt:{select_stmt}"),
                }
                .fail(),
                MeasurementName::Name(_) => Ok(()),
            }
        }
        MeasurementSelection::Subquery(_) => Unimplemented {
            msg: format!("select from subquery, stmt:{select_stmt}"),
        }
        .fail(),
    }
}

#[cfg(test)]
mod test {
    use influxql_parser::{select::SelectStatement, statement::Statement};

    use super::check_select_statement;

    #[test]
    fn test_check_select_from() {
        let from_measurement = parse_select("select * from a;");
        let from_multi_measurements = parse_select("select * from a,b;");
        let from_regex = parse_select(r#"select * from /d/"#);
        let from_subquery = parse_select("select * from (select a,b from c)");

        let res = check_select_statement(&from_measurement);
        assert!(res.is_ok());

        let res = check_select_statement(&from_multi_measurements);
        let err = res.err().unwrap();
        assert!(err
            .to_string()
            .contains("select from multiple measurements"));

        let res = check_select_statement(&from_regex);
        let err = res.err().unwrap();
        assert!(err.to_string().contains("select from regex"));

        let res = check_select_statement(&from_subquery);
        let err = res.err().unwrap();
        assert!(err.to_string().contains("select from subquery"));
    }

    fn parse_select(influxql: &str) -> SelectStatement {
        let stmt = influxql_parser::parse_statements(influxql).unwrap()[0].clone();
        if let Statement::Select(select_stmt) = stmt {
            *select_stmt
        } else {
            unreachable!()
        }
    }
}
