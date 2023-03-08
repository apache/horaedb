// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Influxql planner.

use common_util::error::{BoxError, GenericResult};
use influxdb_influxql_parser::{
    select::SelectStatement, statement::Statement as InfluxqlStatement,
};
use snafu::ResultExt;
use sqlparser::ast::Statement as SqlStatement;
use table_engine::table::TableRef;

use super::select::{converter::Converter, rewriter::Rewriter};
use crate::{
    plan::Plan,
    planner::{BuildInfluxqlPlan, Result},
    provider::MetaProvider,
};

#[allow(dead_code)]
pub(crate) struct Planner<'a, P: MetaProvider> {
    sql_planner: crate::planner::PlannerDelegate<'a, P>,
}

impl<'a, P: MetaProvider> Planner<'a, P> {
    pub fn new(sql_planner: crate::planner::PlannerDelegate<'a, P>) -> Self {
        Self { sql_planner }
    }

    pub fn statement_to_plan(self, stmt: InfluxqlStatement) -> Result<Plan> {
        match stmt {
            InfluxqlStatement::Select(stmt) => self.select_to_plan(*stmt),
            InfluxqlStatement::CreateDatabase(_) => todo!(),
            InfluxqlStatement::ShowDatabases(_) => todo!(),
            InfluxqlStatement::ShowRetentionPolicies(_) => todo!(),
            InfluxqlStatement::ShowTagKeys(_) => todo!(),
            InfluxqlStatement::ShowTagValues(_) => todo!(),
            InfluxqlStatement::ShowFieldKeys(_) => todo!(),
            InfluxqlStatement::ShowMeasurements(_) => todo!(),
            InfluxqlStatement::Delete(_) => todo!(),
            InfluxqlStatement::DropMeasurement(_) => todo!(),
            InfluxqlStatement::Explain(_) => todo!(),
        }
    }

    pub fn select_to_plan(self, stmt: SelectStatement) -> Result<Plan> {
        let mut stmt = stmt;
        let provider_impl = MeasurementProviderImpl(&self.sql_planner);
        let rewriter = Rewriter::new(&provider_impl);
        rewriter
            .rewrite(&mut stmt)
            .box_err()
            .context(BuildInfluxqlPlan)?;

        let sql_stmt = SqlStatement::Query(Box::new(
            Converter::convert(stmt)
                .box_err()
                .context(BuildInfluxqlPlan)?,
        ));

        self.sql_planner
            .sql_statement_to_plan(sql_stmt)
            .box_err()
            .context(BuildInfluxqlPlan)
    }
}

pub trait MeasurementProvider {
    fn measurement(&self, measurement_name: &str) -> GenericResult<Option<TableRef>>;
}

struct MeasurementProviderImpl<'a, P: MetaProvider>(&'a crate::planner::PlannerDelegate<'a, P>);

impl<'a, P: MetaProvider> MeasurementProvider for MeasurementProviderImpl<'a, P> {
    fn measurement(&self, measurement_name: &str) -> GenericResult<Option<TableRef>> {
        self.0.find_table(measurement_name).box_err()
    }
}
