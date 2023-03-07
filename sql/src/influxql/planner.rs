// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Influxql planner.

use common_util::error::BoxError;
use influxdb_influxql_parser::statement::Statement as InfluxqlStatement;
use snafu::ResultExt;
use table_engine::table::TableRef;

use crate::{influxql::error::*, plan::Plan, provider::MetaProvider};

#[allow(dead_code)]
pub(crate) struct Planner<'a, P: MetaProvider> {
    sql_planner: crate::planner::PlannerDelegate<'a, P>,
}

impl<'a, P: MetaProvider> Planner<'a, P> {
    pub fn new(sql_planner: crate::planner::PlannerDelegate<'a, P>) -> Self {
        Self { sql_planner }
    }

    pub fn statement_to_plan(&self, stmt: InfluxqlStatement) -> Result<Plan> {
        match stmt {
            InfluxqlStatement::Select(_) => todo!(),
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
}

pub trait MeasurementProvider {
    fn measurement(&self, measurement_name: &str) -> Result<Option<TableRef>>;
}

pub(crate) struct MeasurementProviderImpl<'a, P: MetaProvider>(
    crate::planner::PlannerDelegate<'a, P>,
);

impl<'a, P: MetaProvider> MeasurementProvider for MeasurementProviderImpl<'a, P> {
    fn measurement(&self, measurement_name: &str) -> Result<Option<TableRef>> {
        self.0
            .find_table(measurement_name)
            .box_err()
            .context(RewriteWithCause {
                msg: format!("failed to find measurement, measurement:{measurement_name}"),
            })
    }
}
