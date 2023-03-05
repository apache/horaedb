use influxdb_influxql_parser::statement::Statement as InfluxqlStatement;

use super::stmt_rewriter::StmtRewriter;
use crate::{influxql::error::*, plan::Plan, provider::MetaProvider};

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

    fn rewrite_stmt(&self, stmt: InfluxqlStatement) -> Result<InfluxqlStatement> {
        let mut stmt = stmt;
        let stmt_rewriter = StmtRewriter::new(&self.sql_planner);
        // stmt_rewriter.rewrite_from(&mut stmt)?;
        todo!()
    }
}
