// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! InfluxQL planner

use std::{collections::HashMap, sync::Arc};

use common_util::error::BoxError;
use datafusion::{error::DataFusionError, sql::planner::ContextProvider};
use datafusion_expr::TableSource;
use influxql_logical_planner::plan::{
    ceresdb_schema_to_influxdb, InfluxQLToLogicalPlan, SchemaProvider,
};
use influxql_parser::{
    common::{MeasurementName, QualifiedMeasurementName},
    select::{MeasurementSelection, SelectStatement},
    show_measurements::ShowMeasurementsStatement,
    statement::Statement as InfluxqlStatement,
};
use influxql_schema::Schema;
use snafu::{ensure, ResultExt};
use table_engine::table::TableRef;

use crate::{
    influxql::error::*,
    plan::{Plan, QueryPlan, QueryType, ShowPlan, ShowTablesPlan},
    provider::{ContextProviderAdapter, MetaProvider},
};

// Same with iox
pub const CERESDB_MEASUREMENT_COLUMN_NAME: &str = "iox::measurement";

struct InfluxQLSchemaProvider<'a, P: MetaProvider> {
    context_provider: ContextProviderAdapter<'a, P>,
    tables: HashMap<String, (Arc<dyn TableSource>, Schema)>,
}

impl<'a, P: MetaProvider> SchemaProvider for InfluxQLSchemaProvider<'a, P> {
    fn get_table_provider(&self, name: &str) -> datafusion::error::Result<Arc<dyn TableSource>> {
        self.tables
            .get(name)
            .map(|(t, _)| Arc::clone(t))
            .ok_or_else(|| DataFusionError::Plan(format!("measurement does not exist: {name}")))
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<datafusion_expr::ScalarUDF>> {
        self.context_provider.get_function_meta(name)
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<datafusion_expr::AggregateUDF>> {
        self.context_provider.get_aggregate_meta(name)
    }

    fn table_names(&self) -> Vec<&'_ str> {
        self.tables.keys().map(|k| k.as_str()).collect::<Vec<_>>()
    }

    fn table_schema(&self, name: &str) -> Option<Schema> {
        self.tables.get(name).map(|(_, s)| s.clone())
    }
}

pub(crate) struct Planner<'a, P: MetaProvider> {
    schema_provider: InfluxQLSchemaProvider<'a, P>,
}

fn convert_influxql_schema(ceresdb_schema: common_types::schema::Schema) -> Result<Schema> {
    let arrow_schema = ceresdb_schema.into_arrow_schema_ref();
    ceresdb_schema_to_influxdb(arrow_schema)
        .box_err()
        .and_then(|s| Schema::try_from(s).box_err())
        .context(BuildPlanWithCause {
            msg: "build influxql schema",
        })
}

impl<'a, P: MetaProvider> Planner<'a, P> {
    pub fn try_new(
        context_provider: ContextProviderAdapter<'a, P>,
        all_tables: Vec<TableRef>,
    ) -> Result<Self> {
        let tables = all_tables
            .into_iter()
            .map(|t| {
                let table_name = t.name().to_string();
                let schema = convert_influxql_schema(t.schema())?;
                let table_source = context_provider.table_source(t);
                Ok((table_name, (table_source, schema)))
            })
            .collect::<Result<HashMap<_, _>>>()?;

        Ok(Self {
            schema_provider: InfluxQLSchemaProvider {
                context_provider,
                tables,
            },
        })
    }

    /// Build sql logical plan from [InfluxqlStatement].
    ///
    /// NOTICE: when building plan from influxql select statement,
    /// the [InfluxqlStatement] will be converted to [SqlStatement] first,
    /// and build plan then.
    pub fn statement_to_plan(self, stmt: InfluxqlStatement) -> Result<Plan> {
        match stmt {
            // TODO: show measurement is a temp workaround, it should be implemented in influxql
            // crates.
            InfluxqlStatement::ShowMeasurements(stmt) => self.show_measurements_to_plan(*stmt),
            _ => {
                let planner = InfluxQLToLogicalPlan::new(&self.schema_provider);
                let df_plan =
                    planner
                        .statement_to_plan(stmt)
                        .box_err()
                        .context(BuildPlanWithCause {
                            msg: "planner stmt to plan",
                        })?;
                let tables = Arc::new(
                    self.schema_provider
                        .context_provider
                        .try_into_container()
                        .box_err()
                        .context(BuildPlanWithCause {
                            msg: "get tables from context_provider",
                        })?,
                );
                Ok(Plan::Query(QueryPlan { df_plan, tables }))
            }
        }
    }

    // TODO: support offset/limit/match in stmt
    fn show_measurements_to_plan(self, _stmt: ShowMeasurementsStatement) -> Result<Plan> {
        let plan = ShowTablesPlan {
            pattern: None,
            query_type: QueryType::InfluxQL,
        };
        Ok(Plan::Show(ShowPlan::ShowTablesPlan(plan)))
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
