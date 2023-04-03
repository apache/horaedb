// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Frontend

use std::{sync::Arc, time::Instant};

use catalog::manager::{Manager, ManagerRef};
use ceresdbproto::{prometheus::Expr as PromExpr, storage::WriteTableRequest};
use cluster::config::SchemaConfig;
use common_types::request_id::RequestId;
use influxql_parser::statement::Statement as InfluxqlStatement;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use table_engine::table;

use crate::{
    ast::Statement,
    parser::Parser,
    plan::Plan,
    planner::Planner,
    promql::{ColumnNames, Expr},
    provider::MetaProvider,
};

#[derive(Debug, Snafu)]
pub enum Error {
    // Invalid sql is quite common, so we don't provide a backtrace now.
    #[snafu(display("Invalid sql, sql:{}, err:{}", sql, source))]
    InvalidSql {
        sql: String,
        source: sqlparser::parser::ParserError,
    },

    // TODO(yingwen): Should we store stmt here?
    #[snafu(display("Failed to create plan, err:{}", source))]
    CreatePlan { source: crate::planner::Error },

    #[snafu(display("Invalid prom request, err:{}", source))]
    InvalidPromRequest { source: crate::promql::Error },

    #[snafu(display("Expr is not found in prom request.\nBacktrace:\n{}", backtrace))]
    ExprNotFoundInPromRequest { backtrace: Backtrace },

    // invalid sql is quite common, so we don't provide a backtrace now.
    #[snafu(display("invalid influxql, influxql:{}, err:{}", influxql, parse_err))]
    InvalidInfluxql {
        influxql: String,
        parse_err: influxql_parser::common::ParseError,
    },
}

define_result!(Error);

pub type StatementVec = Vec<Statement>;

/// Context used by Frontend
///
/// We can collect metrics and trace info in it instead of using global
/// metrics or trace collector.
pub struct Context {
    /// Id of the query request.
    pub request_id: RequestId,
    /// Parallelism to read table.
    pub read_parallelism: usize,
    /// Deadline of this request
    pub deadline: Option<Instant>,
}

impl Context {
    pub fn new(request_id: RequestId, deadline: Option<Instant>) -> Self {
        Self {
            request_id,
            deadline,
            read_parallelism: table::DEFAULT_READ_PARALLELISM,
        }
    }
}

/// SQL frontend implementation
///
/// Thought the parser supports using multiple statements in a sql, but
/// this frontend only support planning one statement at a time now
#[derive(Debug)]
pub struct Frontend<P> {
    provider: P,
}

impl<P> Frontend<P> {
    pub fn new(provider: P) -> Self {
        Self { provider }
    }

    /// Parse the sql and returns the statements
    pub fn parse_sql(&self, _ctx: &mut Context, sql: &str) -> Result<StatementVec> {
        Parser::parse_sql(sql).context(InvalidSql { sql })
    }

    /// Parse the request and returns the Expr
    pub fn parse_promql(&self, _ctx: &mut Context, expr: Option<PromExpr>) -> Result<Expr> {
        let expr = expr.context(ExprNotFoundInPromRequest)?;
        Expr::try_from(expr).context(InvalidPromRequest)
    }

    /// Parse the sql and returns the statements
    pub fn parse_influxql(
        &self,
        _ctx: &mut Context,
        influxql: &str,
    ) -> Result<Vec<InfluxqlStatement>> {
        match influxql_parser::parse_statements(influxql) {
            Ok(stmts) => Ok(stmts),
            Err(e) => Err(Error::InvalidInfluxql {
                influxql: influxql.to_string(),
                parse_err: e,
            }),
        }
    }
}

impl<P: MetaProvider> Frontend<P> {
    /// Create logical plan for the statement
    pub fn statement_to_plan(&self, ctx: &mut Context, stmt: Statement) -> Result<Plan> {
        let planner = Planner::new(&self.provider, ctx.request_id, ctx.read_parallelism);

        planner.statement_to_plan(stmt).context(CreatePlan)
    }

    pub fn promql_expr_to_plan(
        &self,
        ctx: &mut Context,
        expr: Expr,
    ) -> Result<(Plan, Arc<ColumnNames>)> {
        let planner = Planner::new(&self.provider, ctx.request_id, ctx.read_parallelism);

        planner.promql_expr_to_plan(expr).context(CreatePlan)
    }

    pub fn influxql_stmt_to_plan(
        &self,
        ctx: &mut Context,
        stmt: InfluxqlStatement,
        manager: ManagerRef,
    ) -> Result<Plan> {
        let planner = Planner::new(&self.provider, ctx.request_id, ctx.read_parallelism);
        let catalog = manager
            .catalog_by_name(self.provider.default_catalog_name())
            .unwrap();
        let catalog = catalog.unwrap();
        let schema = catalog
            .schema_by_name(self.provider.default_schema_name())
            .unwrap()
            .unwrap();
        let all_tables = schema.all_tables().unwrap();

        planner
            .influxql_stmt_to_plan(stmt, all_tables)
            .context(CreatePlan)
    }

    pub fn write_req_to_plan(
        &self,
        ctx: &mut Context,
        schema_config: &SchemaConfig,
        write_table: &WriteTableRequest,
    ) -> Result<Plan> {
        let planner = Planner::new(&self.provider, ctx.request_id, ctx.read_parallelism);

        planner
            .write_req_to_plan(schema_config, write_table)
            .context(CreatePlan)
    }
}
