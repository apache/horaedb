// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! This module implements [write][1] and [query][2] for InfluxDB.
//! [1]: https://docs.influxdata.com/influxdb/v1.8/tools/api/#write-http-endpoint
//! [2]: https://docs.influxdata.com/influxdb/v1.8/tools/api/#query-http-endpoint

pub mod types;

use std::time::Instant;

use common_types::request_id::RequestId;
use common_util::{error::BoxError, time::InstantExt};
use http::StatusCode;
use interpreters::interpreter::Output;
use log::{debug, info};
use query_engine::executor::Executor as QueryExecutor;
use query_frontend::{
    frontend::{Context as SqlContext, Frontend},
    provider::CatalogMetaProvider,
};
use snafu::{ensure, ResultExt};

use crate::{
    context::RequestContext,
    error::{ErrNoCause, ErrWithCause, Internal, Result},
    execute_plan,
    grpc::write::{execute_insert_plan, write_request_to_insert_plan, WriteContext},
    influxdb::types::{
        convert_influxql_output, convert_write_request, InfluxqlRequest, InfluxqlResponse,
        WriteRequest, WriteResponse,
    },
    Proxy,
};

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    pub async fn handle_influxdb_query(
        &self,
        ctx: RequestContext,
        req: InfluxqlRequest,
    ) -> Result<InfluxqlResponse> {
        let output = self.fetch_influxdb_query_output(ctx, req).await?;
        convert_influxql_output(output)
    }

    pub async fn handle_influxdb_write(
        &self,
        ctx: RequestContext,
        req: WriteRequest,
    ) -> Result<WriteResponse> {
        let request_id = RequestId::next_id();
        let deadline = ctx.timeout.map(|t| Instant::now() + t);
        let catalog = &ctx.catalog;
        self.instance.catalog_manager.default_catalog_name();
        let schema = &ctx.schema;
        let schema_config = self
            .schema_config_provider
            .schema_config(schema)
            .box_err()
            .with_context(|| Internal {
                msg: format!("get schema config failed, schema:{schema}"),
            })?;

        let write_context =
            WriteContext::new(request_id, deadline, catalog.clone(), schema.clone());

        let plans = write_request_to_insert_plan(
            self.instance.clone(),
            convert_write_request(req)?,
            schema_config,
            write_context,
        )
        .await
        .box_err()
        .with_context(|| Internal {
            msg: "write request to insert plan",
        })?;

        let mut success = 0;
        for insert_plan in plans {
            success += execute_insert_plan(
                request_id,
                catalog,
                schema,
                self.instance.clone(),
                insert_plan,
                deadline,
            )
            .await
            .box_err()
            .with_context(|| Internal {
                msg: "execute plan",
            })?;
        }
        debug!(
            "Influxdb write finished, catalog:{}, schema:{}, success:{}",
            catalog, schema, success
        );

        Ok(())
    }

    async fn fetch_influxdb_query_output(
        &self,
        ctx: RequestContext,
        req: InfluxqlRequest,
    ) -> Result<Output> {
        let request_id = RequestId::next_id();
        let begin_instant = Instant::now();
        let deadline = ctx.timeout.map(|t| begin_instant + t);

        info!(
            "Influxdb query handler try to process request, request_id:{}, request:{:?}",
            request_id, req
        );

        // TODO(yingwen): Privilege check, cannot access data of other tenant
        // TODO(yingwen): Maybe move MetaProvider to instance
        let provider = CatalogMetaProvider {
            manager: self.instance.catalog_manager.clone(),
            default_catalog: &ctx.catalog,
            default_schema: &ctx.schema,
            function_registry: &*self.instance.function_registry,
        };
        let frontend = Frontend::new(provider);
        let mut sql_ctx = SqlContext::new(request_id, deadline);

        let mut stmts = frontend
            .parse_influxql(&mut sql_ctx, &req.query)
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!("Failed to parse influxql, query:{}", req.query),
            })?;

        if stmts.is_empty() {
            return Ok(Output::AffectedRows(0));
        }

        ensure!(
            stmts.len() == 1,
            ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!(
                    "Only support execute one statement now, current num:{}, query:{}.",
                    stmts.len(),
                    req.query
                ),
            }
        );

        let plan = frontend
            .influxql_stmt_to_plan(&mut sql_ctx, stmts.remove(0))
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!("Failed to build plan, query:{}", req.query),
            })?;

        self.instance
            .limiter
            .try_limit(&plan)
            .box_err()
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "Query is blocked",
            })?;
        let output = execute_plan(
            request_id,
            &ctx.catalog,
            &ctx.schema,
            self.instance.clone(),
            plan,
            deadline,
        )
        .await?;

        info!(
            "Influxdb query handler finished, request_id:{}, cost:{}ms, request:{:?}",
            request_id,
            begin_instant.saturating_elapsed().as_millis(),
            req
        );

        Ok(output)
    }
}
