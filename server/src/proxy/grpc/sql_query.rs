// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Query handler

use std::time::Instant;

use ceresdbproto::storage::{
    storage_service_client::StorageServiceClient, SqlQueryRequest, SqlQueryResponse,
};
use common_types::request_id::RequestId;
use common_util::{error::BoxError, time::InstantExt};
use futures::FutureExt;
use interpreters::{context::Context as InterpreterContext, factory::Factory, interpreter::Output};
use log::{error, info, warn};
use query_engine::executor::Executor as QueryExecutor;
use router::endpoint::Endpoint;
use snafu::{ensure, ResultExt};
use sql::{
    frontend::{Context as SqlContext, Frontend},
    provider::CatalogMetaProvider,
};
use tonic::{transport::Channel, IntoRequest};

use crate::proxy::{
    error::{BadRequest, BadRequestWithoutErr, Internal, InternalWithoutErr, Result},
    forward::{ForwardRequest, ForwardResult},
    Context, Proxy,
};

pub enum QueryResponse {
    Forward(SqlQueryResponse),
    Direct(Output),
}

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    pub async fn handle_query(&self, ctx: Context, req: SqlQueryRequest) -> Result<QueryResponse> {
        let req = match self.maybe_forward_query(&req).await {
            Some(resp) => return Ok(QueryResponse::Forward(resp?)),
            None => req,
        };

        let output = self.fetch_query_output(ctx, &req).await?;
        Ok(QueryResponse::Direct(output))
    }

    async fn maybe_forward_query(&self, req: &SqlQueryRequest) -> Option<Result<SqlQueryResponse>> {
        if req.tables.len() != 1 {
            warn!(
                "Unable to forward query without exactly one table, req:{:?}",
                req
            );

            return None;
        }

        let req_ctx = req.context.as_ref().unwrap();
        let forward_req = ForwardRequest {
            schema: req_ctx.database.clone(),
            table: req.tables[0].clone(),
            req: req.clone().into_request(),
        };
        let do_query = |mut client: StorageServiceClient<Channel>,
                        request: tonic::Request<SqlQueryRequest>,
                        _: &Endpoint| {
            let query = async move {
                client
                    .sql_query(request)
                    .await
                    .map(|resp| resp.into_inner())
                    .box_err()
                    .context(Internal {
                        msg: "Forwarded query failed",
                    })
            }
            .boxed();

            Box::new(query) as _
        };

        match self.forwarder.forward(forward_req, do_query).await {
            Ok(forward_res) => match forward_res {
                ForwardResult::Forwarded(v) => Some(v),
                ForwardResult::Original => None,
            },
            Err(e) => {
                error!("Failed to forward req but the error is ignored, err:{}", e);
                None
            }
        }
    }

    pub async fn fetch_query_output(&self, ctx: Context, req: &SqlQueryRequest) -> Result<Output> {
        let request_id = RequestId::next_id();
        let begin_instant = Instant::now();
        let deadline = ctx.timeout.map(|t| begin_instant + t);
        let catalog = self.instance.catalog_manager.default_catalog_name();

        info!(
            "Grpc handle query begin, request_id:{}, request:{:?}",
            request_id, req,
        );

        let req_ctx = req.context.as_ref().unwrap();
        let schema = &req_ctx.database;
        let instance = &self.instance;
        // TODO(yingwen): Privilege check, cannot access data of other tenant
        // TODO(yingwen): Maybe move MetaProvider to instance
        let provider = CatalogMetaProvider {
            manager: instance.catalog_manager.clone(),
            default_catalog: catalog,
            default_schema: schema,
            function_registry: &*instance.function_registry,
        };
        let frontend = Frontend::new(provider);

        let mut sql_ctx = SqlContext::new(request_id, deadline);
        // Parse sql, frontend error of invalid sql already contains sql
        // TODO(yingwen): Maybe move sql from frontend error to outer error
        let mut stmts = frontend
            .parse_sql(&mut sql_ctx, &req.sql)
            .box_err()
            .context(BadRequest {
                msg: "failed to parse sql",
            })?;

        ensure!(
            !stmts.is_empty(),
            BadRequestWithoutErr {
                msg: format!("No valid query statement provided, sql:{}", req.sql),
            }
        );

        // TODO(yingwen): For simplicity, we only support executing one statement now
        // TODO(yingwen): INSERT/UPDATE/DELETE can be batched
        ensure!(
            stmts.len() == 1,
            BadRequestWithoutErr {
                msg: format!(
                    "Only support execute one statement now, current num:{}, sql:{}",
                    stmts.len(),
                    req.sql
                ),
            }
        );

        // Create logical plan
        // Note: Remember to store sql in error when creating logical plan
        let plan = frontend
            // TODO(yingwen): Check error, some error may indicate that the sql is invalid. Now we
            // return internal server error in those cases
            .statement_to_plan(&mut sql_ctx, stmts.remove(0))
            .box_err()
            .with_context(|| Internal {
                msg: format!("Failed to create plan, query:{}", req.sql),
            })?;

        instance
            .limiter
            .try_limit(&plan)
            .box_err()
            .context(Internal {
                msg: "Query is blocked",
            })?;

        if let Some(deadline) = deadline {
            if deadline.check_deadline() {
                return InternalWithoutErr {
                    msg: "Query timeout",
                }
                .fail();
            }
        }

        // Execute in interpreter
        let interpreter_ctx = InterpreterContext::builder(request_id, deadline)
            // Use current ctx's catalog and schema as default catalog and schema
            .default_catalog_and_schema(catalog.to_string(), schema.to_string())
            .build();
        let interpreter_factory = Factory::new(
            instance.query_executor.clone(),
            instance.catalog_manager.clone(),
            instance.table_engine.clone(),
            instance.table_manipulator.clone(),
        );
        let interpreter = interpreter_factory
            .create(interpreter_ctx, plan)
            .box_err()
            .with_context(|| Internal {
                msg: "Failed to create interpreter",
            })?;

        let output = if let Some(deadline) = deadline {
            tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                interpreter.execute(),
            )
            .await
            .box_err()
            .context(Internal {
                msg: "Query timeout",
            })?
        } else {
            interpreter.execute().await
        }
        .box_err()
        .with_context(|| Internal {
            msg: format!("Failed to execute interpreter, sql:{}", req.sql),
        })?;

        info!(
        "Grpc handle query success, catalog:{}, schema:{}, request_id:{}, cost:{}ms, request:{:?}",
        catalog,
        schema,
        request_id,
        begin_instant.saturating_elapsed().as_millis(),
        req,
    );

        Ok(output)
    }
}
