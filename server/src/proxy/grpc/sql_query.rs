// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Query handler

use std::{sync::Arc, time::Instant};

use arrow_ext::ipc::{CompressOptions, CompressionMethod, RecordBatchesEncoder};
use catalog::schema::{CreateOptions, CreateTableRequest, DropOptions, DropTableRequest};
use ceresdbproto::{
    common::ResponseHeader,
    storage::{
        arrow_payload, sql_query_response, storage_service_client::StorageServiceClient,
        ArrowPayload, SqlQueryRequest, SqlQueryResponse,
    },
};
use common_types::{record_batch::RecordBatch, request_id::RequestId, table::DEFAULT_SHARD_ID};
use common_util::{error::BoxError, time::InstantExt};
use futures::{stream, stream::BoxStream, FutureExt, StreamExt};
use http::StatusCode;
use interpreters::{context::Context as InterpreterContext, factory::Factory, interpreter::Output};
use log::{error, info, warn};
use query_engine::executor::Executor as QueryExecutor;
use router::endpoint::Endpoint;
use snafu::{ensure, OptionExt, ResultExt};
use sql::{
    frontend,
    frontend::{Context as SqlContext, Frontend},
    provider::CatalogMetaProvider,
};
use table_engine::{
    engine::TableState,
    partition::{format_sub_partition_table_name, PartitionInfo},
    remote::model::{GetTableInfoRequest, TableIdentifier},
    table::TableId,
    PARTITION_TABLE_ENGINE_TYPE,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Channel, IntoRequest};

use crate::proxy::{
    error::{self, ErrNoCause, ErrWithCause, Error, Result},
    forward::{ForwardRequest, ForwardResult},
    Context, Proxy,
};

const STREAM_QUERY_CHANNEL_LEN: usize = 20;

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    pub async fn handle_sql_query(&self, ctx: Context, req: SqlQueryRequest) -> SqlQueryResponse {
        self.hotspot_recorder.inc_sql_query_reqs(&req).await;
        match self.handle_sql_query_internal(ctx, req).await {
            Err(e) => {
                error!("Failed to handle sql query, err:{e}");
                SqlQueryResponse {
                    header: Some(error::build_err_header(e)),
                    ..Default::default()
                }
            }
            Ok(v) => v,
        }
    }

    pub async fn handle_stream_sql_query(
        self: Arc<Self>,
        ctx: Context,
        req: SqlQueryRequest,
    ) -> BoxStream<'static, SqlQueryResponse> {
        self.hotspot_recorder.inc_sql_query_reqs(&req).await;
        match self.clone().handle_stream_query_internal(ctx, req).await {
            Err(e) => stream::once(async {
                error!("Failed to handle stream sql query, err:{e}");
                SqlQueryResponse {
                    header: Some(error::build_err_header(e)),
                    ..Default::default()
                }
            })
            .boxed(),
            Ok(v) => v,
        }
    }

    async fn handle_sql_query_internal(
        &self,
        ctx: Context,
        req: SqlQueryRequest,
    ) -> Result<SqlQueryResponse> {
        let req = match self.maybe_forward_sql_query(&req).await {
            Some(resp) => match resp {
                ForwardResult::Forwarded(resp) => return resp,
                ForwardResult::Original => req,
            },
            None => req,
        };

        let output = self.fetch_sql_query_output(ctx, &req).await?;
        convert_output(&output, self.resp_compress_min_length)
    }

    async fn handle_stream_query_internal(
        self: Arc<Self>,
        ctx: Context,
        req: SqlQueryRequest,
    ) -> Result<BoxStream<'static, SqlQueryResponse>> {
        let req = match self.clone().maybe_forward_stream_sql_query(&req).await {
            Some(resp) => match resp {
                ForwardResult::Forwarded(resp) => return resp,
                ForwardResult::Original => req,
            },
            None => req,
        };

        let (tx, rx) = mpsc::channel(STREAM_QUERY_CHANNEL_LEN);
        let runtime = ctx.runtime.clone();
        let resp_compress_min_length = self.resp_compress_min_length;
        let output = self.as_ref().fetch_sql_query_output(ctx, &req).await?;
        runtime.spawn(async move {
            match output {
                Output::AffectedRows(rows) => {
                    let resp =
                        QueryResponseBuilder::with_ok_header().build_with_affected_rows(rows);
                    if tx.send(resp).await.is_err() {
                        error!("Failed to send affected rows resp in stream sql query");
                    }
                }
                Output::Records(batches) => {
                    for batch in &batches {
                        let resp = {
                            let mut writer = QueryResponseWriter::new(resp_compress_min_length);
                            writer.write(batch)?;
                            writer.finish()
                        }?;

                        if tx.send(resp).await.is_err() {
                            error!("Failed to send record batches resp in stream sql query");
                            break;
                        }
                    }
                }
            }
            Ok::<(), Error>(())
        });
        Ok(ReceiverStream::new(rx).boxed())
    }

    async fn maybe_forward_sql_query(
        &self,
        req: &SqlQueryRequest,
    ) -> Option<ForwardResult<SqlQueryResponse, Error>> {
        if req.tables.len() != 1 {
            warn!("Unable to forward sql query without exactly one table, req:{req:?}",);

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
                    .context(ErrWithCause {
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: "Forwarded sql query failed",
                    })
            }
            .boxed();

            Box::new(query) as _
        };

        let forward_result = self.forwarder.forward(forward_req, do_query).await;
        match forward_result {
            Ok(forward_res) => Some(forward_res),
            Err(e) => {
                error!("Failed to forward sql req but the error is ignored, err:{e}");
                None
            }
        }
    }

    async fn maybe_forward_stream_sql_query(
        self: Arc<Self>,
        req: &SqlQueryRequest,
    ) -> Option<ForwardResult<BoxStream<'static, SqlQueryResponse>, Error>> {
        if req.tables.len() != 1 {
            warn!("Unable to forward sql query without exactly one table, req:{req:?}",);

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
                    .stream_sql_query(request)
                    .await
                    .map(|resp| resp.into_inner().boxed())
                    .box_err()
                    .context(ErrWithCause {
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: "Forwarded stream sql query failed",
                    })
                    .map(|stream| {
                        stream
                            .map(|item| {
                                item.box_err()
                                    .context(ErrWithCause {
                                        code: StatusCode::INTERNAL_SERVER_ERROR,
                                        msg: "Fail to fetch stream sql query response",
                                    })
                                    .unwrap_or_else(|e| SqlQueryResponse {
                                        header: Some(error::build_err_header(e)),
                                        ..Default::default()
                                    })
                            })
                            .boxed()
                    })
            }
            .boxed();

            Box::new(query) as _
        };

        let forward_result = self.forwarder.forward(forward_req, do_query).await;

        match forward_result {
            Ok(forward_res) => Some(forward_res),
            Err(e) => {
                error!("Failed to forward stream sql req but the error is ignored, err:{e}");
                None
            }
        }
    }

    async fn fetch_sql_query_output(&self, ctx: Context, req: &SqlQueryRequest) -> Result<Output> {
        let request_id = RequestId::next_id();
        let begin_instant = Instant::now();
        let deadline = ctx.timeout.map(|t| begin_instant + t);
        let catalog = self.instance.catalog_manager.default_catalog_name();

        info!("Handle sql query, request_id:{request_id}, request:{req:?}");

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
            .context(ErrWithCause {
                code: StatusCode::BAD_REQUEST,
                msg: "Failed to parse sql",
            })?;

        ensure!(
            !stmts.is_empty(),
            ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!("No valid query statement provided, sql:{}", req.sql),
            }
        );

        // TODO(yingwen): For simplicity, we only support executing one statement now
        // TODO(yingwen): INSERT/UPDATE/DELETE can be batched
        ensure!(
            stmts.len() == 1,
            ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!(
                    "Only support execute one statement now, current num:{}, sql:{}",
                    stmts.len(),
                    req.sql
                ),
            }
        );

        // Open partition table if needed.
        let table_name = frontend::parse_table_name(&stmts);
        if let Some(table_name) = table_name {
            self.maybe_open_partition_table_if_not_exist(catalog, schema, &table_name)
                .await?;
        }

        // Create logical plan
        // Note: Remember to store sql in error when creating logical plan
        let plan = frontend
            // TODO(yingwen): Check error, some error may indicate that the sql is invalid. Now we
            // return internal server error in those cases
            .statement_to_plan(&mut sql_ctx, stmts.remove(0))
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!("Failed to create plan, query:{}", req.sql),
            })?;

        instance
            .limiter
            .try_limit(&plan)
            .box_err()
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "Query is blocked",
            })?;

        if let Some(deadline) = deadline {
            if deadline.check_deadline() {
                return ErrNoCause {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
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
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "Failed to create interpreter",
            })?;

        let output = if let Some(deadline) = deadline {
            tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                interpreter.execute(),
            )
            .await
            .box_err()
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "Query timeout",
            })?
        } else {
            interpreter.execute().await
        }
        .box_err()
        .with_context(|| ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: format!("Failed to execute interpreter, sql:{}", req.sql),
        })?;

        info!(
        "Handle sql query success, catalog:{}, schema:{}, request_id:{}, cost:{}ms, request:{:?}",
        catalog,
        schema,
        request_id,
        begin_instant.saturating_elapsed().as_millis(),
        req,
    );

        Ok(output)
    }

    async fn maybe_open_partition_table_if_not_exist(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<()> {
        let partition_table_info = self
            .router
            .fetch_partition_table_info(schema_name, table_name)
            .await?;
        if partition_table_info.is_none() {
            return Ok(());
        }

        let partition_table_info = partition_table_info.unwrap();

        let catalog = self
            .instance
            .catalog_manager
            .catalog_by_name(catalog_name)
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!("Failed to find catalog, catalog_name:{catalog_name}"),
            })?
            .with_context(|| ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!("Catalog not found, catalog_name:{catalog_name}"),
            })?;

        // TODO: support create schema if not exist
        let schema = catalog
            .schema_by_name(schema_name)
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!("Failed to find schema, schema_name:{schema_name}"),
            })?
            .context(ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!("Schema not found, schema_name:{schema_name}"),
            })?;
        let table = schema
            .table_by_name(&partition_table_info.name)
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!(
                    "Failed to find table, table_name:{}",
                    partition_table_info.name
                ),
            })?;

        if let Some(table) = table {
            if table.id().as_u64() == partition_table_info.id {
                return Ok(());
            }

            // Drop partition table if table id not match.
            let opts = DropOptions {
                table_engine: self.instance.partition_table_engine.clone(),
            };
            schema
                .drop_table(
                    DropTableRequest {
                        catalog_name: catalog_name.to_string(),
                        schema_name: schema_name.to_string(),
                        table_name: table_name.to_string(),
                        engine: PARTITION_TABLE_ENGINE_TYPE.to_string(),
                    },
                    opts,
                )
                .await
                .box_err()
                .with_context(|| ErrWithCause {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    msg: format!("Failed to drop partition table, table_name:{table_name}"),
                })?;
        }

        // If table not exists, open it.
        // Get table_schema from first sub partition table.
        let first_sub_partition_table_name = get_sub_partition_name(
            &partition_table_info.name,
            &partition_table_info.partition_info,
            0usize,
        );
        let table = self
            .instance
            .remote_engine_ref
            .get_table_info(GetTableInfoRequest {
                table: TableIdentifier {
                    catalog: catalog_name.to_string(),
                    schema: schema_name.to_string(),
                    table: first_sub_partition_table_name,
                },
            })
            .await
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "Failed to get table",
            })?;

        // Partition table is a virtual table, so we need to create it manually.
        // Partition info is stored in ceresmeta, so we need to use create_table_request
        // to create it.
        let create_table_request = CreateTableRequest {
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
            table_name: partition_table_info.name,
            table_id: Some(TableId::new(partition_table_info.id)),
            table_schema: table.table_schema,
            engine: table.engine,
            options: Default::default(),
            state: TableState::Stable,
            shard_id: DEFAULT_SHARD_ID,
            partition_info: Some(partition_table_info.partition_info),
        };
        let create_opts = CreateOptions {
            table_engine: self.instance.partition_table_engine.clone(),
            create_if_not_exists: true,
        };
        schema
            .create_table(create_table_request.clone(), create_opts)
            .await
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!("Failed to create table, request:{create_table_request:?}"),
            })?;
        Ok(())
    }
}

// TODO(chenxiang): Output can have both `rows` and `affected_rows`
pub fn convert_output(
    output: &Output,
    resp_compress_min_length: usize,
) -> Result<SqlQueryResponse> {
    match output {
        Output::Records(batches) => {
            let mut writer = QueryResponseWriter::new(resp_compress_min_length);
            writer.write_batches(batches)?;
            writer.finish()
        }
        Output::AffectedRows(rows) => {
            Ok(QueryResponseBuilder::with_ok_header().build_with_affected_rows(*rows))
        }
    }
}

/// Builder for building [`SqlQueryResponse`].
#[derive(Debug, Default)]
pub struct QueryResponseBuilder {
    header: ResponseHeader,
}

impl QueryResponseBuilder {
    pub fn with_ok_header() -> Self {
        let header = ResponseHeader {
            code: StatusCode::OK.as_u16() as u32,
            ..Default::default()
        };
        Self { header }
    }

    pub fn build_with_affected_rows(self, affected_rows: usize) -> SqlQueryResponse {
        let output = Some(sql_query_response::Output::AffectedRows(
            affected_rows as u32,
        ));
        SqlQueryResponse {
            header: Some(self.header),
            output,
        }
    }

    pub fn build_with_empty_arrow_payload(self) -> SqlQueryResponse {
        let payload = ArrowPayload {
            record_batches: Vec::new(),
            compression: arrow_payload::Compression::None as i32,
        };
        self.build_with_arrow_payload(payload)
    }

    pub fn build_with_arrow_payload(self, payload: ArrowPayload) -> SqlQueryResponse {
        let output = Some(sql_query_response::Output::Arrow(payload));
        SqlQueryResponse {
            header: Some(self.header),
            output,
        }
    }
}

/// Writer for encoding multiple [`RecordBatch`]es to the [`SqlQueryResponse`].
///
/// Whether to do compression depends on the size of the encoded bytes.
pub struct QueryResponseWriter {
    encoder: RecordBatchesEncoder,
}

impl QueryResponseWriter {
    pub fn new(compress_min_length: usize) -> Self {
        let compress_opts = CompressOptions {
            compress_min_length,
            method: CompressionMethod::Zstd,
        };
        Self {
            encoder: RecordBatchesEncoder::new(compress_opts),
        }
    }

    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        self.encoder
            .write(batch.as_arrow_record_batch())
            .box_err()
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "Failed to encode record batch",
            })
    }

    pub fn write_batches(&mut self, record_batch: &[RecordBatch]) -> Result<()> {
        for batch in record_batch {
            self.write(batch)?;
        }

        Ok(())
    }

    pub fn finish(self) -> Result<SqlQueryResponse> {
        let compress_output = self.encoder.finish().box_err().context(ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: "Failed to encode record batch",
        })?;

        if compress_output.payload.is_empty() {
            return Ok(QueryResponseBuilder::with_ok_header().build_with_empty_arrow_payload());
        }

        let compression = match compress_output.method {
            CompressionMethod::None => arrow_payload::Compression::None,
            CompressionMethod::Zstd => arrow_payload::Compression::Zstd,
        };
        let resp = QueryResponseBuilder::with_ok_header().build_with_arrow_payload(ArrowPayload {
            record_batches: vec![compress_output.payload],
            compression: compression as i32,
        });

        Ok(resp)
    }
}

fn get_sub_partition_name(table_name: &str, partition_info: &PartitionInfo, id: usize) -> String {
    let partition_name = partition_info.get_definitions()[id].name.clone();
    format_sub_partition_table_name(table_name, &partition_name)
}
