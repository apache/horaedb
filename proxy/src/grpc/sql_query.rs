// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Query handler

use std::sync::Arc;

use arrow_ext::ipc::{CompressOptions, CompressionMethod, RecordBatchesEncoder};
use ceresdbproto::{
    common::ResponseHeader,
    storage::{
        arrow_payload, sql_query_response, storage_service_client::StorageServiceClient,
        ArrowPayload, SqlQueryRequest, SqlQueryResponse,
    },
};
use common_types::record_batch::RecordBatch;
use futures::{stream, stream::BoxStream, FutureExt, StreamExt};
use generic_error::BoxError;
use http::StatusCode;
use interpreters::interpreter::Output;
use log::{error, warn};
use query_engine::{executor::Executor as QueryExecutor, physical_planner::PhysicalPlanner};
use router::endpoint::Endpoint;
use snafu::ResultExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Channel, IntoRequest};

use crate::{
    error::{self, ErrNoCause, ErrWithCause, Error, Result},
    forward::{ForwardRequest, ForwardResult},
    metrics::GRPC_HANDLER_COUNTER_VEC,
    read::SqlResponse,
    Context, Proxy,
};

const STREAM_QUERY_CHANNEL_LEN: usize = 20;

impl<Q: QueryExecutor + 'static, P: PhysicalPlanner> Proxy<Q, P> {
    pub async fn handle_sql_query(&self, ctx: Context, req: SqlQueryRequest) -> SqlQueryResponse {
        // Incoming query maybe larger than query_failed + query_succeeded for some
        // corner case, like lots of time-consuming queries come in at the same time and
        // cause server OOM.
        GRPC_HANDLER_COUNTER_VEC.incoming_query.inc();

        self.hotspot_recorder.inc_sql_query_reqs(&req).await;
        match self.handle_sql_query_internal(&ctx, &req).await {
            Err(e) => {
                error!("Failed to handle sql query, ctx:{ctx:?}, err:{e}");

                GRPC_HANDLER_COUNTER_VEC.query_failed.inc();
                SqlQueryResponse {
                    header: Some(error::build_err_header(e)),
                    ..Default::default()
                }
            }
            Ok(v) => {
                GRPC_HANDLER_COUNTER_VEC.query_succeeded.inc();
                v
            }
        }
    }

    async fn handle_sql_query_internal(
        &self,
        ctx: &Context,
        req: &SqlQueryRequest,
    ) -> Result<SqlQueryResponse> {
        if req.context.is_none() {
            return ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: "Database is not set",
            }
            .fail();
        }

        let req_context = req.context.as_ref().unwrap();
        let schema = &req_context.database;
        match self.handle_sql(ctx, schema, &req.sql, false).await? {
            SqlResponse::Forwarded(resp) => Ok(resp),
            SqlResponse::Local(output) => convert_output(&output, self.resp_compress_min_length),
        }
    }

    pub async fn handle_stream_sql_query(
        self: Arc<Self>,
        ctx: Context,
        req: SqlQueryRequest,
    ) -> BoxStream<'static, SqlQueryResponse> {
        GRPC_HANDLER_COUNTER_VEC.stream_query.inc();
        self.hotspot_recorder.inc_sql_query_reqs(&req).await;
        match self.clone().handle_stream_query_internal(&ctx, &req).await {
            Err(e) => stream::once(async {
                error!("Failed to handle stream sql query, err:{e}");
                GRPC_HANDLER_COUNTER_VEC.stream_query_failed.inc();
                SqlQueryResponse {
                    header: Some(error::build_err_header(e)),
                    ..Default::default()
                }
            })
            .boxed(),
            Ok(v) => {
                GRPC_HANDLER_COUNTER_VEC.stream_query_succeeded.inc();
                v
            }
        }
    }

    async fn handle_stream_query_internal(
        self: Arc<Self>,
        ctx: &Context,
        req: &SqlQueryRequest,
    ) -> Result<BoxStream<'static, SqlQueryResponse>> {
        if req.context.is_none() {
            return ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: "Database is not set",
            }
            .fail();
        }

        let req_context = req.context.as_ref().unwrap();
        let schema = &req_context.database;
        let req = match self.clone().maybe_forward_stream_sql_query(ctx, req).await {
            Some(resp) => match resp {
                ForwardResult::Forwarded(resp) => return resp,
                ForwardResult::Local => req,
            },
            None => req,
        };

        let (tx, rx) = mpsc::channel(STREAM_QUERY_CHANNEL_LEN);
        let runtime = ctx.runtime.clone();
        let resp_compress_min_length = self.resp_compress_min_length;
        let output = self
            .as_ref()
            .fetch_sql_query_output(ctx, schema, &req.sql, false)
            .await?;
        runtime.spawn(async move {
            match output {
                Output::AffectedRows(rows) => {
                    let resp =
                        QueryResponseBuilder::with_ok_header().build_with_affected_rows(rows);
                    if tx.send(resp).await.is_err() {
                        error!("Failed to send affected rows resp in stream sql query");
                    }
                    GRPC_HANDLER_COUNTER_VEC
                        .query_affected_row
                        .inc_by(rows as u64);
                }
                Output::Records(batches) => {
                    let mut num_rows = 0;
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
                        num_rows += batch.num_rows();
                    }
                    GRPC_HANDLER_COUNTER_VEC
                        .query_succeeded_row
                        .inc_by(num_rows as u64);
                }
            }
            Ok::<(), Error>(())
        });
        Ok(ReceiverStream::new(rx).boxed())
    }

    async fn maybe_forward_stream_sql_query(
        self: Arc<Self>,
        ctx: &Context,
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
            forwarded_from: ctx.forwarded_from.clone(),
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
            let mut num_rows = 0;
            for batch in batches {
                num_rows += batch.num_rows();
            }
            GRPC_HANDLER_COUNTER_VEC
                .query_succeeded_row
                .inc_by(num_rows as u64);
            writer.finish()
        }
        Output::AffectedRows(rows) => {
            GRPC_HANDLER_COUNTER_VEC
                .query_affected_row
                .inc_by(*rows as u64);
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
