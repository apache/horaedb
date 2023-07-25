// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

// Remote engine rpc service implementation.

use std::{
    sync::{Arc, RwLock},
    time::Instant,
};

use arrow_ext::ipc::{self, CompressOptions, CompressOutput, CompressionMethod};
use async_trait::async_trait;
use catalog::{manager::ManagerRef, schema::SchemaRef};
use ceresdbproto::{
    remote_engine::{
        read_response::Output::Arrow, remote_engine_service_server::RemoteEngineService,
        GetTableInfoRequest, GetTableInfoResponse, ReadRequest, ReadResponse, WriteBatchRequest,
        WriteRequest, WriteResponse,
    },
    storage::{arrow_payload, ArrowPayload},
};
use common_types::record_batch::RecordBatch;
use futures::stream::{self, BoxStream, FuturesUnordered, StreamExt};
use generic_error::BoxError;
use log::{error, info};
use proxy::instance::InstanceRef;
use query_engine::executor::Executor as QueryExecutor;
use snafu::{OptionExt, ResultExt};
use table_engine::{
    engine::EngineRuntimes, remote::model::TableIdentifier, stream::PartitionedStreams,
    table::TableRef,
};
use time_ext::InstantExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::grpc::{
    metrics::{
        REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC, REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC,
    },
    remote_engine_service::{
        dedup_request::{DedupMap, Notifiers, RequestKey},
        error::{ErrNoCause, ErrWithCause, Result, StatusCode},
    },
};

pub mod dedup_request;
mod error;

const STREAM_QUERY_CHANNEL_LEN: usize = 20;
const DEFAULT_COMPRESS_MIN_LENGTH: usize = 80 * 1024;

#[derive(Clone)]
pub struct RemoteEngineServiceImpl<Q: QueryExecutor + 'static> {
    pub instance: InstanceRef<Q>,
    pub runtimes: Arc<EngineRuntimes>,
    pub dedup_map: Arc<RwLock<DedupMap>>,
}

impl<Q: QueryExecutor + 'static> RemoteEngineServiceImpl<Q> {
    async fn stream_read_internal(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<ReceiverStream<Result<RecordBatch>>> {
        let instant = Instant::now();
        let ctx = self.handler_ctx();
        let (tx, rx) = mpsc::channel(STREAM_QUERY_CHANNEL_LEN);

        let request = request.into_inner();
        let table_engine::remote::model::ReadRequest {
            table,
            read_request,
        } = request.clone().try_into().box_err().context(ErrWithCause {
            code: StatusCode::BadRequest,
            msg: "fail to convert read request",
        })?;

        let request_key = RequestKey::new(
            table.table,
            read_request.predicate.clone(),
            read_request.projected_schema.projection(),
            read_request.order,
        );

        {
            let dedup_map = self.dedup_map.read().unwrap();
            if dedup_map.contains_key(&request_key) {
                let notifiers = dedup_map.get_notifiers(&request_key).unwrap();
                notifiers.add_notifier(tx);

                REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC
                    .stream_read
                    .observe(instant.saturating_elapsed().as_secs_f64());
                return Ok(ReceiverStream::new(rx));
            } else {
                drop(dedup_map);

                let mut dedup_map = self.dedup_map.write().unwrap();
                if dedup_map.contains_key(&request_key) {
                    let notifiers = dedup_map.get_notifiers(&request_key).unwrap();
                    notifiers.add_notifier(tx);

                    REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC
                        .stream_read
                        .observe(instant.saturating_elapsed().as_secs_f64());
                    return Ok(ReceiverStream::new(rx));
                } else {
                    let notifiers = Notifiers::new(tx);
                    dedup_map.add_notifiers(request_key.clone(), notifiers);
                }
            }
        }

        let handle = self
            .runtimes
            .read_runtime
            .spawn(async move { handle_stream_read(ctx, request).await });
        let streams = handle.await.box_err().context(ErrWithCause {
            code: StatusCode::Internal,
            msg: "fail to join task",
        })??;

        let mut stream_read = FuturesUnordered::new();
        for stream in streams.streams {
            let mut stream = stream.map(|result| {
                result.box_err().context(ErrWithCause {
                    code: StatusCode::Internal,
                    msg: "record batch failed",
                })
            });

            let handle = self.runtimes.read_runtime.spawn(async move {
                let mut batches = Vec::new();
                while let Some(batch) = stream.next().await {
                    batches.push(batch)
                }

                batches
            });
            stream_read.push(handle);
        }

        let mut batches = Vec::new();
        while let Some(result) = stream_read.next().await {
            let batch = result.box_err().context(ErrWithCause {
                code: StatusCode::Internal,
                msg: "failed to join task",
            })?;
            batches.extend(batch);
        }

        let notifiers = {
            let mut dedup_map = self.dedup_map.write().unwrap();
            let notifiers = dedup_map.delete_notifiers(request_key).unwrap();
            notifiers.into_notifiers()
        };

        let mut num_rows = 0;
        for batch in batches {
            match batch {
                Ok(batch) => {
                    num_rows += batch.num_rows() * notifiers.len();
                    for tx in &notifiers {
                        if let Err(e) = tx.send(Ok(batch.clone())).await {
                            error!("Failed to send handler result, err:{}.", e);
                        }
                    }
                }
                Err(_) => {
                    for tx in &notifiers {
                        if let Err(e) = tx
                            .send(
                                ErrNoCause {
                                    code: StatusCode::Internal,
                                    msg: "failed to handler request".to_string(),
                                }
                                .fail(),
                            )
                            .await
                        {
                            error!("Failed to send handler result, err:{}.", e);
                        }
                    }
                }
            }
        }

        REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC
            .query_succeeded_row
            .inc_by(num_rows as u64);

        REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .stream_read
            .observe(instant.saturating_elapsed().as_secs_f64());
        Ok(ReceiverStream::new(rx))
    }

    async fn write_internal(
        &self,
        request: Request<WriteRequest>,
    ) -> std::result::Result<Response<WriteResponse>, Status> {
        let begin_instant = Instant::now();
        let ctx = self.handler_ctx();
        let handle = self.runtimes.write_runtime.spawn(async move {
            let request = request.into_inner();
            handle_write(ctx, request).await
        });

        let res = handle.await.box_err().context(ErrWithCause {
            code: StatusCode::Internal,
            msg: "fail to join task",
        });

        let mut resp = WriteResponse::default();
        match res {
            Ok(Ok(v)) => {
                resp.header = Some(error::build_ok_header());
                resp.affected_rows = v.affected_rows;
            }
            Ok(Err(e)) | Err(e) => {
                resp.header = Some(error::build_err_header(e));
            }
        };

        REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .write
            .observe(begin_instant.saturating_elapsed().as_secs_f64());
        Ok(Response::new(resp))
    }

    async fn get_table_info_internal(
        &self,
        request: Request<GetTableInfoRequest>,
    ) -> std::result::Result<Response<GetTableInfoResponse>, Status> {
        let begin_instant = Instant::now();
        let ctx = self.handler_ctx();
        let handle = self.runtimes.read_runtime.spawn(async move {
            let request = request.into_inner();
            handle_get_table_info(ctx, request).await
        });

        let res = handle.await.box_err().context(ErrWithCause {
            code: StatusCode::Internal,
            msg: "fail to join task",
        });

        let mut resp = GetTableInfoResponse::default();
        match res {
            Ok(Ok(v)) => {
                resp.header = Some(error::build_ok_header());
                resp.table_info = v.table_info;
            }
            Ok(Err(e)) | Err(e) => {
                resp.header = Some(error::build_err_header(e));
            }
        };

        REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .get_table_info
            .observe(begin_instant.saturating_elapsed().as_secs_f64());
        Ok(Response::new(resp))
    }

    async fn write_batch_internal(
        &self,
        request: Request<WriteBatchRequest>,
    ) -> std::result::Result<Response<WriteResponse>, Status> {
        let begin_instant = Instant::now();
        let request = request.into_inner();
        let mut write_table_handles = Vec::with_capacity(request.batch.len());
        for one_request in request.batch {
            let ctx = self.handler_ctx();
            let handle = self
                .runtimes
                .write_runtime
                .spawn(handle_write(ctx, one_request));
            write_table_handles.push(handle);
        }

        let mut batch_resp = WriteResponse {
            header: Some(error::build_ok_header()),
            affected_rows: 0,
        };
        for write_handle in write_table_handles {
            let write_result = write_handle.await.box_err().context(ErrWithCause {
                code: StatusCode::Internal,
                msg: "fail to run the join task",
            });
            // The underlying write can't be cancelled, so just ignore the left write
            // handles (don't abort them) if any error is encountered.
            match write_result {
                Ok(res) => match res {
                    Ok(resp) => batch_resp.affected_rows += resp.affected_rows,
                    Err(e) => {
                        error!("Failed to write batches, err:{e}");
                        batch_resp.header = Some(error::build_err_header(e));
                        break;
                    }
                },
                Err(e) => {
                    error!("Failed to write batches, err:{e}");
                    batch_resp.header = Some(error::build_err_header(e));
                    break;
                }
            };
        }

        REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .write_batch
            .observe(begin_instant.saturating_elapsed().as_secs_f64());

        Ok(Response::new(batch_resp))
    }

    fn handler_ctx(&self) -> HandlerContext {
        HandlerContext {
            catalog_manager: self.instance.catalog_manager.clone(),
        }
    }
}

/// Context for handling all kinds of remote engine service.
#[derive(Clone)]
struct HandlerContext {
    catalog_manager: ManagerRef,
}

#[async_trait]
impl<Q: QueryExecutor + 'static> RemoteEngineService for RemoteEngineServiceImpl<Q> {
    type ReadStream = BoxStream<'static, std::result::Result<ReadResponse, Status>>;

    async fn read(
        &self,
        request: Request<ReadRequest>,
    ) -> std::result::Result<Response<Self::ReadStream>, Status> {
        REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC.stream_query.inc();
        match self.stream_read_internal(request).await {
            Ok(stream) => {
                let new_stream: Self::ReadStream = Box::pin(stream.map(|res| match res {
                    Ok(record_batch) => {
                        let resp = match ipc::encode_record_batch(
                            &record_batch.into_arrow_record_batch(),
                            CompressOptions {
                                compress_min_length: DEFAULT_COMPRESS_MIN_LENGTH,
                                method: CompressionMethod::Zstd,
                            },
                        )
                        .box_err()
                        .context(ErrWithCause {
                            code: StatusCode::Internal,
                            msg: "encode record batch failed",
                        }) {
                            Err(e) => ReadResponse {
                                header: Some(error::build_err_header(e)),
                                ..Default::default()
                            },
                            Ok(CompressOutput { payload, method }) => {
                                let compression = match method {
                                    CompressionMethod::None => arrow_payload::Compression::None,
                                    CompressionMethod::Zstd => arrow_payload::Compression::Zstd,
                                };

                                ReadResponse {
                                    header: Some(error::build_ok_header()),
                                    output: Some(Arrow(ArrowPayload {
                                        record_batches: vec![payload],
                                        compression: compression as i32,
                                    })),
                                }
                            }
                        };

                        Ok(resp)
                    }
                    Err(e) => {
                        let resp = ReadResponse {
                            header: Some(error::build_err_header(e)),
                            ..Default::default()
                        };
                        Ok(resp)
                    }
                }));

                Ok(Response::new(new_stream))
            }
            Err(e) => {
                let resp = ReadResponse {
                    header: Some(error::build_err_header(e)),
                    ..Default::default()
                };
                let stream = stream::once(async { Ok(resp) });
                Ok(Response::new(Box::pin(stream)))
            }
        }
    }

    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> std::result::Result<Response<WriteResponse>, Status> {
        self.write_internal(request).await
    }

    async fn get_table_info(
        &self,
        request: Request<GetTableInfoRequest>,
    ) -> std::result::Result<Response<GetTableInfoResponse>, Status> {
        self.get_table_info_internal(request).await
    }

    async fn write_batch(
        &self,
        request: Request<WriteBatchRequest>,
    ) -> std::result::Result<Response<WriteResponse>, Status> {
        self.write_batch_internal(request).await
    }
}

async fn handle_stream_read(
    ctx: HandlerContext,
    request: ReadRequest,
) -> Result<PartitionedStreams> {
    let table_engine::remote::model::ReadRequest {
        table: table_ident,
        read_request,
    } = request.try_into().box_err().context(ErrWithCause {
        code: StatusCode::BadRequest,
        msg: "fail to convert read request",
    })?;

    let request_id = read_request.request_id;
    info!(
        "Handle stream read, request_id:{request_id}, table:{table_ident:?}, read_options:{:?}, read_order:{:?}, predicate:{:?} ",
        read_request.opts,
        read_request.order,
        read_request.predicate,
    );

    let begin = Instant::now();
    let table = find_table_by_identifier(&ctx, &table_ident)?;
    let res = table
        .partitioned_read(read_request)
        .await
        .box_err()
        .with_context(|| ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to read table, table:{table_ident:?}"),
        });
    match res {
        Ok(streams) => {
            info!(
        "Handle stream read success, request_id:{request_id}, table:{table_ident:?}, cost:{:?}",
        begin.elapsed(),
    );
            REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC
                .stream_query_succeeded
                .inc();
            Ok(streams)
        }
        Err(e) => {
            REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC
                .stream_query_failed
                .inc();
            Err(e)
        }
    }
}

async fn handle_write(ctx: HandlerContext, request: WriteRequest) -> Result<WriteResponse> {
    let write_request: table_engine::remote::model::WriteRequest =
        request.try_into().box_err().context(ErrWithCause {
            code: StatusCode::BadRequest,
            msg: "fail to convert write request",
        })?;

    let num_rows = write_request.write_request.row_group.num_rows();
    let table = find_table_by_identifier(&ctx, &write_request.table)?;

    let res = table
        .write(write_request.write_request)
        .await
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to write table, table:{:?}", write_request.table),
        });
    match res {
        Ok(affected_rows) => {
            REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC.write_succeeded.inc();
            REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC
                .write_succeeded_row
                .inc_by(affected_rows as u64);
            Ok(WriteResponse {
                header: None,
                affected_rows: affected_rows as u64,
            })
        }
        Err(e) => {
            REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC.write_failed.inc();
            REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC
                .write_failed_row
                .inc_by(num_rows as u64);
            Err(e)
        }
    }
}

async fn handle_get_table_info(
    ctx: HandlerContext,
    request: GetTableInfoRequest,
) -> Result<GetTableInfoResponse> {
    let request: table_engine::remote::model::GetTableInfoRequest =
        request.try_into().box_err().context(ErrWithCause {
            code: StatusCode::BadRequest,
            msg: "fail to convert get table info request",
        })?;

    let schema = find_schema_by_identifier(&ctx, &request.table)?;
    let table = schema
        .table_by_name(&request.table.table)
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to get table, table:{}", request.table.table),
        })?
        .context(ErrNoCause {
            code: StatusCode::NotFound,
            msg: format!("table is not found, table:{}", request.table.table),
        })?;

    Ok(GetTableInfoResponse {
        header: None,
        table_info: Some(ceresdbproto::remote_engine::TableInfo {
            catalog_name: request.table.catalog,
            schema_name: schema.name().to_string(),
            schema_id: schema.id().as_u32(),
            table_name: table.name().to_string(),
            table_id: table.id().as_u64(),
            table_schema: Some((&table.schema()).into()),
            engine: table.engine_type().to_string(),
            options: table.options(),
            partition_info: table.partition_info().map(Into::into),
        }),
    })
}

fn find_table_by_identifier(
    ctx: &HandlerContext,
    table_identifier: &TableIdentifier,
) -> Result<TableRef> {
    let schema = find_schema_by_identifier(ctx, table_identifier)?;

    schema
        .table_by_name(&table_identifier.table)
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to get table, table:{}", table_identifier.table),
        })?
        .context(ErrNoCause {
            code: StatusCode::NotFound,
            msg: format!("table is not found, table:{}", table_identifier.table),
        })
}

fn find_schema_by_identifier(
    ctx: &HandlerContext,
    table_identifier: &TableIdentifier,
) -> Result<SchemaRef> {
    let catalog = ctx
        .catalog_manager
        .catalog_by_name(&table_identifier.catalog)
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to get catalog, catalog:{}", table_identifier.catalog),
        })?
        .context(ErrNoCause {
            code: StatusCode::NotFound,
            msg: format!("catalog is not found, catalog:{}", table_identifier.catalog),
        })?;
    catalog
        .schema_by_name(&table_identifier.schema)
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: format!(
                "fail to get schema of table, schema:{}",
                table_identifier.schema
            ),
        })?
        .context(ErrNoCause {
            code: StatusCode::NotFound,
            msg: format!(
                "schema of table is not found, schema:{}",
                table_identifier.schema
            ),
        })
}
