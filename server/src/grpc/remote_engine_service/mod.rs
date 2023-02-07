// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// Remote engine rpc service implementation.

use std::{sync::Arc, time::Instant};

use arrow_ext::ipc::{self, CompressOptions, CompressOutput};
use async_trait::async_trait;
use catalog::manager::ManagerRef;
use common_types::{record_batch::RecordBatch, RemoteEngineVersion};
use common_util::time::InstantExt;
use futures::stream::{self, BoxStream, StreamExt};
use log::error;
use proto::remote_engine::{
    remote_engine_service_server::RemoteEngineService, ReadRequest, ReadResponse, WriteRequest,
    WriteResponse,
};
use query_engine::executor::Executor as QueryExecutor;
use snafu::{OptionExt, ResultExt};
use table_engine::{
    engine::EngineRuntimes, remote::model::TableIdentifier, stream::PartitionedStreams,
    table::TableRef,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::{
    grpc::{
        metrics::REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC,
        remote_engine_service::error::{
            build_ok_header, ErrNoCause, ErrWithCause, Result, StatusCode,
        },
    },
    instance::InstanceRef,
};

pub(crate) mod error;

const STREAM_QUERY_CHANNEL_LEN: usize = 20;

#[derive(Clone)]
pub struct RemoteEngineServiceImpl<Q: QueryExecutor + 'static> {
    pub instance: InstanceRef<Q>,
    pub runtimes: Arc<EngineRuntimes>,
}

impl<Q: QueryExecutor + 'static> RemoteEngineServiceImpl<Q> {
    async fn stream_read_internal(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<ReceiverStream<Result<RecordBatch>>> {
        let instant = Instant::now();
        let ctx = self.handler_ctx();
        let (tx, rx) = mpsc::channel(STREAM_QUERY_CHANNEL_LEN);
        let handle = self.runtimes.read_runtime.spawn(async move {
            let read_request = request.into_inner();
            handle_stream_read(ctx, read_request).await
        });
        let streams = handle
            .await
            .map_err(|e| Box::new(e) as _)
            .context(ErrWithCause {
                code: StatusCode::Internal,
                msg: "fail to join task",
            })??;

        for stream in streams.streams {
            let mut stream = stream.map(|result| {
                result.map_err(|e| Box::new(e) as _).context(ErrWithCause {
                    code: StatusCode::Internal,
                    msg: "record batch failed",
                })
            });
            let tx = tx.clone();
            let _ = self.runtimes.read_runtime.spawn(async move {
                while let Some(batch) = stream.next().await {
                    if let Err(e) = tx.send(batch).await {
                        error!("Failed to send handler result, err:{}.", e);
                        break;
                    }
                }
            });
        }

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

        let res = handle
            .await
            .map_err(|e| Box::new(e) as _)
            .context(ErrWithCause {
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

    fn handler_ctx(&self) -> HandlerContext {
        HandlerContext {
            catalog_manager: self.instance.catalog_manager.clone(),
        }
    }
}

/// Context for handling all kinds of remote engine service.
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
        match self.stream_read_internal(request).await {
            Ok(stream) => {
                let new_stream: Self::ReadStream = Box::pin(stream.map(|res| match res {
                    Ok(record_batch) => {
                        let resp = match ipc::encode_record_batch(
                            &record_batch.into_arrow_record_batch(),
                            // TODO: Set compress_min_size to 0 for now, we should set it to a
                            // proper value.
                            CompressOptions {
                                compress_min_length: 0,
                                method: ipc::CompressionMethod::Zstd,
                            },
                        )
                        .map_err(|e| Box::new(e) as _)
                        .context(ErrWithCause {
                            code: StatusCode::Internal,
                            msg: "encode record batch failed",
                        }) {
                            Err(e) => ReadResponse {
                                header: Some(error::build_err_header(e)),
                                ..Default::default()
                            },
                            Ok(CompressOutput { payload, .. }) => ReadResponse {
                                header: Some(build_ok_header()),
                                version: RemoteEngineVersion::ArrowIPCWithZstd.as_u32(),
                                rows: payload,
                            },
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

                Ok(tonic::Response::new(new_stream))
            }
            Err(e) => {
                let resp = ReadResponse {
                    header: Some(error::build_err_header(e)),
                    ..Default::default()
                };
                let stream = stream::once(async { Ok(resp) });
                Ok(tonic::Response::new(Box::pin(stream)))
            }
        }
    }

    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> std::result::Result<Response<WriteResponse>, Status> {
        self.write_internal(request).await
    }
}

async fn handle_stream_read(
    ctx: HandlerContext,
    request: ReadRequest,
) -> Result<PartitionedStreams> {
    let read_request: table_engine::remote::model::ReadRequest = request
        .try_into()
        .map_err(|e| Box::new(e) as _)
        .context(ErrWithCause {
            code: StatusCode::BadRequest,
            msg: "fail to convert read request",
        })?;

    let table = find_table_by_identifier(&ctx, &read_request.table)?;

    let streams = table
        .partitioned_read(read_request.read_request)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to read table, table:{:?}", read_request.table),
        })?;

    Ok(streams)
}

async fn handle_write(ctx: HandlerContext, request: WriteRequest) -> Result<WriteResponse> {
    let write_request: table_engine::remote::model::WriteRequest = request
        .try_into()
        .map_err(|e| Box::new(e) as _)
        .context(ErrWithCause {
            code: StatusCode::BadRequest,
            msg: "fail to convert write request",
        })?;

    let table = find_table_by_identifier(&ctx, &write_request.table)?;

    let affected_rows = table
        .write(write_request.write_request)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to write table, table:{:?}", write_request.table),
        })?;
    Ok(WriteResponse {
        header: None,
        affected_rows: affected_rows as u64,
    })
}

fn find_table_by_identifier(
    ctx: &HandlerContext,
    table_identifier: &TableIdentifier,
) -> Result<TableRef> {
    let catalog = ctx
        .catalog_manager
        .catalog_by_name(&table_identifier.catalog)
        .map_err(|e| Box::new(e) as _)
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to get catalog, catalog:{}", table_identifier.catalog),
        })?
        .context(ErrNoCause {
            code: StatusCode::NotFound,
            msg: format!("catalog is not found, catalog:{}", table_identifier.catalog),
        })?;
    let schema = catalog
        .schema_by_name(&table_identifier.schema)
        .map_err(|e| Box::new(e) as _)
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
        })?;

    schema
        .table_by_name(&table_identifier.table)
        .map_err(|e| Box::new(e) as _)
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to get table, table:{}", table_identifier.table),
        })?
        .context(ErrNoCause {
            code: StatusCode::NotFound,
            msg: format!("table is not found, table:{}", table_identifier.table),
        })
}
