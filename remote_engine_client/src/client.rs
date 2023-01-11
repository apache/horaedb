// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Client for accessing remote table engine

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use arrow_ext::ipc;
use common_types::{
    projected_schema::ProjectedSchema, record_batch::RecordBatch, schema::RecordSchema,
    RemoteEngineVersion,
};
use futures::{Stream, StreamExt};
use proto::remote_engine::{self, remote_engine_service_client::*};
use router::RouterRef;
use snafu::ResultExt;
use table_engine::remote::model::{ReadRequest, TableIdentifier, WriteRequest};
use tonic::{transport::Channel, Request, Streaming};

use crate::{cached_router::CachedRouter, config::Config, error::*, status_code};

pub struct Client {
    cached_router: CachedRouter,
}

impl Client {
    pub fn new(config: Config, router: RouterRef) -> Self {
        let cached_router = CachedRouter::new(router, config);

        Self { cached_router }
    }

    pub async fn read(&self, request: ReadRequest) -> Result<ClientReadRecordBatchStream> {
        // Find the channel from router firstly.
        let channel = self.cached_router.route(&request.table).await?;

        // Read from remote.
        let table_ident = request.table.clone();
        let projected_schema = request.read_request.projected_schema.clone();
        let mut rpc_client = RemoteEngineServiceClient::<Channel>::new(channel);
        let request_pb = proto::remote_engine::ReadRequest::try_from(request)
            .map_err(|e| Box::new(e) as _)
            .context(ConvertReadRequest {
                msg: "convert to pb failed",
            })?;

        let result = rpc_client
            .read(Request::new(request_pb))
            .await
            .with_context(|| Rpc {
                table_ident: table_ident.clone(),
                msg: "read from remote failed",
            });

        let response = match result {
            Ok(response) => response,

            Err(e) => {
                // If occurred error, we simply evict the corresponding channel now.
                // TODO: evict according to the type of error.
                self.cached_router.evict(&table_ident).await;
                return Err(e);
            }
        };

        let response = response.into_inner();
        let remote_read_record_batch_stream =
            ClientReadRecordBatchStream::new(table_ident, response, projected_schema);

        Ok(remote_read_record_batch_stream)
    }

    pub async fn write(&self, request: WriteRequest) -> Result<usize> {
        // Find the channel from router firstly.
        let channel = self.cached_router.route(&request.table).await?;

        // Write to remote.
        let table_ident = request.table.clone();

        let request_pb = proto::remote_engine::WriteRequest::try_from(request)
            .map_err(|e| Box::new(e) as _)
            .context(ConvertWriteRequest {
                msg: "convert to pb failed",
            })?;
        let mut rpc_client = RemoteEngineServiceClient::<Channel>::new(channel);

        let result = rpc_client
            .write(Request::new(request_pb))
            .await
            .with_context(|| Rpc {
                table_ident: table_ident.clone(),
                msg: "write to remote failed",
            });

        let response = match result {
            Ok(response) => response,

            Err(e) => {
                // If occurred error, we simply evict the corresponding channel now.
                // TODO: evict according to the type of error.
                self.cached_router.evict(&table_ident).await;
                return Err(e);
            }
        };

        let response = response.into_inner();
        if let Some(header) = response.header && !status_code::is_ok(header.code) {
            Server {
                table_ident: table_ident.clone(),
                code: header.code,
                msg: header.error,
            }.fail()
        } else {
            Ok(response.affected_rows as usize)
        }
    }
}

pub struct ClientReadRecordBatchStream {
    pub table_ident: TableIdentifier,
    pub response_stream: Streaming<remote_engine::ReadResponse>,
    pub projected_schema: ProjectedSchema,
    pub projected_record_schema: RecordSchema,
}

impl ClientReadRecordBatchStream {
    pub fn new(
        table_ident: TableIdentifier,
        response_stream: Streaming<remote_engine::ReadResponse>,
        projected_schema: ProjectedSchema,
    ) -> Self {
        let projected_record_schema = projected_schema.to_record_schema();
        Self {
            table_ident,
            response_stream,
            projected_schema,
            projected_record_schema,
        }
    }
}

impl Stream for ClientReadRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.response_stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(response))) => {
                // Check header.
                if let Some(header) = response.header && !status_code::is_ok(header.code) {
                    return Poll::Ready(Some(Server {
                        table_ident: this.table_ident.clone(),
                        code: header.code,
                        msg: header.error,
                    }.fail()));
                }

                let record_batch = match RemoteEngineVersion::try_from(response.version)
                    .context(ConvertVersion)?
                {
                    RemoteEngineVersion::ArrowIPCWithZstd => {
                        ipc::decode_record_batch(response.rows, ipc::Compression::Zstd)
                            .map_err(|e| Box::new(e) as _)
                            .context(ConvertReadResponse {
                                msg: "decode record batch failed",
                                version: response.version,
                            })
                            .and_then(|v| {
                                RecordBatch::try_from(v)
                                    .map_err(|e| Box::new(e) as _)
                                    .context(ConvertReadResponse {
                                        msg: "convert record batch failed",
                                        version: response.version,
                                    })
                            })
                    }
                };

                Poll::Ready(Some(record_batch))
            }

            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e).context(Rpc {
                table_ident: this.table_ident.clone(),
                msg: "poll read response failed",
            }))),

            Poll::Ready(None) => Poll::Ready(None),

            Poll::Pending => Poll::Pending,
        }
    }
}
