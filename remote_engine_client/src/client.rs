// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Client for accessing remote table engine

use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow_ext::{ipc, ipc::CompressionMethod};
use ceresdbproto::{
    remote_engine::{self, read_response::Output::Arrow, remote_engine_service_client::*},
    storage::arrow_payload,
};
use common_types::{
    projected_schema::ProjectedSchema, record_batch::RecordBatch, schema::RecordSchema,
};
use common_util::{error::BoxError, runtime::Runtime};
use futures::{future::join_all, Stream, StreamExt};
use router::RouterRef;
use snafu::{ensure, OptionExt, ResultExt};
use table_engine::{
    remote::model::{
        GetTableInfoRequest, ReadRequest, TableIdentifier, TableInfo, WriteBatchRequest,
        WriteBatchResult, WriteRequest,
    },
    table::{SchemaId, TableId},
};
use tonic::{transport::Channel, Request, Streaming};

use crate::{cached_router::CachedRouter, config::Config, error::*, status_code};

struct WriteBatchContext {
    table_idents: Vec<TableIdentifier>,
    request: WriteBatchRequest,
    channel: Channel,
}

pub struct Client {
    cached_router: Arc<CachedRouter>,
    worker_runtime: Arc<Runtime>,
}

impl Client {
    pub fn new(config: Config, router: RouterRef, worker_runtime: Arc<Runtime>) -> Self {
        let cached_router = CachedRouter::new(router, config);

        Self {
            cached_router: Arc::new(cached_router),
            worker_runtime,
        }
    }

    pub async fn read(&self, request: ReadRequest) -> Result<ClientReadRecordBatchStream> {
        // Find the channel from router firstly.
        let channel = self.cached_router.route(&request.table).await?;

        // Read from remote.
        let table_ident = request.table.clone();
        let projected_schema = request.read_request.projected_schema.clone();
        let mut rpc_client = RemoteEngineServiceClient::<Channel>::new(channel.channel_inner);
        let request_pb = ceresdbproto::remote_engine::ReadRequest::try_from(request)
            .box_err()
            .context(Convert {
                msg: "Failed to convert ReadRequest to pb",
            })?;

        let result = rpc_client
            .read(Request::new(request_pb))
            .await
            .with_context(|| Rpc {
                msg: "Failed to read from remote engine",
            });

        let response = match result {
            Ok(response) => response,

            Err(e) => {
                // If occurred error, we simply evict the corresponding channel now.
                // TODO: evict according to the type of error.
                self.cached_router.evict(&[table_ident]).await;
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

        let request_pb = ceresdbproto::remote_engine::WriteRequest::try_from(request)
            .box_err()
            .context(Convert {
                msg: "Failed to convert WriteRequest to pb",
            })?;
        let mut rpc_client = RemoteEngineServiceClient::<Channel>::new(channel.channel_inner);

        let result = rpc_client
            .write(Request::new(request_pb))
            .await
            .with_context(|| Rpc {
                msg: "Failed to write to remote engine",
            });

        let response = match result {
            Ok(response) => response,

            Err(e) => {
                // If occurred error, we simply evict the corresponding channel now.
                // TODO: evict according to the type of error.
                self.cached_router.evict(&[table_ident]).await;
                return Err(e);
            }
        };

        let response = response.into_inner();
        if let Some(header) = response.header && !status_code::is_ok(header.code) {
            Server {
                code: header.code,
                msg: header.error,
            }.fail()
        } else {
            Ok(response.affected_rows as usize)
        }
    }

    pub async fn write_batch(&self, requests: Vec<WriteRequest>) -> Result<Vec<WriteBatchResult>> {
        // Find the channels from router firstly.
        let mut write_batch_contexts_by_endpoint = HashMap::new();
        for request in requests {
            let channel = self.cached_router.route(&request.table).await?;
            let write_batch_context = write_batch_contexts_by_endpoint
                .entry(channel.endpoint)
                .or_insert(WriteBatchContext {
                    table_idents: Vec::new(),
                    request: WriteBatchRequest::default(),
                    channel: channel.channel_inner,
                });
            write_batch_context.table_idents.push(request.table.clone());
            write_batch_context.request.batch.push(request);
        }

        // Merge according to endpoint.
        let mut remote_writes = Vec::with_capacity(write_batch_contexts_by_endpoint.len());
        let mut written_tables = Vec::with_capacity(write_batch_contexts_by_endpoint.len());
        for (_, context) in write_batch_contexts_by_endpoint {
            // Write to remote.
            let WriteBatchContext {
                table_idents,
                request,
                channel,
            } = context;
            let handle = self.worker_runtime.spawn(async move {
                let batch_request_pb =
                    match ceresdbproto::remote_engine::WriteBatchRequest::try_from(request)
                        .box_err()
                    {
                        Ok(pb) => pb,
                        Err(e) => return Err(e),
                    };

                let mut rpc_client = RemoteEngineServiceClient::<Channel>::new(channel);
                let rpc_result = rpc_client
                    .write_batch(Request::new(batch_request_pb))
                    .await
                    .box_err();

                rpc_result
            });

            remote_writes.push(handle);
            written_tables.push(table_idents);
        }

        let remote_write_results = join_all(remote_writes).await;
        let mut results = Vec::with_capacity(remote_write_results.len());
        for (table_idents, batch_result) in written_tables.into_iter().zip(remote_write_results) {
            // If it's runtime error, don't evict entires from route cache.
            let remote_write_result = match batch_result.box_err() {
                Ok(result) => result,
                Err(e) => {
                    results.push(WriteBatchResult {
                        table_idents,
                        result: Err(e),
                    });
                    continue;
                }
            };

            // Check remote write result then.
            let remote_write_response = match remote_write_result {
                Ok(response) => response,

                Err(e) => {
                    // If occurred error of remote write, we simply evict the corresponding channel
                    // now. TODO: evict according to the type of error.
                    self.cached_router.evict(&table_idents).await;
                    results.push(WriteBatchResult {
                        table_idents,
                        result: Err(e),
                    });
                    continue;
                }
            };

            // Check response finally.
            let remote_write_response = remote_write_response.into_inner();
            if let Some(header) = remote_write_response.header && !status_code::is_ok(header.code) {
                let err = Server {
                    code: header.code,
                    msg: header.error,
                }.fail().box_err();
                results.push(WriteBatchResult {
                    table_idents,
                    result: err,
                });
            } else {
                results.push(WriteBatchResult {
                    table_idents,
                    result: Ok(remote_write_response.affected_rows),
                });
            }
        }
        Ok(results)
    }

    pub async fn get_table_info(&self, request: GetTableInfoRequest) -> Result<TableInfo> {
        // Find the channel from router firstly.
        let channel = self.cached_router.route(&request.table).await?;
        let table_ident = request.table.clone();
        let request_pb = ceresdbproto::remote_engine::GetTableInfoRequest::try_from(request)
            .box_err()
            .context(Convert {
                msg: "Failed to convert GetTableInfoRequest to pb",
            })?;

        let mut rpc_client = RemoteEngineServiceClient::<Channel>::new(channel.channel_inner);

        let result = rpc_client
            .get_table_info(Request::new(request_pb))
            .await
            .with_context(|| Rpc {
                msg: "Failed to get table info",
            });

        let response = match result {
            Ok(response) => response,
            Err(e) => {
                // If occurred error, we simply evict the corresponding channel now.
                // TODO: evict according to the type of error.
                self.cached_router.evict(&[table_ident]).await;
                return Err(e);
            }
        };

        let response = response.into_inner();
        if let Some(header) = response.header && !status_code::is_ok(header.code) {
            Server {
                code: header.code,
                msg: header.error,
            }.fail()
        } else {
            let table_info = response.table_info.context(Server {
                code: status_code::StatusCode::Internal.as_u32(),
                msg: "Table info is empty",
            })?;

            Ok(TableInfo {
                catalog_name: table_info.catalog_name,
                schema_name: table_info.schema_name,
                schema_id: SchemaId::from(table_info.schema_id),
                table_name: table_info.table_name,
                table_id: TableId::from(table_info.table_id),
                table_schema: table_info.table_schema.map(TryInto::try_into).transpose().box_err()
                    .context(Convert { msg: "Failed to covert table schema" })?
                    .context(Server {
                        code: status_code::StatusCode::Internal.as_u32(),
                        msg: "Table schema is empty",
                    })?,
                engine: table_info.engine,
                options: table_info.options,
                partition_info: table_info.partition_info.map(TryInto::try_into).transpose().box_err()
                    .context(Convert { msg: "Failed to covert partition info" })?,
            })
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
                        code: header.code,
                        msg: header.error,
                    }.fail()));
                }

                match response.output {
                    None => Poll::Ready(None),
                    Some(v) => {
                        let record_batch = match v {
                            Arrow(mut v) => {
                                if v.record_batches.len() != 1 {
                                    return Poll::Ready(Some(
                                        InvalidRecordBatchNumber {
                                            batch_num: v.record_batches.len(),
                                        }
                                        .fail(),
                                    ));
                                }

                                let compression = match v.compression() {
                                    arrow_payload::Compression::None => CompressionMethod::None,
                                    arrow_payload::Compression::Zstd => CompressionMethod::Zstd,
                                };

                                ipc::decode_record_batches(
                                    v.record_batches.swap_remove(0),
                                    compression,
                                )
                                .map_err(|e| Box::new(e) as _)
                                .context(Convert {
                                    msg: "decode read record batch",
                                })
                                .and_then(
                                    |mut record_batch_vec| {
                                        ensure!(
                                            record_batch_vec.len() == 1,
                                            InvalidRecordBatchNumber {
                                                batch_num: record_batch_vec.len()
                                            }
                                        );
                                        record_batch_vec
                                            .swap_remove(0)
                                            .try_into()
                                            .map_err(|e| Box::new(e) as _)
                                            .context(Convert {
                                                msg: "convert read record batch",
                                            })
                                    },
                                )
                            }
                        };
                        Poll::Ready(Some(record_batch))
                    }
                }
            }

            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e).context(Rpc {
                msg: "poll read response",
            }))),

            Poll::Ready(None) => Poll::Ready(None),

            Poll::Pending => Poll::Pending,
        }
    }
}
