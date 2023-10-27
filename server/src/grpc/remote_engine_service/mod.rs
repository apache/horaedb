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

// Remote engine rpc service implementation.

use std::{
    hash::Hash,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use arrow_ext::ipc::{self, CompressOptions, CompressOutput, CompressionMethod};
use async_trait::async_trait;
use catalog::{manager::ManagerRef, schema::SchemaRef};
use ceresdbproto::{
    remote_engine::{
        execute_plan_request, read_response::Output::Arrow,
        remote_engine_service_server::RemoteEngineService, row_group, AlterTableOptionsRequest,
        AlterTableOptionsResponse, AlterTableSchemaRequest, AlterTableSchemaResponse, ExecContext,
        ExecutePlanRequest, GetTableInfoRequest, GetTableInfoResponse, ReadRequest, ReadResponse,
        WriteBatchRequest, WriteRequest, WriteResponse,
    },
    storage::{arrow_payload, ArrowPayload},
};
use common_types::{record_batch::RecordBatch, request_id::RequestId};
use futures::{
    stream::{self, BoxStream, FuturesUnordered, StreamExt},
    Future,
};
use generic_error::BoxError;
use logger::{error, info};
use notifier::notifier::{ExecutionGuard, RequestNotifiers, RequestResult};
use proxy::{
    hotspot::{HotspotRecorder, Message},
    instance::InstanceRef,
};
use query_engine::{
    datafusion_impl::physical_plan::{DataFusionPhysicalPlanAdapter, TypedPlan},
    QueryEngineRef, QueryEngineType,
};
use snafu::{OptionExt, ResultExt};
use table_engine::{
    engine::EngineRuntimes,
    predicate::PredicateRef,
    remote::model::{self, TableIdentifier},
    stream::{PartitionedStreams, SendableRecordBatchStream},
    table::{AlterSchemaRequest, TableRef},
};
use time_ext::InstantExt;
use tokio::sync::mpsc::{self, Sender};
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::{Request, Response, Status};

use super::metrics::REMOTE_ENGINE_WRITE_BATCH_NUM_ROWS_HISTOGRAM;
use crate::{
    config::QueryDedupConfig,
    grpc::{
        metrics::{
            REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC,
            REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC,
        },
        remote_engine_service::error::{ErrNoCause, ErrWithCause, Result, StatusCode},
    },
};

pub mod error;

const STREAM_QUERY_CHANNEL_LEN: usize = 200;
const DEFAULT_COMPRESS_MIN_LENGTH: usize = 80 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct StreamReadReqKey {
    table: String,
    predicate: PredicateRef,
    projection: Option<Vec<usize>>,
}

impl StreamReadReqKey {
    pub fn new(table: String, predicate: PredicateRef, projection: Option<Vec<usize>>) -> Self {
        Self {
            table,
            predicate,
            projection,
        }
    }
}

pub type StreamReadRequestNotifiers =
    Arc<RequestNotifiers<StreamReadReqKey, mpsc::Sender<Result<RecordBatch>>>>;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PhysicalPlanKey {
    encoded_plan: Vec<u8>,
}

pub type PhysicalPlanNotifiers =
    Arc<RequestNotifiers<PhysicalPlanKey, mpsc::Sender<Result<RecordBatch>>>>;

/// Stream metric
trait MetricCollector: 'static + Send + Unpin {
    fn collect(self);
}

struct StreamReadMetricCollector(Instant);

impl MetricCollector for StreamReadMetricCollector {
    fn collect(self) {
        REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .stream_read
            .observe(self.0.saturating_elapsed().as_secs_f64());
    }
}

struct ExecutePlanMetricCollector(Instant);

impl MetricCollector for ExecutePlanMetricCollector {
    fn collect(self) {
        REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .execute_physical_plan
            .observe(self.0.saturating_elapsed().as_secs_f64());
    }
}

/// Stream with metric
struct StreamWithMetric<M: MetricCollector> {
    inner: BoxStream<'static, Result<RecordBatch>>,
    metric: Option<M>,
}

impl<M: MetricCollector> StreamWithMetric<M> {
    fn new(inner: BoxStream<'static, Result<RecordBatch>>, metric: M) -> Self {
        Self {
            inner,
            metric: Some(metric),
        }
    }
}

impl<M: MetricCollector> Stream for StreamWithMetric<M> {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().inner.poll_next_unpin(cx)
    }
}

impl<M: MetricCollector> Drop for StreamWithMetric<M> {
    fn drop(&mut self) {
        let metric = self.metric.take();
        if let Some(metric) = metric {
            metric.collect();
        }
    }
}

macro_rules! record_stream_to_response_stream {
    ($record_stream_result:ident, $StreamType:ident) => {
        match $record_stream_result {
            Ok(stream) => {
                let new_stream: Self::$StreamType = Box::pin(stream.map(|res| match res {
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
    };
}

#[derive(Clone)]
pub struct QueryDedup {
    pub config: QueryDedupConfig,
    pub request_notifiers: StreamReadRequestNotifiers,
    pub physical_plan_notifiers: PhysicalPlanNotifiers,
}

#[derive(Clone)]
pub struct RemoteEngineServiceImpl {
    pub instance: InstanceRef,
    pub runtimes: Arc<EngineRuntimes>,
    pub query_dedup: Option<QueryDedup>,
    pub hotspot_recorder: Arc<HotspotRecorder>,
}

impl RemoteEngineServiceImpl {
    async fn stream_read_internal(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<StreamWithMetric<StreamReadMetricCollector>> {
        let metric = StreamReadMetricCollector(Instant::now());

        let ctx = self.handler_ctx();
        let (tx, rx) = mpsc::channel(STREAM_QUERY_CHANNEL_LEN);
        let handle = self.runtimes.read_runtime.spawn(async move {
            let read_request = request.into_inner();
            handle_stream_read(ctx, read_request).await
        });
        let streams = handle.await.box_err().context(ErrWithCause {
            code: StatusCode::Internal,
            msg: "fail to join task",
        })??;

        for stream in streams.streams {
            let mut stream = stream.map(|result| {
                result.box_err().context(ErrWithCause {
                    code: StatusCode::Internal,
                    msg: "record batch failed",
                })
            });
            let tx = tx.clone();
            self.runtimes.read_runtime.spawn(async move {
                let mut num_rows = 0;
                while let Some(batch) = stream.next().await {
                    if let Ok(record_batch) = &batch {
                        num_rows += record_batch.num_rows();
                    }
                    if let Err(e) = tx.send(batch).await {
                        error!("Failed to send handler result, err:{}.", e);
                        break;
                    }
                }
                REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC
                    .query_succeeded_row
                    .inc_by(num_rows as u64);
            });
        }

        Ok(StreamWithMetric::new(
            Box::pin(ReceiverStream::new(rx)),
            metric,
        ))
    }

    async fn dedup_stream_read_internal(
        &self,
        query_dedup: QueryDedup,
        request: Request<ReadRequest>,
    ) -> Result<StreamWithMetric<StreamReadMetricCollector>> {
        let metric = StreamReadMetricCollector(Instant::now());

        let request = request.into_inner();
        let table_engine::remote::model::ReadRequest {
            table,
            read_request,
        } = request.clone().try_into().box_err().context(ErrWithCause {
            code: StatusCode::BadRequest,
            msg: "fail to convert read request",
        })?;

        let request_key = StreamReadReqKey::new(
            table.table.clone(),
            read_request.predicate.clone(),
            read_request.projected_schema.projection(),
        );

        let QueryDedup {
            config,
            request_notifiers,
            ..
        } = query_dedup;

        let (tx, rx) = mpsc::channel(config.notify_queue_cap);
        match request_notifiers.insert_notifier(request_key.clone(), tx) {
            // The first request, need to handle it, and then notify the other requests.
            RequestResult::First => {
                let ctx = self.handler_ctx();
                let query = async move { handle_stream_read(ctx, request).await };
                self.read_and_send_dedupped_resps(
                    request_key,
                    query,
                    request_notifiers.clone(),
                    config.notify_timeout.0,
                )
                .await?;
            }
            // The request is waiting for the result of first request.
            RequestResult::Wait => {
                // TODO: add metrics to collect the time cost of waited stream
                // read.
            }
        }

        Ok(StreamWithMetric::new(
            Box::pin(ReceiverStream::new(rx)),
            metric,
        ))
    }

    async fn read_and_send_dedupped_resps<K, F>(
        &self,
        request_key: K,
        query: F,
        notifiers: Arc<RequestNotifiers<K, mpsc::Sender<Result<RecordBatch>>>>,
        notify_timeout: Duration,
    ) -> Result<()>
    where
        K: Hash + PartialEq + Eq,
        F: Future<Output = Result<PartitionedStreams>> + Send + 'static,
    {
        // This is used to remove key when future is cancelled.
        let mut guard = ExecutionGuard::new(|| {
            notifiers.take_notifiers(&request_key);
        });
        let handle = self.runtimes.read_runtime.spawn(query);
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

        // Collect all the data from the stream to let more duplicate request query to
        // be batched.
        let mut resps = Vec::new();
        while let Some(result) = stream_read.next().await {
            let batch = result.box_err().context(ErrWithCause {
                code: StatusCode::Internal,
                msg: "failed to join task",
            })?;
            resps.extend(batch);
        }

        // We should set cancel to guard, otherwise the key will be removed twice.
        guard.cancel();
        let notifiers = notifiers.take_notifiers(&request_key).unwrap();

        // Do send in background to avoid blocking the rpc procedure.
        self.runtimes.read_runtime.spawn(async move {
            Self::send_dedupped_resps(resps, notifiers, notify_timeout).await;
        });

        Ok(())
    }

    /// Send the response to the queriers that share the same query request.
    async fn send_dedupped_resps(
        resps: Vec<Result<RecordBatch>>,
        notifiers: Vec<Sender<Result<RecordBatch>>>,
        notify_timeout: Duration,
    ) {
        let mut num_rows = 0;
        for resp in resps {
            match resp {
                Ok(batch) => {
                    num_rows += batch.num_rows();
                    for notifier in &notifiers {
                        if let Err(e) = notifier
                            .send_timeout(Ok(batch.clone()), notify_timeout)
                            .await
                        {
                            error!("Failed to send handler result, err:{}.", e);
                        }
                    }
                }
                Err(_) => {
                    for notifier in &notifiers {
                        let err = ErrNoCause {
                            code: StatusCode::Internal,
                            msg: "failed to handler request".to_string(),
                        }
                        .fail();

                        if let Err(e) = notifier.send_timeout(err, notify_timeout).await {
                            error!("Failed to send handler result, err:{}.", e);
                        }
                    }
                    break;
                }
            }
        }

        let total_num_rows = (num_rows * notifiers.len()) as u64;
        let num_dedupped_reqs = (notifiers.len() - 1) as u64;
        REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC
            .query_succeeded_row
            .inc_by(total_num_rows);
        REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC
            .dedupped_stream_query
            .inc_by(num_dedupped_reqs);
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

    async fn execute_physical_plan_internal(
        &self,
        request: Request<ExecutePlanRequest>,
    ) -> Result<StreamWithMetric<ExecutePlanMetricCollector>> {
        let metric = ExecutePlanMetricCollector(Instant::now());
        let request = request.into_inner();
        let query_engine = self.instance.query_engine.clone();
        let (ctx, encoded_plan) = extract_plan_from_req(request)?;

        let stream = self
            .runtimes
            .read_runtime
            .spawn(async move { handle_execute_plan(ctx, encoded_plan, query_engine).await })
            .await
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::Internal,
                msg: "failed to run execute physical plan task",
            })??
            .map(|result| {
                result.box_err().context(ErrWithCause {
                    code: StatusCode::Internal,
                    msg: "failed to poll record batch for remote physical plan",
                })
            });

        Ok(StreamWithMetric::new(Box::pin(stream), metric))
    }

    async fn dedup_execute_physical_plan_internal(
        &self,
        query_dedup: QueryDedup,
        request: Request<ExecutePlanRequest>,
    ) -> Result<StreamWithMetric<ExecutePlanMetricCollector>> {
        let metric = ExecutePlanMetricCollector(Instant::now());
        let request = request.into_inner();

        let query_engine = self.instance.query_engine.clone();
        let (ctx, encoded_plan) = extract_plan_from_req(request)?;
        let key = PhysicalPlanKey {
            encoded_plan: encoded_plan.clone(),
        };

        let QueryDedup {
            config,
            physical_plan_notifiers,
            ..
        } = query_dedup;

        let (tx, rx) = mpsc::channel(config.notify_queue_cap);
        match physical_plan_notifiers.insert_notifier(key.clone(), tx) {
            // The first request, need to handle it, and then notify the other requests.
            RequestResult::First => {
                let query = async move {
                    handle_execute_plan(ctx, encoded_plan, query_engine)
                        .await
                        .map(PartitionedStreams::one_stream)
                };
                self.read_and_send_dedupped_resps(
                    key,
                    query,
                    physical_plan_notifiers,
                    config.notify_timeout.0,
                )
                .await?;
            }
            // The request is waiting for the result of first request.
            RequestResult::Wait => {
                // TODO: add metrics to collect the time cost of waited stream
                // read.
            }
        }

        Ok(StreamWithMetric::new(
            Box::pin(ReceiverStream::new(rx)),
            metric,
        ))
    }

    async fn alter_table_schema_internal(
        &self,
        request: Request<AlterTableSchemaRequest>,
    ) -> std::result::Result<Response<AlterTableSchemaResponse>, Status> {
        let begin_instant = Instant::now();
        let ctx = self.handler_ctx();
        let handle = self.runtimes.read_runtime.spawn(async move {
            let request = request.into_inner();
            handle_alter_table_schema(ctx, request).await
        });

        let res = handle.await.box_err().context(ErrWithCause {
            code: StatusCode::Internal,
            msg: "fail to join task",
        });

        let mut resp = AlterTableSchemaResponse::default();
        match res {
            Ok(Ok(_)) => {
                resp.header = Some(error::build_ok_header());
            }
            Ok(Err(e)) | Err(e) => {
                resp.header = Some(error::build_err_header(e));
            }
        };

        REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .alter_table_schema
            .observe(begin_instant.saturating_elapsed().as_secs_f64());
        Ok(Response::new(resp))
    }

    async fn alter_table_options_internal(
        &self,
        request: Request<AlterTableOptionsRequest>,
    ) -> std::result::Result<Response<AlterTableOptionsResponse>, Status> {
        let begin_instant = Instant::now();
        let ctx = self.handler_ctx();
        let handle = self.runtimes.read_runtime.spawn(async move {
            let request = request.into_inner();
            handle_alter_table_options(ctx, request).await
        });

        let res = handle.await.box_err().context(ErrWithCause {
            code: StatusCode::Internal,
            msg: "fail to join task",
        });

        let mut resp = AlterTableOptionsResponse::default();
        match res {
            Ok(Ok(_)) => {
                resp.header = Some(error::build_ok_header());
            }
            Ok(Err(e)) | Err(e) => {
                resp.header = Some(error::build_err_header(e));
            }
        };

        REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .alter_table_options
            .observe(begin_instant.saturating_elapsed().as_secs_f64());
        Ok(Response::new(resp))
    }

    fn handler_ctx(&self) -> HandlerContext {
        HandlerContext {
            catalog_manager: self.instance.catalog_manager.clone(),
            hotspot_recorder: self.hotspot_recorder.clone(),
        }
    }
}

/// Context for handling all kinds of remote engine service.
#[derive(Clone)]
struct HandlerContext {
    catalog_manager: ManagerRef,
    hotspot_recorder: Arc<HotspotRecorder>,
}

#[async_trait]
impl RemoteEngineService for RemoteEngineServiceImpl {
    type ExecutePhysicalPlanStream = BoxStream<'static, std::result::Result<ReadResponse, Status>>;
    type ReadStream = BoxStream<'static, std::result::Result<ReadResponse, Status>>;

    async fn read(
        &self,
        request: Request<ReadRequest>,
    ) -> std::result::Result<Response<Self::ReadStream>, Status> {
        if let Some(table) = &request.get_ref().table {
            self.hotspot_recorder
                .send_msg_or_log(
                    "inc_query_reqs",
                    Message::Query(format_hot_key(&table.schema, &table.table)),
                )
                .await
        }

        REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC.stream_query.inc();
        let result = match self.query_dedup.clone() {
            Some(query_dedup) => self.dedup_stream_read_internal(query_dedup, request).await,
            None => self.stream_read_internal(request).await,
        };

        record_stream_to_response_stream!(result, ReadStream)
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

    async fn execute_physical_plan(
        &self,
        request: Request<ExecutePlanRequest>,
    ) -> std::result::Result<Response<Self::ExecutePhysicalPlanStream>, Status> {
        if let Some(table) = &request.get_ref().table {
            self.hotspot_recorder
                .send_msg_or_log(
                    "inc_remote_plan_reqs",
                    Message::Query(format_hot_key(&table.schema, &table.table)),
                )
                .await
        }

        let record_stream_result = match self.query_dedup.clone() {
            Some(query_dedup) => {
                self.dedup_execute_physical_plan_internal(query_dedup, request)
                    .await
            }
            None => self.execute_physical_plan_internal(request).await,
        };

        record_stream_to_response_stream!(record_stream_result, ExecutePhysicalPlanStream)
    }

    async fn alter_table_schema(
        &self,
        request: Request<AlterTableSchemaRequest>,
    ) -> std::result::Result<Response<AlterTableSchemaResponse>, Status> {
        self.alter_table_schema_internal(request).await
    }

    async fn alter_table_options(
        &self,
        request: Request<AlterTableOptionsRequest>,
    ) -> std::result::Result<Response<AlterTableOptionsResponse>, Status> {
        self.alter_table_options_internal(request).await
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
        "Handle stream read, request_id:{request_id}, table:{table_ident:?}, read_options:{:?}, predicate:{:?} ",
        read_request.opts,
        read_request.predicate,
    );

    let begin = Instant::now();
    let table = find_table_by_identifier(&ctx, &table_ident)?;
    let res = table
        .partitioned_read(read_request.clone())
        .await
        .box_err()
        .with_context(|| ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to read table, table:{table_ident:?}"),
        });
    match res {
        Ok(streams) => {
            info!(
                "Handle stream read success, request_id:{request_id}, table:{table_ident:?}, cost:{:?}, read_options:{:?}, predicate:{:?}",
                begin.elapsed(),
                read_request.opts,
                read_request.predicate,
            );

            REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC
                .stream_query_succeeded
                .inc();
            Ok(streams)
        }
        Err(e) => {
            error!(
                "Handle stream read failed, request_id:{request_id}, table:{table_ident:?}, cost:{:?}, read_options:{:?}, predicate:{:?}, err:{e}",
                begin.elapsed(),
                read_request.opts,
                read_request.predicate,
            );

            REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC
                .stream_query_failed
                .inc();
            Err(e)
        }
    }
}

fn format_hot_key(schema: &str, table: &str) -> String {
    format!("{schema}/{table}")
}

async fn record_write(
    hotspot_recorder: &Arc<HotspotRecorder>,
    request: &table_engine::remote::model::WriteRequest,
) {
    let hot_key = format_hot_key(&request.table.schema, &request.table.table);
    let row_count = request.write_request.row_group.num_rows();
    let field_count = row_count * request.write_request.row_group.schema().num_columns();
    hotspot_recorder
        .send_msg_or_log(
            "inc_write_reqs",
            Message::Write {
                key: hot_key,
                row_count,
                field_count,
            },
        )
        .await;
}

async fn handle_write(ctx: HandlerContext, request: WriteRequest) -> Result<WriteResponse> {
    let table_ident: TableIdentifier = request
        .table
        .context(ErrNoCause {
            code: StatusCode::BadRequest,
            msg: "missing table ident",
        })?
        .into();

    let rows_payload = request
        .row_group
        .context(ErrNoCause {
            code: StatusCode::BadRequest,
            msg: "missing row group payload",
        })?
        .rows
        .context(ErrNoCause {
            code: StatusCode::BadRequest,
            msg: "missing rows payload",
        })?;

    let table = find_table_by_identifier(&ctx, &table_ident)?;
    let write_request = match rows_payload {
        row_group::Rows::Arrow(_) => {
            // The payload encoded in arrow format won't be accept any more.
            return ErrNoCause {
                code: StatusCode::BadRequest,
                msg: "payload encoded in arrow format is not supported anymore",
            }
            .fail();
        }
        row_group::Rows::Contiguous(payload) => {
            let schema = table.schema();
            let row_group =
                model::WriteRequest::decode_row_group_from_contiguous_payload(payload, &schema)
                    .box_err()
                    .context(ErrWithCause {
                        code: StatusCode::BadRequest,
                        msg: "failed to decode row group payload",
                    })?;
            model::WriteRequest::new(table_ident, row_group)
        }
    };

    // In theory we should record write request we at the beginning of server's
    // handle, but the payload is encoded, so we cannot record until decode payload
    // here.
    record_write(&ctx.hotspot_recorder, &write_request).await;

    let num_rows = write_request.write_request.row_group.num_rows();
    REMOTE_ENGINE_WRITE_BATCH_NUM_ROWS_HISTOGRAM.observe(num_rows as f64);

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

fn extract_plan_from_req(request: ExecutePlanRequest) -> Result<(ExecContext, Vec<u8>)> {
    // Build execution context.
    let ctx_in_req = request.context.with_context(|| ErrNoCause {
        code: StatusCode::Internal,
        msg: "execution context not found in physical plan request",
    })?;
    let typed_plan_in_req = request.physical_plan.with_context(|| ErrNoCause {
        code: StatusCode::Internal,
        msg: "plan not found in physical plan request",
    })?;
    // FIXME: return the type from query engine.
    let valid_plan = check_and_extract_plan(typed_plan_in_req, QueryEngineType::Datafusion)?;

    Ok((ctx_in_req, valid_plan))
}

async fn handle_execute_plan(
    ctx: ExecContext,
    encoded_plan: Vec<u8>,
    query_engine: QueryEngineRef,
) -> Result<SendableRecordBatchStream> {
    let ExecContext {
        request_id,
        default_catalog,
        default_schema,
        timeout_ms,
    } = ctx;

    let request_id = RequestId::from(request_id);
    let deadline = if timeout_ms >= 0 {
        Some(Instant::now() + Duration::from_millis(timeout_ms as u64))
    } else {
        None
    };

    let exec_ctx = query_engine::context::Context {
        request_id,
        deadline,
        default_catalog,
        default_schema,
    };

    // TODO: Build remote plan in physical planner.
    let physical_plan = Box::new(DataFusionPhysicalPlanAdapter::new(TypedPlan::Remote(
        encoded_plan,
    )));

    // Execute plan.
    let executor = query_engine.executor();
    executor
        .execute(&exec_ctx, physical_plan)
        .await
        .box_err()
        .with_context(|| ErrWithCause {
            code: StatusCode::Internal,
            msg: "failed to execute remote plan",
        })
}

async fn handle_alter_table_schema(
    ctx: HandlerContext,
    request: AlterTableSchemaRequest,
) -> Result<()> {
    let request: table_engine::remote::model::AlterTableSchemaRequest =
        request.try_into().box_err().context(ErrWithCause {
            code: StatusCode::BadRequest,
            msg: "fail to convert alter table schema",
        })?;

    let schema = find_schema_by_identifier(&ctx, &request.table_ident)?;
    let table = schema
        .table_by_name(&request.table_ident.table)
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to get table, table:{}", request.table_ident.table),
        })?
        .context(ErrNoCause {
            code: StatusCode::NotFound,
            msg: format!("table is not found, table:{}", request.table_ident.table),
        })?;

    table
        .alter_schema(AlterSchemaRequest {
            schema: request.table_schema,
            pre_schema_version: request.pre_schema_version,
        })
        .await
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: format!(
                "fail to alter table schema, table:{}",
                request.table_ident.table
            ),
        })?;
    Ok(())
}

async fn handle_alter_table_options(
    ctx: HandlerContext,
    request: AlterTableOptionsRequest,
) -> Result<()> {
    let request: table_engine::remote::model::AlterTableOptionsRequest =
        request.try_into().box_err().context(ErrWithCause {
            code: StatusCode::BadRequest,
            msg: "fail to convert alter table options",
        })?;

    let schema = find_schema_by_identifier(&ctx, &request.table_ident)?;
    let table = schema
        .table_by_name(&request.table_ident.table)
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to get table, table:{}", request.table_ident.table),
        })?
        .context(ErrNoCause {
            code: StatusCode::NotFound,
            msg: format!("table is not found, table:{}", request.table_ident.table),
        })?;

    table
        .alter_options(request.options)
        .await
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: format!(
                "fail to alter table options, table:{}",
                request.table_ident.table
            ),
        })?;
    Ok(())
}

fn check_and_extract_plan(
    typed_plan: execute_plan_request::PhysicalPlan,
    engine_type: QueryEngineType,
) -> Result<Vec<u8>> {
    match (typed_plan, engine_type) {
        (execute_plan_request::PhysicalPlan::Datafusion(plan), QueryEngineType::Datafusion) => {
            Ok(plan)
        }
    }
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
