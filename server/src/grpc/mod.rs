// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Grpc services

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Instant,
};

use ceresdbproto_deps::ceresdbproto::{
    common::ResponseHeader,
    prometheus::{PrometheusQueryRequest, PrometheusQueryResponse},
    storage::{
        QueryRequest, QueryResponse, RouteRequest, RouteResponse, Value_oneof_value, WriteMetric,
        WriteRequest, WriteResponse,
    },
    storage_grpc::{self, StorageService},
};
use cluster::config::SchemaConfig;
use common_types::{
    column_schema::{self, ColumnSchema},
    datum::DatumKind,
    schema::{Builder as SchemaBuilder, Error as SchemaError, Schema, TSID_COLUMN},
};
use common_util::{define_result, time::InstantExt};
use futures::{stream::StreamExt, FutureExt, SinkExt, TryFutureExt};
use grpcio::{
    ClientStreamingSink, Environment, Metadata, RequestStream, RpcContext, Server, ServerBuilder,
    ServerStreamingSink, UnarySink, WriteFlags,
};
use log::{error, info};
use query_engine::executor::Executor as QueryExecutor;
use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};
use sql::plan::CreateTablePlan;
use table_engine::engine::EngineRuntimes;
use tokio::sync::oneshot;
use warp::http::StatusCode;

use crate::{
    consts,
    error::{ErrNoCause, ErrWithCause, Result as ServerResult, ServerError},
    grpc::metrics::GRPC_HANDLER_DURATION_HISTOGRAM_VEC,
    instance::InstanceRef,
    route::{Router, RouterRef},
    schema_config_provider::{self, SchemaConfigProviderRef},
};

mod metrics;
mod prom_query;
mod query;
mod route;
mod write;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to build rpc server, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    BuildRpcServer {
        source: grpcio::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Missing meta client config.\nBacktrace:\n{}", backtrace))]
    MissingMetaClientConfig { backtrace: Backtrace },

    #[snafu(display("Missing grpc environment.\nBacktrace:\n{}", backtrace))]
    MissingEnv { backtrace: Backtrace },

    #[snafu(display("Missing runtimes.\nBacktrace:\n{}", backtrace))]
    MissingRuntimes { backtrace: Backtrace },

    #[snafu(display("Missing instance.\nBacktrace:\n{}", backtrace))]
    MissingInstance { backtrace: Backtrace },

    #[snafu(display("Missing router.\nBacktrace:\n{}", backtrace))]
    MissingRouter { backtrace: Backtrace },

    #[snafu(display("Missing schema config provider.\nBacktrace:\n{}", backtrace))]
    MissingSchemaConfigProvider { backtrace: Backtrace },

    #[snafu(display("Catalog name is not utf8.\nBacktrace:\n{}", backtrace))]
    ParseCatalogName {
        source: std::string::FromUtf8Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Schema name is not utf8.\nBacktrace:\n{}", backtrace))]
    ParseSchemaName {
        source: std::string::FromUtf8Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Fail to build table schema for metric: {}, err:{}", metric, source))]
    BuildTableSchema { metric: String, source: SchemaError },

    #[snafu(display(
        "Fail to build column schema from column: {}, err:{}",
        column_name,
        source
    ))]
    BuildColumnSchema {
        column_name: String,
        source: column_schema::Error,
    },
    #[snafu(display("Invalid column: {} schema, err:{}", column_name, source))]
    InvalidColumnSchema {
        column_name: String,
        source: column_schema::Error,
    },

    #[snafu(display("Invalid argument: {}", msg))]
    InvalidArgument { msg: String },

    #[snafu(display(
        "Failed to send response to grpc sink, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    GrpcSink {
        source: grpcio::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Get schema config failed, err:{}", source))]
    GetSchemaConfig {
        source: schema_config_provider::Error,
    },
}

const STREAM_QUERY_CHANNEL_LEN: usize = 20;

define_result!(Error);

/// Rpc request header
#[derive(Debug, Default)]
pub struct RequestHeader {
    metas: HashMap<String, Vec<u8>>,
}

impl From<&Metadata> for RequestHeader {
    fn from(meta: &Metadata) -> Self {
        let metas = meta
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_vec()))
            .collect();

        Self { metas }
    }
}

impl RequestHeader {
    pub fn get(&self, key: &str) -> Option<&[u8]> {
        self.metas.get(key).map(|v| v.as_slice())
    }
}

pub struct HandlerContext<'a, Q> {
    #[allow(dead_code)]
    header: RequestHeader,
    router: RouterRef,
    instance: InstanceRef<Q>,
    catalog: String,
    schema: String,
    schema_config: Option<&'a SchemaConfig>,
}

impl<'a, Q> HandlerContext<'a, Q> {
    fn new(
        header: RequestHeader,
        router: Arc<dyn Router + Sync + Send>,
        instance: InstanceRef<Q>,
        schema_config_provider: &'a SchemaConfigProviderRef,
    ) -> Result<Self> {
        let default_catalog = instance.catalog_manager.default_catalog_name();
        let default_schema = instance.catalog_manager.default_schema_name();

        let catalog = header
            .get(consts::CATALOG_HEADER)
            .map(|v| String::from_utf8(v.to_vec()))
            .transpose()
            .context(ParseCatalogName)?
            .unwrap_or_else(|| default_catalog.to_string());

        let schema = header
            .get(consts::TENANT_HEADER)
            .map(|v| String::from_utf8(v.to_vec()))
            .transpose()
            .context(ParseSchemaName)?
            .unwrap_or_else(|| default_schema.to_string());

        let schema_config = schema_config_provider
            .schema_config(&schema)
            .context(GetSchemaConfig)?;

        Ok(Self {
            header,
            router,
            instance,
            catalog,
            schema,
            schema_config,
        })
    }

    #[inline]
    fn catalog(&self) -> &str {
        &self.catalog
    }

    #[inline]
    fn tenant(&self) -> &str {
        &self.schema
    }
}

/// Rpc services manages all grpc services of the server.
pub struct RpcServices {
    /// The grpc server
    rpc_server: Server,
}

impl RpcServices {
    /// Start the rpc services
    pub async fn start(&mut self) -> Result<()> {
        self.rpc_server.start();
        for (host, port) in self.rpc_server.bind_addrs() {
            info!("Grpc server listening on {}:{}", host, port);
        }

        Ok(())
    }

    pub fn shutdown(&mut self) {
        self.rpc_server.shutdown();
    }
}

pub struct Builder<Q> {
    bind_addr: String,
    port: u16,
    env: Option<Arc<Environment>>,
    runtimes: Option<Arc<EngineRuntimes>>,
    instance: Option<InstanceRef<Q>>,
    router: Option<RouterRef>,
    schema_config_provider: Option<SchemaConfigProviderRef>,
}

impl<Q> Builder<Q> {
    pub fn new() -> Self {
        Self {
            bind_addr: String::from("0.0.0.0"),
            port: 38081,
            env: None,
            runtimes: None,
            instance: None,
            router: None,
            schema_config_provider: None,
        }
    }

    pub fn bind_addr(mut self, addr: String) -> Self {
        self.bind_addr = addr;
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn env(mut self, env: Arc<Environment>) -> Self {
        self.env = Some(env);
        self
    }

    pub fn runtimes(mut self, runtimes: Arc<EngineRuntimes>) -> Self {
        self.runtimes = Some(runtimes);
        self
    }

    pub fn instance(mut self, instance: InstanceRef<Q>) -> Self {
        self.instance = Some(instance);
        self
    }

    pub fn router(mut self, router: RouterRef) -> Self {
        self.router = Some(router);
        self
    }

    pub fn schema_config_provider(mut self, provider: SchemaConfigProviderRef) -> Self {
        self.schema_config_provider = Some(provider);
        self
    }
}

impl<Q: QueryExecutor + 'static> Builder<Q> {
    pub fn build(self) -> Result<RpcServices> {
        let runtimes = self.runtimes.context(MissingRuntimes)?;
        let instance = self.instance.context(MissingInstance)?;
        let router = self.router.context(MissingRouter)?;
        let schema_config_provider = self
            .schema_config_provider
            .context(MissingSchemaConfigProvider)?;

        let storage_service = StorageServiceImpl {
            router,
            instance,
            runtimes,
            schema_config_provider,
        };
        let rpc_service = storage_grpc::create_storage_service(storage_service);

        let env = self.env.context(MissingEnv)?;

        let rpc_server = ServerBuilder::new(env)
            .register_service(rpc_service)
            .bind(self.bind_addr, self.port)
            .build()
            .context(BuildRpcServer)?;

        Ok(RpcServices { rpc_server })
    }
}

fn build_err_header(err: ServerError) -> ResponseHeader {
    let mut header = ResponseHeader::new();
    header.set_code(err.code().as_u16().into());
    header.set_error(err.error_message());

    header
}

fn build_ok_header() -> ResponseHeader {
    let mut header = ResponseHeader::new();
    header.set_code(StatusCode::OK.as_u16().into());

    header
}

struct StorageServiceImpl<Q> {
    router: Arc<dyn Router + Send + Sync>,
    instance: InstanceRef<Q>,
    runtimes: Arc<EngineRuntimes>,
    schema_config_provider: SchemaConfigProviderRef,
}

impl<Q> Clone for StorageServiceImpl<Q> {
    fn clone(&self) -> Self {
        Self {
            router: self.router.clone(),
            instance: self.instance.clone(),
            runtimes: self.runtimes.clone(),
            schema_config_provider: self.schema_config_provider.clone(),
        }
    }
}

macro_rules! handle_request {
    ($mod_name: ident, $handle_fn: ident, $req_ty: ident, $resp_ty: ident) => {
        fn $mod_name(&mut self, ctx: RpcContext<'_>, req: $req_ty, sink: UnarySink<$resp_ty>) {
            let begin_instant = Instant::now();

            let router = self.router.clone();
            let header = RequestHeader::from(ctx.request_headers());
            let instance = self.instance.clone();
            let (tx, rx) = oneshot::channel();

            // The future spawned by tokio cannot be executed by other executor/runtime, so

            let runtime = match stringify!($mod_name) {
                "query" => &self.runtimes.read_runtime,
                "write" => &self.runtimes.write_runtime,
                _ => &self.runtimes.bg_runtime,
            };

            let schema_config_provider = self.schema_config_provider.clone();
            // we need to pass the result via channel
            runtime.spawn(
                async move {
                    let handler_ctx =
                        HandlerContext::new(header, router, instance, &schema_config_provider)
                            .map_err(|e| Box::new(e) as _)
                            .context(ErrWithCause {
                                code: StatusCode::BAD_REQUEST,
                                msg: "Invalid header",
                            })?;
                    $mod_name::$handle_fn(&handler_ctx, req).await.map_err(|e| {
                        error!(
                            "Failed to handle request, mod:{}, handler:{}, err:{}",
                            stringify!($mod_name),
                            stringify!($handle_fn),
                            e
                        );
                        e
                    })
                }
                .then(|resp_result| async move {
                    if tx.send(resp_result).is_err() {
                        error!(
                            "Failed to send handler result, mod:{}, handler:{}",
                            stringify!($mod_name),
                            stringify!($handle_fn),
                        )
                    }
                }),
            );

            let task = async move {
                let resp_result = match rx.await {
                    Ok(resp_result) => resp_result,
                    Err(_e) => ErrNoCause {
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: "Result channel disconnected",
                    }
                    .fail(),
                };

                let resp = match resp_result {
                    Ok(resp) => resp,
                    Err(e) => {
                        let mut resp = $resp_ty::new();
                        resp.set_header(build_err_header(e));
                        resp
                    }
                };
                let ret = sink.success(resp).await.context(GrpcSink);

                GRPC_HANDLER_DURATION_HISTOGRAM_VEC
                    .$handle_fn
                    .observe(begin_instant.saturating_elapsed().as_secs_f64());

                ret?;

                Result::Ok(())
            }
            .map_err(move |e| {
                error!(
                    "Failed to reply grpc resp, mod:{}, handler:{}, err:{:?}",
                    stringify!($mod_name),
                    stringify!($handle_fn),
                    e
                )
            })
            .map(|_| ());

            ctx.spawn(task);
        }
    };
}

impl<Q: QueryExecutor + 'static> StorageService for StorageServiceImpl<Q> {
    handle_request!(route, handle_route, RouteRequest, RouteResponse);

    handle_request!(write, handle_write, WriteRequest, WriteResponse);

    handle_request!(query, handle_query, QueryRequest, QueryResponse);

    handle_request!(
        prom_query,
        handle_query,
        PrometheusQueryRequest,
        PrometheusQueryResponse
    );

    fn stream_write(
        &mut self,
        ctx: RpcContext<'_>,
        mut stream_req: RequestStream<WriteRequest>,
        sink: ClientStreamingSink<WriteResponse>,
    ) {
        let begin_instant = Instant::now();
        let router = self.router.clone();
        let header = RequestHeader::from(ctx.request_headers());
        let instance = self.instance.clone();
        let schema_config_provider = self.schema_config_provider.clone();

        let (tx, rx) = oneshot::channel();
        self.runtimes.write_runtime.spawn(async move {
            let handler_ctx = HandlerContext::new(header, router, instance, &schema_config_provider)
                .map_err(|e| Box::new(e) as _)
                .context(ErrWithCause {
                    code: StatusCode::BAD_REQUEST,
                    msg: "Invalid header",
                })?;
            let mut total_success = 0;
            let mut resp = WriteResponse::new();
            let mut has_err = false;
            while let Some(req) = stream_req.next().await {
                let write_result = write::handle_write(
                    &handler_ctx,
                    req.map_err(|e| Box::new(e) as _).context(ErrWithCause {
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: "Failed to fetch request",
                    })?,
                )
                .await
                .map_err(|e| {
                    error!("Failed to handle request, mod:stream_write, handler:handle_stream_write, err:{}", e);
                    e
                });

                match write_result {
                    Ok(write_resp) => total_success += write_resp.success,
                    Err(e) => {
                        resp.set_header(build_err_header(e));
                        has_err = true;
                        break;
                    }
                }
            }
            if !has_err {
                resp.set_header(build_ok_header());
                resp.set_success(total_success as u32);
            }

            ServerResult::Ok(resp)
        }.then(|resp_result| async move {
            if tx.send(resp_result).is_err() {
                error!("Failed to send handler result, mod:stream_write, handler:handle_stream_write");
                }
            }),
        );

        let task = async move {
            let resp_result = match rx.await {
                Ok(resp_result) => resp_result,
                Err(_e) => ErrNoCause {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    msg: "Result channel disconnected",
                }
                .fail(),
            };

            let resp = match resp_result {
                Ok(resp) => resp,
                Err(e) => {
                    let mut resp = WriteResponse::new();
                    resp.set_header(build_err_header(e));
                    resp
                }
            };
            sink.success(resp).await.context(GrpcSink)?;

            GRPC_HANDLER_DURATION_HISTOGRAM_VEC
                .handle_stream_write
                .observe(begin_instant.saturating_elapsed().as_secs_f64());

            Result::Ok(())
        }
        .map_err(move |e| {
            error!(
                "Failed to reply grpc resp, mod:stream_write, handler:handle_stream_write, err:{}",
                e
            )
        })
        .map(|_| ());

        ctx.spawn(task);
    }

    fn stream_query(
        &mut self,
        ctx: RpcContext<'_>,
        req: QueryRequest,
        mut sink: ServerStreamingSink<QueryResponse>,
    ) {
        let begin_instant = Instant::now();
        let router = self.router.clone();
        let header = RequestHeader::from(ctx.request_headers());
        let instance = self.instance.clone();
        let schema_config_provider = self.schema_config_provider.clone();
        let (tx, mut rx) = tokio::sync::mpsc::channel(STREAM_QUERY_CHANNEL_LEN);
        self.runtimes.read_runtime.spawn(async move {
            let handler_ctx = HandlerContext::new(header, router, instance, &schema_config_provider)
                .map_err(|e| Box::new(e) as _)
                .context(ErrWithCause {
                    code: StatusCode::BAD_REQUEST,
                    msg: "Invalid header",
                })?;
            let output = query::fetch_query_output(&handler_ctx, &req)
                .await
                .map_err(|e| {
                    error!("Failed to handle request, mod:stream_query, handler:handle_stream_query, err:{}", e);
                    e
                })?;
            if let Some(batch) = query::get_record_batch(&output) {
                for i in 0..batch.len() {
                    let resp = query::convert_records(&batch[i..i + 1]);
                    if tx.send(resp).await.is_err() {
                        error!("Failed to send handler result, mod:stream_query, handler:handle_stream_query");
                        break;
                    }
                }
            } else {
                let mut resp = QueryResponse::new();
                resp.set_header(build_ok_header());

                if tx.send(ServerResult::Ok(resp)).await.is_err() {
                    error!("Failed to send handler result, mod:stream_query, handler:handle_stream_query");
                }
            }
            ServerResult::Ok(())
        });

        let mut has_err = false;
        let task = async move {
            while let Some(result) = rx.recv().await {
                let resp = match result {
                    Ok(resp) => resp,
                    Err(e) => {
                        has_err = true;
                        let mut resp = QueryResponse::new();
                        resp.set_header(build_err_header(e));
                        resp
                    }
                };
                sink.send((resp, WriteFlags::default()))
                    .await
                    .context(GrpcSink)?;
                if has_err {
                    break;
                }
            }
            sink.flush().await.context(GrpcSink)?;
            sink.close().await.context(GrpcSink)?;
            GRPC_HANDLER_DURATION_HISTOGRAM_VEC
                .handle_stream_query
                .observe(begin_instant.saturating_elapsed().as_secs_f64());
            Result::Ok(())
        }
        .map_err(move |e| {
            error!(
                "Failed to reply grpc resp, mod:stream_query, handler:handle_stream_query, err:{}",
                e
            );
        })
        .map(|_| ());

        ctx.spawn(task);
    }
}

/// Create CreateTablePlan from a write metric.
// The caller must ENSURE that the HandlerContext's schema_config is not None.
pub fn write_metric_to_create_table_plan<Q: QueryExecutor + 'static>(
    ctx: &HandlerContext<Q>,
    write_metric: &WriteMetric,
) -> Result<CreateTablePlan> {
    let schema_config = ctx.schema_config.unwrap();
    Ok(CreateTablePlan {
        engine: schema_config.default_engine_type.clone(),
        if_not_exists: true,
        table: write_metric.get_metric().to_string(),
        table_schema: build_schema_from_metric(schema_config, write_metric)?,
        options: HashMap::default(),
    })
}

fn build_column_schema(
    column_name: &str,
    data_type: DatumKind,
    is_tag: bool,
) -> Result<ColumnSchema> {
    let builder = column_schema::Builder::new(column_name.to_string(), data_type)
        .is_nullable(true)
        .is_tag(is_tag);

    builder.build().context(BuildColumnSchema { column_name })
}

fn build_schema_from_metric(schema_config: &SchemaConfig, metric: &WriteMetric) -> Result<Schema> {
    let field_names = metric.get_field_names();
    let tag_names = metric.get_tag_names();
    let table_name = metric.get_metric();

    let mut schema_builder =
        SchemaBuilder::with_capacity(field_names.len()).auto_increment_column_id(true);

    let write_entries = metric.get_entries();

    ensure!(
        !write_entries.is_empty(),
        InvalidArgument {
            msg: format!("Emtpy write entires to write table:{}", table_name,),
        }
    );

    let mut name_column_map: BTreeMap<_, ColumnSchema> = BTreeMap::new();
    for write_entry in write_entries {
        // parse tags
        for tag in write_entry.get_tags() {
            let name_index = tag.name_index as usize;
            ensure!(
                name_index < tag_names.len(),
                InvalidArgument {
                    msg: format!(
                        "tag index {} is not found in tag_names:{:?}, table:{}",
                        name_index, tag_names, table_name,
                    ),
                }
            );

            let tag_name = &tag_names[name_index];

            let tag_value = tag
                .get_value()
                .value
                .as_ref()
                .with_context(|| InvalidArgument {
                    msg: format!("Tag value is needed, tag_name:{} ", tag_name),
                })?;

            let data_type = try_get_data_type_from_value(tag_value)?;

            if let Some(column_schema) = name_column_map.get(tag_name) {
                ensure_data_type_compatible(table_name, tag_name, true, data_type, column_schema)?;
            }
            let column_schema = build_column_schema(tag_name, data_type, true)?;
            name_column_map.insert(tag_name, column_schema);
        }

        // parse fields
        for field_group in write_entry.get_field_groups().iter() {
            for field in field_group.get_fields() {
                if (field.name_index as usize) < field_names.len() {
                    let field_name = &field_names[field.name_index as usize];
                    let field_value =
                        field
                            .get_value()
                            .value
                            .as_ref()
                            .with_context(|| InvalidArgument {
                                msg: format!(
                                    "Field: {} value is needed, table:{}",
                                    field_name, table_name
                                ),
                            })?;

                    let data_type = try_get_data_type_from_value(field_value)?;

                    if let Some(column_schema) = name_column_map.get(field_name) {
                        ensure_data_type_compatible(
                            table_name,
                            field_name,
                            false,
                            data_type,
                            column_schema,
                        )?;
                    }

                    let column_schema = build_column_schema(field_name, data_type, false)?;
                    name_column_map.insert(field_name, column_schema);
                }
            }
        }
    }

    // Timestamp column will be the last column
    let timestamp_column_schema = column_schema::Builder::new(
        schema_config.default_timestamp_column_name.clone(),
        DatumKind::Timestamp,
    )
    .is_nullable(false)
    .build()
    .context(InvalidColumnSchema {
        column_name: TSID_COLUMN,
    })?;

    // Use (timestamp, tsid) as primary key.
    let tsid_column_schema =
        column_schema::Builder::new(TSID_COLUMN.to_string(), DatumKind::UInt64)
            .is_nullable(false)
            .build()
            .context(InvalidColumnSchema {
                column_name: TSID_COLUMN,
            })?;

    schema_builder = schema_builder
        .enable_tsid_primary_key(true)
        .add_key_column(timestamp_column_schema)
        .with_context(|| BuildTableSchema { metric: table_name })?
        .add_key_column(tsid_column_schema)
        .with_context(|| BuildTableSchema { metric: table_name })?;

    for col in name_column_map.into_values() {
        schema_builder = schema_builder
            .add_normal_column(col)
            .with_context(|| BuildTableSchema { metric: table_name })?;
    }

    schema_builder.build().with_context(|| BuildTableSchema {
        metric: metric.get_metric(),
    })
}

fn ensure_data_type_compatible(
    table_name: &str,
    column_name: &str,
    is_tag: bool,
    data_type: DatumKind,
    column_schema: &ColumnSchema,
) -> Result<()> {
    ensure!(
        column_schema.is_tag == is_tag,
        InvalidArgument {
            msg: format!(
                "Duplicated column: {} in fields and tags for table: {}",
                column_name, table_name,
            ),
        }
    );
    ensure!(
        column_schema.data_type == data_type,
        InvalidArgument {
            msg: format!(
                "Column: {} in table: {} data type is not same, expected: {}, actual: {}",
                column_name, table_name, column_schema.data_type, data_type,
            ),
        }
    );
    Ok(())
}

fn try_get_data_type_from_value(value: &Value_oneof_value) -> Result<DatumKind> {
    match value {
        Value_oneof_value::float64_value(_) => Ok(DatumKind::Double),
        Value_oneof_value::string_value(_) => Ok(DatumKind::String),
        Value_oneof_value::int64_value(_) => Ok(DatumKind::Int64),
        Value_oneof_value::float32_value(_) => Ok(DatumKind::Float),
        Value_oneof_value::int32_value(_) => Ok(DatumKind::Int32),
        Value_oneof_value::int16_value(_) => Ok(DatumKind::Int16),
        Value_oneof_value::int8_value(_) => Ok(DatumKind::Int8),
        Value_oneof_value::bool_value(_) => Ok(DatumKind::Boolean),
        Value_oneof_value::uint64_value(_) => Ok(DatumKind::UInt64),
        Value_oneof_value::uint32_value(_) => Ok(DatumKind::UInt32),
        Value_oneof_value::uint16_value(_) => Ok(DatumKind::UInt16),
        Value_oneof_value::uint8_value(_) => Ok(DatumKind::UInt8),
        Value_oneof_value::timestamp_value(_) => Ok(DatumKind::Timestamp),
        Value_oneof_value::varbinary_value(_) => Ok(DatumKind::Varbinary),
    }
}

#[cfg(test)]
mod tests {
    use ceresdbproto_deps::ceresdbproto::storage::{
        Field, FieldGroup, Tag, Value, WriteEntry, WriteMetric,
    };
    use cluster::config::SchemaConfig;
    use common_types::datum::DatumKind;

    use super::*;

    const TAG1: &str = "host";
    const TAG2: &str = "idc";
    const FIELD1: &str = "cpu";
    const FIELD2: &str = "memory";
    const FIELD3: &str = "log";
    const FIELD4: &str = "ping_ok";
    const METRIC: &str = "pod_system_metric";
    const TIMESTAMP_COLUMN: &str = "custom_timestamp";

    fn generate_write_metric() -> WriteMetric {
        let mut write_metric = WriteMetric::default();
        write_metric.set_metric(METRIC.to_string());

        let tag_names = vec![TAG1.to_string(), TAG2.to_string()];
        let field_names = vec![
            FIELD1.to_string(),
            FIELD2.to_string(),
            FIELD3.to_string(),
            FIELD4.to_string(),
        ];

        write_metric.set_field_names(field_names.into());
        write_metric.set_tag_names(tag_names.into());

        //tags
        let mut tag1 = Tag::new();
        tag1.set_name_index(0);
        let mut tag_val1 = Value::new();
        tag_val1.set_string_value("test.host".to_string());
        tag1.set_value(tag_val1);
        let mut tag2 = Tag::new();
        tag2.set_name_index(1);
        let mut tag_val2 = Value::new();
        tag_val2.set_string_value("test.idc".to_string());
        tag2.set_value(tag_val2);
        let tags = vec![tag1, tag2];

        //fields
        let mut field1 = Field::new();
        field1.set_name_index(0);
        let mut field_val1 = Value::new();
        field_val1.set_float64_value(100.0);
        field1.set_value(field_val1);
        let mut field2 = Field::new();
        field2.set_name_index(1);
        let mut field_val2 = Value::new();
        field_val2.set_float64_value(1024.0);
        field2.set_value(field_val2);
        let mut field3 = Field::new();
        field3.set_name_index(2);
        let mut field_val3 = Value::new();
        field_val3.set_string_value("test log".to_string());
        field3.set_value(field_val3);
        let mut field4 = Field::new();
        field4.set_name_index(3);
        let mut field_val4 = Value::new();
        field_val4.set_bool_value(true);
        field4.set_value(field_val4);

        let mut field_group1 = FieldGroup::new();
        field_group1.set_timestamp(1000);
        field_group1.set_fields(vec![field1.clone(), field4].into());

        let mut field_group2 = FieldGroup::new();
        field_group2.set_timestamp(2000);
        field_group2.set_fields(vec![field1.clone(), field2.clone()].into());

        let mut field_group3 = FieldGroup::new();
        field_group3.set_timestamp(3000);
        field_group3.set_fields(vec![field3].into());

        let mut write_entry = WriteEntry::new();
        write_entry.set_tags(tags.into());
        write_entry.set_field_groups(vec![field_group1, field_group2, field_group3].into());

        write_metric.set_entries(vec![write_entry].into());

        write_metric
    }

    #[test]
    fn test_build_schema_from_metric() {
        let schema_config = SchemaConfig {
            auto_create_tables: true,
            default_timestamp_column_name: TIMESTAMP_COLUMN.to_string(),
            ..SchemaConfig::default()
        };
        let write_metric = generate_write_metric();

        let schema = build_schema_from_metric(&schema_config, &write_metric);
        assert!(schema.is_ok());

        let schema = schema.unwrap();

        assert_eq!(8, schema.num_columns());
        assert_eq!(2, schema.num_key_columns());
        assert_eq!(TIMESTAMP_COLUMN, schema.timestamp_name());
        let tsid = schema.tsid_column();
        assert!(tsid.is_some());

        let key_columns = schema.key_columns();
        assert_eq!(2, key_columns.len());
        assert_eq!(TIMESTAMP_COLUMN, key_columns[0].name);
        assert_eq!("tsid", key_columns[1].name);

        let columns = schema.normal_columns();
        assert_eq!(6, columns.len());

        // sorted by column names because of btree
        assert_eq!(FIELD1, columns[0].name);
        assert!(!columns[0].is_tag);
        assert_eq!(DatumKind::Double, columns[0].data_type);
        assert_eq!(TAG1, columns[1].name);
        assert!(columns[1].is_tag);
        assert_eq!(DatumKind::String, columns[1].data_type);
        assert_eq!(TAG2, columns[2].name);
        assert!(columns[2].is_tag);
        assert_eq!(DatumKind::String, columns[2].data_type);
        assert_eq!(FIELD3, columns[3].name);
        assert!(!columns[3].is_tag);
        assert_eq!(DatumKind::String, columns[3].data_type);
        assert_eq!(FIELD2, columns[4].name);
        assert!(!columns[4].is_tag);
        assert_eq!(DatumKind::Double, columns[4].data_type);
        assert_eq!(FIELD4, columns[5].name);
        assert!(!columns[5].is_tag);
        assert_eq!(DatumKind::Boolean, columns[5].data_type);

        for column in columns {
            assert!(column.is_nullable);
        }
    }
}
