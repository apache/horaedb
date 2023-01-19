// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// Storage rpc service implement.

use std::{
    collections::{BTreeMap, HashMap},
    stringify,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use ceresdbproto::{
    prometheus::{PrometheusQueryRequest, PrometheusQueryResponse},
    storage::{
        storage_service_server::StorageService, value::Value, RouteRequest, RouteResponse,
        SqlQueryRequest, SqlQueryResponse, WriteRequest, WriteResponse, WriteTableRequest,
    },
};
use cluster::config::SchemaConfig;
use common_types::{
    column_schema::{self, ColumnSchema},
    datum::DatumKind,
    schema::{Builder as SchemaBuilder, Schema, TSID_COLUMN},
};
use common_util::{runtime::JoinHandle, time::InstantExt};
use futures::stream::{self, BoxStream, StreamExt};
use http::StatusCode;
use log::{error, warn};
use paste::paste;
use query_engine::executor::Executor as QueryExecutor;
use router::{Router, RouterRef};
use snafu::{ensure, OptionExt, ResultExt};
use sql::plan::CreateTablePlan;
use table_engine::engine::EngineRuntimes;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::{KeyAndValueRef, MetadataMap};

use crate::{
    consts,
    grpc::{
        forward::ForwarderRef,
        metrics::GRPC_HANDLER_DURATION_HISTOGRAM_VEC,
        storage_service::error::{ErrNoCause, ErrWithCause, Result},
    },
    instance::InstanceRef,
    schema_config_provider::SchemaConfigProviderRef,
};

pub(crate) mod error;
mod prom_query;
mod route;
mod sql_query;
pub(crate) mod write;
mod write;

const STREAM_QUERY_CHANNEL_LEN: usize = 20;

/// Rpc request header
#[derive(Debug, Default)]
pub struct RequestHeader {
    metas: HashMap<String, Vec<u8>>,
}

impl From<&MetadataMap> for RequestHeader {
    fn from(meta: &MetadataMap) -> Self {
        let metas = meta
            .iter()
            .filter_map(|kv| match kv {
                KeyAndValueRef::Ascii(key, val) => {
                    // TODO: The value may be encoded in base64, which is not expected.
                    Some((key.to_string(), val.as_encoded_bytes().to_vec()))
                }
                KeyAndValueRef::Binary(key, val) => {
                    warn!(
                        "Binary header is not supported yet and will be omit, key:{:?}, val:{:?}",
                        key, val
                    );
                    None
                }
            })
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
    forwarder: Option<ForwarderRef>,
    timeout: Option<Duration>,
    min_rows_per_batch: usize,
}

impl<'a, Q> HandlerContext<'a, Q> {
    fn new(
        header: RequestHeader,
        router: Arc<dyn Router + Sync + Send>,
        instance: InstanceRef<Q>,
        schema_config_provider: &'a SchemaConfigProviderRef,
        forwarder: Option<ForwarderRef>,
        timeout: Option<Duration>,
        min_rows_per_batch: usize,
    ) -> Result<Self> {
        let default_catalog = instance.catalog_manager.default_catalog_name();
        let default_schema = instance.catalog_manager.default_schema_name();

        let catalog = header
            .get(consts::CATALOG_HEADER)
            .map(|v| String::from_utf8(v.to_vec()))
            .transpose()
            .map_err(|e| Box::new(e) as _)
            .context(ErrWithCause {
                code: StatusCode::BAD_REQUEST,
                msg: "fail to parse catalog name",
            })?
            .unwrap_or_else(|| default_catalog.to_string());

        let schema = header
            .get(consts::SCHEMA_HEADER)
            .map(|v| String::from_utf8(v.to_vec()))
            .transpose()
            .map_err(|e| Box::new(e) as _)
            .context(ErrWithCause {
                code: StatusCode::BAD_REQUEST,
                msg: "fail to parse schema name",
            })?
            .unwrap_or_else(|| default_schema.to_string());

        let schema_config = schema_config_provider
            .schema_config(&schema)
            .map_err(|e| Box::new(e) as _)
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!("fail to fetch schema config, schema_name:{}", schema),
            })?;

        Ok(Self {
            header,
            router,
            instance,
            catalog,
            schema,
            schema_config,
            forwarder,
            timeout,
            min_rows_per_batch,
        })
    }

    #[inline]
    fn catalog(&self) -> &str {
        &self.catalog
    }

    #[inline]
    fn schema(&self) -> &str {
        &self.schema
    }
}

#[derive(Clone)]
pub struct StorageServiceImpl<Q: QueryExecutor + 'static> {
    pub router: Arc<dyn Router + Send + Sync>,
    pub instance: InstanceRef<Q>,
    pub runtimes: Arc<EngineRuntimes>,
    pub schema_config_provider: SchemaConfigProviderRef,
    pub forwarder: Option<ForwarderRef>,
    pub timeout: Option<Duration>,
    pub min_rows_per_batch: usize,
}

macro_rules! handle_request {
    ($mod_name: ident, $handle_fn: ident, $req_ty: ident, $resp_ty: ident) => {
        paste! {
            async fn [<$mod_name _internal>] (
                &self,
                request: tonic::Request<$req_ty>,
            ) -> std::result::Result<tonic::Response<$resp_ty>, tonic::Status> {
                let instant = Instant::now();

                let router = self.router.clone();
                let header = RequestHeader::from(request.metadata());
                let instance = self.instance.clone();
                let forwarder = self.forwarder.clone();
                let timeout = self.timeout;
                let min_rows_per_batch = self.min_rows_per_batch;

                // The future spawned by tokio cannot be executed by other executor/runtime, so

                let runtime = match stringify!($mod_name) {
                    "sql_query" | "prom_query" => &self.runtimes.read_runtime,
                    "write" => &self.runtimes.write_runtime,
                    _ => &self.runtimes.bg_runtime,
                };

                let schema_config_provider = self.schema_config_provider.clone();
                // we need to pass the result via channel
                let join_handle = runtime.spawn(async move {
                    let handler_ctx =
                        HandlerContext::new(header, router, instance, &schema_config_provider, forwarder, timeout, min_rows_per_batch)
                            .map_err(|e| Box::new(e) as _)
                            .context(ErrWithCause {
                                code: StatusCode::BAD_REQUEST,
                                msg: "invalid header",
                            })?;
                    $mod_name::$handle_fn(&handler_ctx, request.into_inner())
                        .await
                        .map_err(|e| {
                            error!(
                                "Failed to handle request, mod:{}, handler:{}, err:{}",
                                stringify!($mod_name),
                                stringify!($handle_fn),
                                e
                            );
                            e
                        })
                });

                let res = join_handle
                    .await
                    .map_err(|e| Box::new(e) as _)
                    .context(ErrWithCause {
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: "fail to join the spawn task",
                    });

                let resp = match res {
                    Ok(Ok(v)) => v,
                    Ok(Err(e)) | Err(e) => {
                        let mut resp = $resp_ty::default();
                        let header = error::build_err_header(e);
                        resp.header = Some(header);
                        resp
                    },
                };

                GRPC_HANDLER_DURATION_HISTOGRAM_VEC
                    .$handle_fn
                    .observe(instant.saturating_elapsed().as_secs_f64());
                Ok(tonic::Response::new(resp))
            }
        }
    };
}

impl<Q: QueryExecutor + 'static> StorageServiceImpl<Q> {
    handle_request!(route, handle_route, RouteRequest, RouteResponse);

    handle_request!(write, handle_write, WriteRequest, WriteResponse);

    handle_request!(sql_query, handle_query, SqlQueryRequest, SqlQueryResponse);

    handle_request!(
        prom_query,
        handle_query,
        PrometheusQueryRequest,
        PrometheusQueryResponse
    );

    async fn stream_write_internal(
        &self,
        request: tonic::Request<tonic::Streaming<WriteRequest>>,
    ) -> Result<WriteResponse> {
        let begin_instant = Instant::now();
        let router = self.router.clone();
        let header = RequestHeader::from(request.metadata());
        let instance = self.instance.clone();
        let schema_config_provider = self.schema_config_provider.clone();

        let handler_ctx = HandlerContext::new(
            header,
            router,
            instance,
            &schema_config_provider,
            self.forwarder.clone(),
            self.timeout,
            self.min_rows_per_batch,
        )
        .map_err(|e| Box::new(e) as _)
        .context(ErrWithCause {
            code: StatusCode::BAD_REQUEST,
            msg: "invalid header",
        })?;

        let mut total_success = 0;
        let mut resp = WriteResponse::default();
        let mut has_err = false;
        let mut stream = request.into_inner();
        while let Some(req) = stream.next().await {
            let write_req = req.map_err(|e| Box::new(e) as _).context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "failed to fetch request",
            })?;

            let write_result = write::handle_write(
                &handler_ctx,
                write_req,
            )
            .await
            .map_err(|e| {
                error!("Failed to handle request, mod:stream_write, handler:handle_stream_write, err:{}", e);
                e
            });

            match write_result {
                Ok(write_resp) => total_success += write_resp.success,
                Err(e) => {
                    resp.header = Some(error::build_err_header(e));
                    has_err = true;
                    break;
                }
            }
        }

        if !has_err {
            resp.header = Some(error::build_ok_header());
            resp.success = total_success as u32;
        }

        GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .handle_stream_write
            .observe(begin_instant.saturating_elapsed().as_secs_f64());

        Ok(resp)
    }

    async fn stream_sql_query_internal(
        &self,
        request: tonic::Request<SqlQueryRequest>,
    ) -> Result<ReceiverStream<Result<SqlQueryResponse>>> {
        let begin_instant = Instant::now();
        let router = self.router.clone();
        let header = RequestHeader::from(request.metadata());
        let instance = self.instance.clone();
        let schema_config_provider = self.schema_config_provider.clone();
        let forwarder = self.forwarder.clone();
        let timeout = self.timeout;
        let min_rows_per_batch = self.min_rows_per_batch;

        let (tx, rx) = mpsc::channel(STREAM_QUERY_CHANNEL_LEN);
        let _: JoinHandle<Result<()>> = self.runtimes.read_runtime.spawn(async move {
            let handler_ctx = HandlerContext::new(header, router, instance, &schema_config_provider, forwarder, timeout, min_rows_per_batch)
                .map_err(|e| Box::new(e) as _)
                .context(ErrWithCause {
                    code: StatusCode::BAD_REQUEST,
                    msg: "invalid header",
                })?;

            let query_req = request.into_inner();
            let output = sql_query::fetch_query_output(&handler_ctx, &query_req)
                    .await
                    .map_err(|e| {
                        error!("Failed to handle request, mod:stream_query, handler:handle_stream_query, err:{}", e);
                        e
                    })?;
            if let Some(batch) = sql_query::get_record_batch(output) {
                for i in 0..batch.len() {
                    let resp = sql_query::convert_records(&batch[i..i + 1], min_rows_per_batch);
                    if tx.send(resp).await.is_err() {
                        error!("Failed to send handler result, mod:stream_query, handler:handle_stream_query");
                        break;
                    }
                }
            } else {
                let resp = SqlQueryResponse {
                    header: Some(error::build_ok_header()),
                    ..Default::default()
                };

                if tx.send(Result::Ok(resp)).await.is_err() {
                    error!(
                        "Failed to send handler result, mod:stream_query, handler:handle_stream_query"
                    );
                }
            }

            Ok(())
        });

        GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .handle_stream_query
            .observe(begin_instant.saturating_elapsed().as_secs_f64());

        Ok(ReceiverStream::new(rx))
    }
}

#[async_trait]
impl<Q: QueryExecutor + 'static> StorageService for StorageServiceImpl<Q> {
    type StreamSqlQueryStream =
        BoxStream<'static, std::result::Result<SqlQueryResponse, tonic::Status>>;

    async fn route(
        &self,
        request: tonic::Request<RouteRequest>,
    ) -> std::result::Result<tonic::Response<RouteResponse>, tonic::Status> {
        self.route_internal(request).await
    }

    async fn write(
        &self,
        request: tonic::Request<WriteRequest>,
    ) -> std::result::Result<tonic::Response<WriteResponse>, tonic::Status> {
        self.write_internal(request).await
    }

    async fn sql_query(
        &self,
        request: tonic::Request<SqlQueryRequest>,
    ) -> std::result::Result<tonic::Response<SqlQueryResponse>, tonic::Status> {
        self.sql_query_internal(request).await
    }

    async fn prom_query(
        &self,
        request: tonic::Request<PrometheusQueryRequest>,
    ) -> std::result::Result<tonic::Response<PrometheusQueryResponse>, tonic::Status> {
        self.prom_query_internal(request).await
    }

    async fn stream_write(
        &self,
        request: tonic::Request<tonic::Streaming<WriteRequest>>,
    ) -> std::result::Result<tonic::Response<WriteResponse>, tonic::Status> {
        let resp = match self.stream_write_internal(request).await {
            Ok(resp) => resp,
            Err(e) => WriteResponse {
                header: Some(error::build_err_header(e)),
                ..Default::default()
            },
        };
        Ok(tonic::Response::new(resp))
    }

    async fn stream_sql_query(
        &self,
        request: tonic::Request<SqlQueryRequest>,
    ) -> std::result::Result<tonic::Response<Self::StreamSqlQueryStream>, tonic::Status> {
        match self.stream_sql_query_internal(request).await {
            Ok(stream) => {
                let new_stream: Self::StreamSqlQueryStream =
                    Box::pin(stream.map(|res| match res {
                        Ok(resp) => Ok(resp),
                        Err(e) => {
                            let resp = SqlQueryResponse {
                                header: Some(error::build_err_header(e)),
                                ..Default::default()
                            };
                            Ok(resp)
                        }
                    }));

                Ok(tonic::Response::new(new_stream))
            }
            Err(e) => {
                let resp = SqlQueryResponse {
                    header: Some(error::build_err_header(e)),
                    ..Default::default()
                };
                let stream = stream::once(async { Ok(resp) });
                Ok(tonic::Response::new(Box::pin(stream)))
            }
        }
    }
}

/// Create CreateTablePlan from a write metric.
// The caller must ENSURE that the HandlerContext's schema_config is not None.
pub fn write_table_request_to_create_table_plan<Q: QueryExecutor + 'static>(
    schema_config: Option<&SchemaConfig>,
    ctx: &HandlerContext<Q>,
    write_table: &WriteTableRequest,
) -> Result<CreateTablePlan> {
    let schema_config = schema_config.unwrap();
    Ok(CreateTablePlan {
        engine: schema_config.default_engine_type.clone(),
        if_not_exists: true,
        table: write_table.table.clone(),
        table_schema: build_schema_from_write_table_request(schema_config, write_table)?,
        options: HashMap::default(),
        partition_info: None,
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

    builder
        .build()
        .map_err(|e| Box::new(e) as _)
        .context(ErrWithCause {
            code: StatusCode::BAD_REQUEST,
            msg: "invalid column schema",
        })
}

fn build_schema_from_write_table_request(
    schema_config: &SchemaConfig,
    write_table_req: &WriteTableRequest,
) -> Result<Schema> {
    let WriteTableRequest {
        table,
        field_names,
        tag_names,
        entries: write_entries,
    } = write_table_req;

    let mut schema_builder =
        SchemaBuilder::with_capacity(field_names.len()).auto_increment_column_id(true);

    ensure!(
        !write_entries.is_empty(),
        ErrNoCause {
            code: StatusCode::BAD_REQUEST,
            msg: format!("empty write entires to write table:{}", table),
        }
    );

    let mut name_column_map: BTreeMap<_, ColumnSchema> = BTreeMap::new();
    for write_entry in write_entries {
        // parse tags
        for tag in &write_entry.tags {
            let name_index = tag.name_index as usize;
            ensure!(
                name_index < tag_names.len(),
                ErrNoCause {
                    code: StatusCode::BAD_REQUEST,
                    msg: format!(
                        "tag index {} is not found in tag_names:{:?}, table:{}",
                        name_index, tag_names, table,
                    ),
                }
            );

            let tag_name = &tag_names[name_index];

            let tag_value = tag
                .value
                .as_ref()
                .with_context(|| ErrNoCause {
                    code: StatusCode::BAD_REQUEST,
                    msg: format!("Tag({}) value is needed, table_name:{} ", tag_name, table),
                })?
                .value
                .as_ref()
                .with_context(|| ErrNoCause {
                    code: StatusCode::BAD_REQUEST,
                    msg: format!(
                        "Tag({}) value type is not supported, table_name:{}",
                        tag_name, table
                    ),
                })?;

            let data_type = try_get_data_type_from_value(tag_value)?;

            if let Some(column_schema) = name_column_map.get(tag_name) {
                ensure_data_type_compatible(table, tag_name, true, data_type, column_schema)?;
            }
            let column_schema = build_column_schema(tag_name, data_type, true)?;
            name_column_map.insert(tag_name, column_schema);
        }

        // parse fields
        for field_group in &write_entry.field_groups {
            for field in &field_group.fields {
                if (field.name_index as usize) < field_names.len() {
                    let field_name = &field_names[field.name_index as usize];
                    let field_value = field
                        .value
                        .as_ref()
                        .with_context(|| ErrNoCause {
                            code: StatusCode::BAD_REQUEST,
                            msg: format!("Field({}) value is needed, table:{}", field_name, table),
                        })?
                        .value
                        .as_ref()
                        .with_context(|| ErrNoCause {
                            code: StatusCode::BAD_REQUEST,
                            msg: format!(
                                "Field({}) value type is not supported, table:{}",
                                field_name, table
                            ),
                        })?;

                    let data_type = try_get_data_type_from_value(field_value)?;

                    if let Some(column_schema) = name_column_map.get(field_name) {
                        ensure_data_type_compatible(
                            table,
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
    .map_err(|e| Box::new(e) as _)
    .context(ErrWithCause {
        code: StatusCode::BAD_REQUEST,
        msg: "invalid timestamp column schema to build",
    })?;

    // Use (timestamp, tsid) as primary key.
    let tsid_column_schema =
        column_schema::Builder::new(TSID_COLUMN.to_string(), DatumKind::UInt64)
            .is_nullable(false)
            .build()
            .map_err(|e| Box::new(e) as _)
            .context(ErrWithCause {
                code: StatusCode::BAD_REQUEST,
                msg: "invalid tsid column schema to build",
            })?;

    schema_builder = schema_builder
        .add_key_column(timestamp_column_schema)
        .map_err(|e| Box::new(e) as _)
        .context(ErrWithCause {
            code: StatusCode::BAD_REQUEST,
            msg: "invalid timestamp column to add",
        })?
        .add_key_column(tsid_column_schema)
        .map_err(|e| Box::new(e) as _)
        .context(ErrWithCause {
            code: StatusCode::BAD_REQUEST,
            msg: "invalid tsid column to add",
        })?;

    for col in name_column_map.into_values() {
        schema_builder = schema_builder
            .add_normal_column(col)
            .map_err(|e| Box::new(e) as _)
            .context(ErrWithCause {
                code: StatusCode::BAD_REQUEST,
                msg: "invalid normal column to add",
            })?;
    }

    schema_builder
        .build()
        .map_err(|e| Box::new(e) as _)
        .context(ErrWithCause {
            code: StatusCode::BAD_REQUEST,
            msg: "invalid schema to build",
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
        ErrNoCause {
            code: StatusCode::BAD_REQUEST,
            msg: format!(
                "Duplicated column: {} in fields and tags for table: {}",
                column_name, table_name,
            ),
        }
    );

    ensure!(
        column_schema.data_type == data_type,
        ErrNoCause {
            code: StatusCode::BAD_REQUEST,
            msg: format!(
                "Column: {} in table: {} data type is not same, expected: {}, actual: {}",
                column_name, table_name, column_schema.data_type, data_type,
            ),
        }
    );

    Ok(())
}

fn try_get_data_type_from_value(value: &Value) -> Result<DatumKind> {
    match value {
        Value::Float64Value(_) => Ok(DatumKind::Double),
        Value::StringValue(_) => Ok(DatumKind::String),
        Value::Int64Value(_) => Ok(DatumKind::Int64),
        Value::Float32Value(_) => Ok(DatumKind::Float),
        Value::Int32Value(_) => Ok(DatumKind::Int32),
        Value::Int16Value(_) => Ok(DatumKind::Int16),
        Value::Int8Value(_) => Ok(DatumKind::Int8),
        Value::BoolValue(_) => Ok(DatumKind::Boolean),
        Value::Uint64Value(_) => Ok(DatumKind::UInt64),
        Value::Uint32Value(_) => Ok(DatumKind::UInt32),
        Value::Uint16Value(_) => Ok(DatumKind::UInt16),
        Value::Uint8Value(_) => Ok(DatumKind::UInt8),
        Value::TimestampValue(_) => Ok(DatumKind::Timestamp),
        Value::VarbinaryValue(_) => Ok(DatumKind::Varbinary),
    }
}

#[cfg(test)]
mod tests {
    use ceresdbproto::storage::{value, Field, FieldGroup, Tag, Value, WriteSeriesEntry};
    use cluster::config::SchemaConfig;
    use common_types::datum::DatumKind;

    use super::*;

    const TAG1: &str = "host";
    const TAG2: &str = "idc";
    const FIELD1: &str = "cpu";
    const FIELD2: &str = "memory";
    const FIELD3: &str = "log";
    const FIELD4: &str = "ping_ok";
    const TABLE: &str = "pod_system_table";
    const TIMESTAMP_COLUMN: &str = "custom_timestamp";

    fn make_tag(name_index: u32, val: &str) -> Tag {
        Tag {
            name_index,
            value: Some(Value {
                value: Some(value::Value::StringValue(val.to_string())),
            }),
        }
    }

    fn make_field(name_index: u32, val: value::Value) -> Field {
        Field {
            name_index,
            value: Some(Value { value: Some(val) }),
        }
    }

    fn generate_write_table_request() -> WriteTableRequest {
        let tag1 = make_tag(0, "test.host");
        let tag2 = make_tag(1, "test.idc");
        let tags = vec![tag1, tag2];

        let field1 = make_field(0, value::Value::Float64Value(100.0));
        let field2 = make_field(1, value::Value::Float64Value(1024.0));
        let field3 = make_field(2, value::Value::StringValue("test log".to_string()));
        let field4 = make_field(3, value::Value::BoolValue(true));

        let field_group1 = FieldGroup {
            timestamp: 1000,
            fields: vec![field1.clone(), field4],
        };
        let field_group2 = FieldGroup {
            timestamp: 2000,
            fields: vec![field1, field2],
        };
        let field_group3 = FieldGroup {
            timestamp: 3000,
            fields: vec![field3],
        };

        let write_entry = WriteSeriesEntry {
            tags,
            field_groups: vec![field_group1, field_group2, field_group3],
        };

        let tag_names = vec![TAG1.to_string(), TAG2.to_string()];
        let field_names = vec![
            FIELD1.to_string(),
            FIELD2.to_string(),
            FIELD3.to_string(),
            FIELD4.to_string(),
        ];

        WriteTableRequest {
            table: TABLE.to_string(),
            tag_names,
            field_names,
            entries: vec![write_entry],
        }
    }

    #[test]
    fn test_build_schema_from_write_table_request() {
        let schema_config = SchemaConfig {
            auto_create_tables: true,
            default_timestamp_column_name: TIMESTAMP_COLUMN.to_string(),
            ..SchemaConfig::default()
        };
        let write_table_request = generate_write_table_request();

        let schema = build_schema_from_write_table_request(&schema_config, &write_table_request);
        assert!(schema.is_ok());

        let schema = schema.unwrap();

        assert_eq!(8, schema.num_columns());
        assert_eq!(2, schema.num_primary_key_columns());
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
