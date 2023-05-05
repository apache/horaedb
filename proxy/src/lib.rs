// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! The proxy module provides features such as forwarding and authentication,
//! adapts to different protocols.

#![feature(trait_alias)]

pub mod context;
pub(crate) mod error;
pub(crate) mod error_util;
#[allow(dead_code)]
pub mod forward;
pub(crate) mod grpc;
pub mod handlers;
pub mod hotspot;
mod hotspot_lru;
pub mod http;
pub mod instance;
pub mod limiter;
pub mod schema_config_provider;
pub(crate) mod util;

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};

use ::http::StatusCode;
use bytes::Bytes;
use catalog::schema::{CreateOptions, CreateTableRequest, DropOptions, DropTableRequest};
use ceresdbproto::storage::{
    storage_service_client::StorageServiceClient, value, PrometheusRemoteQueryRequest,
    PrometheusRemoteQueryResponse, SqlQueryRequest, SqlQueryResponse, WriteSeriesEntry,
    WriteTableRequest,
};
use cluster::config::SchemaConfig;
use common_types::{
    column_schema::ColumnSchema,
    datum::{Datum, DatumKind},
    request_id::RequestId,
    row::{Row, RowGroupBuilder},
    schema::Schema,
    table::DEFAULT_SHARD_ID,
    time::Timestamp,
};
use common_util::{error::BoxError, runtime::Runtime};
use futures::FutureExt;
use interpreters::{context::Context as InterpreterContext, factory::Factory, interpreter::Output};
use log::{debug, error, info, warn};
use query_engine::executor::Executor as QueryExecutor;
use query_frontend::{
    frontend,
    frontend::{Context as FrontendContext, Frontend},
    plan::{AlterTableOperation, AlterTablePlan, InsertPlan, Plan},
    planner::build_schema_from_write_table_request,
    provider::CatalogMetaProvider,
};
use router::{endpoint::Endpoint, Router};
use snafu::{ensure, OptionExt, ResultExt};
use table_engine::{
    engine::{EngineRuntimes, TableState},
    remote::model::{GetTableInfoRequest, TableIdentifier},
    table::{TableId, TableRef},
    PARTITION_TABLE_ENGINE_TYPE,
};
use tonic::{transport::Channel, IntoRequest};

use crate::{
    error::{ErrNoCause, ErrWithCause, Error, Internal, Result},
    forward::{ForwardRequest, ForwardResult, Forwarder, ForwarderRef},
    grpc::write::WriteContext,
    hotspot::HotspotRecorder,
    instance::InstanceRef,
    schema_config_provider::SchemaConfigProviderRef,
};

pub struct Proxy<Q> {
    router: Arc<dyn Router + Send + Sync>,
    forwarder: ForwarderRef,
    instance: InstanceRef<Q>,
    resp_compress_min_length: usize,
    auto_create_table: bool,
    schema_config_provider: SchemaConfigProviderRef,
    hotspot_recorder: Arc<HotspotRecorder>,
    engine_runtimes: Arc<EngineRuntimes>,
}

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        router: Arc<dyn Router + Send + Sync>,
        instance: InstanceRef<Q>,
        forward_config: forward::Config,
        local_endpoint: Endpoint,
        resp_compress_min_length: usize,
        auto_create_table: bool,
        schema_config_provider: SchemaConfigProviderRef,
        hotspot_config: hotspot::Config,
        engine_runtimes: Arc<EngineRuntimes>,
    ) -> Self {
        let forwarder = Arc::new(Forwarder::new(
            forward_config,
            router.clone(),
            local_endpoint,
        ));
        let hotspot_recorder = Arc::new(HotspotRecorder::new(
            hotspot_config,
            engine_runtimes.default_runtime.clone(),
        ));

        Self {
            router,
            instance,
            forwarder,
            resp_compress_min_length,
            auto_create_table,
            schema_config_provider,
            hotspot_recorder,
            engine_runtimes,
        }
    }

    pub fn instance(&self) -> InstanceRef<Q> {
        self.instance.clone()
    }

    async fn maybe_forward_sql_query(
        &self,
        req: &SqlQueryRequest,
    ) -> Result<Option<ForwardResult<SqlQueryResponse, Error>>> {
        let table_name = frontend::parse_table_name_with_sql(&req.sql)
            .box_err()
            .with_context(|| Internal {
                msg: format!("Failed to parse table name with sql, sql:{}", req.sql),
            })?;
        if table_name.is_none() {
            warn!("Unable to forward sql query without table name, req:{req:?}",);
            return Ok(None);
        }

        let req_ctx = req.context.as_ref().unwrap();
        let forward_req = ForwardRequest {
            schema: req_ctx.database.clone(),
            table: table_name.unwrap(),
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
        Ok(match forward_result {
            Ok(forward_res) => Some(forward_res),
            Err(e) => {
                error!("Failed to forward sql req but the error is ignored, err:{e}");
                None
            }
        })
    }

    async fn maybe_forward_prom_remote_query(
        &self,
        metric: String,
        req: PrometheusRemoteQueryRequest,
    ) -> Result<Option<ForwardResult<PrometheusRemoteQueryResponse, Error>>> {
        let req_ctx = req.context.as_ref().unwrap();
        let forward_req = ForwardRequest {
            schema: req_ctx.database.clone(),
            table: metric,
            req: req.into_request(),
        };
        let do_query = |mut client: StorageServiceClient<Channel>,
                        request: tonic::Request<PrometheusRemoteQueryRequest>,
                        _: &Endpoint| {
            let query = async move {
                client
                    .prom_remote_query(request)
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
        Ok(match forward_result {
            Ok(forward_res) => Some(forward_res),
            Err(e) => {
                error!("Failed to forward prom req but the error is ignored, err:{e}");
                None
            }
        })
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
        let first_sub_partition_table_name = util::get_sub_partition_name(
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
            options: table.options,
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

    async fn write_request_to_insert_plan(
        &self,
        table_requests: Vec<WriteTableRequest>,
        schema_config: Option<&SchemaConfig>,
        write_context: WriteContext,
    ) -> Result<Vec<InsertPlan>> {
        let mut plan_vec = Vec::with_capacity(table_requests.len());

        let WriteContext {
            request_id,
            catalog,
            schema,
            deadline,
            auto_create_table,
        } = write_context;
        let schema_config = schema_config.cloned().unwrap_or_default();
        for write_table_req in table_requests {
            let table_name = &write_table_req.table;
            self.maybe_open_partition_table_if_not_exist(&catalog, &schema, table_name)
                .await?;
            let mut table = try_get_table(&catalog, &schema, self.instance.clone(), table_name)?;

            match table.clone() {
                None => {
                    if auto_create_table {
                        create_table(
                            request_id,
                            &catalog,
                            &schema,
                            self.instance.clone(),
                            &write_table_req,
                            &schema_config,
                            deadline,
                        )
                        .await?;
                        // try to get table again
                        table =
                            try_get_table(&catalog, &schema, self.instance.clone(), table_name)?;
                    }
                }
                Some(t) => {
                    if auto_create_table {
                        // The reasons for making the decision to add columns before writing are as
                        // follows:
                        // * If judged based on the error message returned, it may cause data that
                        //   has already been successfully written to be written again and affect
                        //   the accuracy of the data.
                        // * Currently, the decision to add columns is made at the request level,
                        //   not at the row level, so the cost is relatively small.
                        let table_schema = t.schema();
                        let columns =
                            find_new_columns(&table_schema, &schema_config, &write_table_req)?;
                        if !columns.is_empty() {
                            execute_add_columns_plan(
                                request_id,
                                &catalog,
                                &schema,
                                self.instance.clone(),
                                t,
                                columns,
                                deadline,
                            )
                            .await?;
                        }
                    }
                }
            }

            match table {
                Some(table) => {
                    let plan = write_table_request_to_insert_plan(table, write_table_req)?;
                    plan_vec.push(plan);
                }
                None => {
                    return ErrNoCause {
                        code: StatusCode::BAD_REQUEST,
                        msg: format!("Table not found, schema:{schema}, table:{table_name}"),
                    }
                    .fail();
                }
            }
        }

        Ok(plan_vec)
    }
}

#[derive(Clone)]
pub struct Context {
    pub timeout: Option<Duration>,
    pub runtime: Arc<Runtime>,
}

async fn execute_plan<Q: QueryExecutor + 'static>(
    request_id: RequestId,
    catalog: &str,
    schema: &str,
    instance: InstanceRef<Q>,
    plan: Plan,
    deadline: Option<Instant>,
) -> Result<Output> {
    instance
        .limiter
        .try_limit(&plan)
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: "Request is blocked",
        })?;

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
        .context(ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: "Failed to create interpreter",
        })?;

    if let Some(deadline) = deadline {
        tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            interpreter.execute(),
        )
        .await
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: "Plan execution timeout",
        })
        .and_then(|v| {
            v.box_err().context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "Failed to execute interpreter",
            })
        })
    } else {
        interpreter.execute().await.box_err().context(ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: "Failed to execute interpreter",
        })
    }
}

fn try_get_table<Q: QueryExecutor + 'static>(
    catalog: &str,
    schema: &str,
    instance: InstanceRef<Q>,
    table_name: &str,
) -> Result<Option<TableRef>> {
    instance
        .catalog_manager
        .catalog_by_name(catalog)
        .box_err()
        .with_context(|| ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: format!("Failed to find catalog, catalog_name:{catalog}"),
        })?
        .with_context(|| ErrNoCause {
            code: StatusCode::BAD_REQUEST,
            msg: format!("Catalog not found, catalog_name:{catalog}"),
        })?
        .schema_by_name(schema)
        .box_err()
        .with_context(|| ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: format!("Failed to find schema, schema_name:{schema}"),
        })?
        .with_context(|| ErrNoCause {
            code: StatusCode::BAD_REQUEST,
            msg: format!("Schema not found, schema_name:{schema}"),
        })?
        .table_by_name(table_name)
        .box_err()
        .with_context(|| ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: format!("Failed to find table, table:{table_name}"),
        })
}

async fn create_table<Q: QueryExecutor + 'static>(
    request_id: RequestId,
    catalog: &str,
    schema: &str,
    instance: InstanceRef<Q>,
    write_table_req: &WriteTableRequest,
    schema_config: &SchemaConfig,
    deadline: Option<Instant>,
) -> Result<()> {
    let provider = CatalogMetaProvider {
        manager: instance.catalog_manager.clone(),
        default_catalog: catalog,
        default_schema: schema,
        function_registry: &*instance.function_registry,
    };
    let frontend = Frontend::new(provider);
    let mut ctx = FrontendContext::new(request_id, deadline);
    let plan = frontend
        .write_req_to_plan(&mut ctx, schema_config, write_table_req)
        .box_err()
        .with_context(|| ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: format!(
                "Failed to build creating table plan, table:{}",
                write_table_req.table
            ),
        })?;

    debug!("Grpc handle create table begin, plan:{:?}", plan);

    let output = execute_plan(request_id, catalog, schema, instance, plan, deadline).await;
    output.and_then(|output| match output {
        Output::AffectedRows(_) => Ok(()),
        Output::Records(_) => ErrNoCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: "Invalid output type, expect AffectedRows, found Records",
        }
        .fail(),
    })
}

fn write_table_request_to_insert_plan(
    table: TableRef,
    write_table_req: WriteTableRequest,
) -> Result<InsertPlan> {
    let schema = table.schema();

    let mut rows_total = Vec::new();
    for write_entry in write_table_req.entries {
        let mut rows = write_entry_to_rows(
            &write_table_req.table,
            &schema,
            &write_table_req.tag_names,
            &write_table_req.field_names,
            write_entry,
        )?;
        rows_total.append(&mut rows);
    }
    // The row group builder will checks nullable.
    let row_group = RowGroupBuilder::with_rows(schema, rows_total)
        .box_err()
        .with_context(|| ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: format!("Failed to build row group, table:{}", table.name()),
        })?
        .build();
    Ok(InsertPlan {
        table,
        rows: row_group,
        default_value_map: BTreeMap::new(),
    })
}

fn write_entry_to_rows(
    table_name: &str,
    schema: &Schema,
    tag_names: &[String],
    field_names: &[String],
    write_series_entry: WriteSeriesEntry,
) -> Result<Vec<Row>> {
    // Init all columns by null.
    let mut rows = vec![
        Row::from_datums(vec![Datum::Null; schema.num_columns()]);
        write_series_entry.field_groups.len()
    ];

    // Fill tsid by default value.
    if let Some(tsid_idx) = schema.index_of_tsid() {
        let kind = &schema.tsid_column().unwrap().data_type;
        let default_datum = Datum::empty(kind);
        for row in &mut rows {
            row[tsid_idx] = default_datum.clone();
        }
    }

    // Fill tags.
    for tag in write_series_entry.tags {
        let name_index = tag.name_index as usize;
        ensure!(
            name_index < tag_names.len(),
            ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!(
                    "Tag {tag:?} is not found in tag_names:{tag_names:?}, table:{table_name}",
                ),
            }
        );

        let tag_name = &tag_names[name_index];
        let tag_index_in_schema = schema.index_of(tag_name).with_context(|| ErrNoCause {
            code: StatusCode::BAD_REQUEST,
            msg: format!("Can't find tag({tag_name}) in schema, table:{table_name}"),
        })?;

        let column_schema = schema.column(tag_index_in_schema);
        ensure!(
            column_schema.is_tag,
            ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!("Column({tag_name}) is a field rather than a tag, table:{table_name}"),
            }
        );

        let tag_value = tag
            .value
            .with_context(|| ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!("Tag({tag_name}) value is needed, table:{table_name}"),
            })?
            .value
            .with_context(|| ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!(
                    "Tag({tag_name}) value type is not supported, table_name:{table_name}"
                ),
            })?;
        for row in &mut rows {
            row[tag_index_in_schema] = convert_proto_value_to_datum(
                table_name,
                tag_name,
                tag_value.clone(),
                column_schema.data_type,
            )?;
        }
    }

    // Fill fields.
    let mut field_name_index: HashMap<String, usize> = HashMap::new();
    for (i, field_group) in write_series_entry.field_groups.into_iter().enumerate() {
        // timestamp
        let timestamp_index_in_schema = schema.timestamp_index();
        rows[i][timestamp_index_in_schema] =
            Datum::Timestamp(Timestamp::new(field_group.timestamp));

        for field in field_group.fields {
            if (field.name_index as usize) < field_names.len() {
                let field_name = &field_names[field.name_index as usize];
                let index_in_schema = if field_name_index.contains_key(field_name) {
                    field_name_index.get(field_name).unwrap().to_owned()
                } else {
                    let index_in_schema =
                        schema.index_of(field_name).with_context(|| ErrNoCause {
                            code: StatusCode::BAD_REQUEST,
                            msg: format!(
                                "Can't find field in schema, table:{table_name}, field_name:{field_name}"
                            ),
                        })?;
                    field_name_index.insert(field_name.to_string(), index_in_schema);
                    index_in_schema
                };
                let column_schema = schema.column(index_in_schema);
                ensure!(
                    !column_schema.is_tag,
                    ErrNoCause {
                        code: StatusCode::BAD_REQUEST,
                        msg: format!(
                            "Column {field_name} is a tag rather than a field, table:{table_name}"
                        )
                    }
                );
                let field_value = field
                    .value
                    .with_context(|| ErrNoCause {
                        code: StatusCode::BAD_REQUEST,
                        msg: format!("Field({field_name}) is needed, table:{table_name}"),
                    })?
                    .value
                    .with_context(|| ErrNoCause {
                        code: StatusCode::BAD_REQUEST,
                        msg: format!(
                            "Field({field_name}) value type is not supported, table:{table_name}"
                        ),
                    })?;

                rows[i][index_in_schema] = convert_proto_value_to_datum(
                    table_name,
                    field_name,
                    field_value,
                    column_schema.data_type,
                )?;
            }
        }
    }

    Ok(rows)
}

/// Convert the `Value_oneof_value` defined in protos into the datum.
fn convert_proto_value_to_datum(
    table_name: &str,
    name: &str,
    value: value::Value,
    data_type: DatumKind,
) -> Result<Datum> {
    match (value, data_type) {
        (value::Value::Float64Value(v), DatumKind::Double) => Ok(Datum::Double(v)),
        (value::Value::StringValue(v), DatumKind::String) => Ok(Datum::String(v.into())),
        (value::Value::Int64Value(v), DatumKind::Int64) => Ok(Datum::Int64(v)),
        (value::Value::Float32Value(v), DatumKind::Float) => Ok(Datum::Float(v)),
        (value::Value::Int32Value(v), DatumKind::Int32) => Ok(Datum::Int32(v)),
        (value::Value::Int16Value(v), DatumKind::Int16) => Ok(Datum::Int16(v as i16)),
        (value::Value::Int8Value(v), DatumKind::Int8) => Ok(Datum::Int8(v as i8)),
        (value::Value::BoolValue(v), DatumKind::Boolean) => Ok(Datum::Boolean(v)),
        (value::Value::Uint64Value(v), DatumKind::UInt64) => Ok(Datum::UInt64(v)),
        (value::Value::Uint32Value(v), DatumKind::UInt32) => Ok(Datum::UInt32(v)),
        (value::Value::Uint16Value(v), DatumKind::UInt16) => Ok(Datum::UInt16(v as u16)),
        (value::Value::Uint8Value(v), DatumKind::UInt8) => Ok(Datum::UInt8(v as u8)),
        (value::Value::TimestampValue(v), DatumKind::Timestamp) => Ok(Datum::Timestamp(Timestamp::new(v))),
        (value::Value::VarbinaryValue(v), DatumKind::Varbinary) => Ok(Datum::Varbinary(Bytes::from(v))),
        (v, _) => ErrNoCause {
            code: StatusCode::BAD_REQUEST,
            msg: format!(
                "Value type is not same, table:{table_name}, value_name:{name}, schema_type:{data_type:?}, actual_value:{v:?}"
            ),
        }
            .fail(),
    }
}

fn find_new_columns(
    schema: &Schema,
    schema_config: &SchemaConfig,
    write_req: &WriteTableRequest,
) -> Result<Vec<ColumnSchema>> {
    let new_schema = build_schema_from_write_table_request(schema_config, write_req)
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: "Build schema from write table request failed",
        })?;

    let columns = new_schema.columns();
    let old_columns = schema.columns();

    // find new columns:
    // 1. timestamp column can't be a new column;
    // 2. column not in old schema is a new column.
    let new_columns = columns
        .iter()
        .enumerate()
        .filter(|(idx, column)| {
            *idx != new_schema.timestamp_index()
                && !old_columns.iter().any(|c| c.name == column.name)
        })
        .map(|(_, column)| column.clone())
        .collect();
    Ok(new_columns)
}

async fn execute_add_columns_plan<Q: QueryExecutor + 'static>(
    request_id: RequestId,
    catalog: &str,
    schema: &str,
    instance: InstanceRef<Q>,
    table: TableRef,
    columns: Vec<ColumnSchema>,
    deadline: Option<Instant>,
) -> Result<()> {
    let table_name = table.name().to_string();
    info!("Add columns start, request_id:{request_id}, table:{table_name}, columns:{columns:?}");

    let plan = Plan::AlterTable(AlterTablePlan {
        table,
        operations: AlterTableOperation::AddColumn(columns),
    });
    let _ = execute_plan(request_id, catalog, schema, instance, plan, deadline).await?;

    info!("Add columns success, request_id:{request_id}, table:{table_name}");
    Ok(())
}

#[cfg(test)]
mod test {
    use catalog::schema::Schema;
    use ceresdbproto::storage::{value, Field, FieldGroup, Tag, Value, WriteSeriesEntry};
    use common_types::{
        column_schema::{self, ColumnSchema},
        datum::{Datum, DatumKind},
        row::Row,
        schema::Builder,
        time::Timestamp,
    };
    use system_catalog::sys_catalog_table::TIMESTAMP_COLUMN_NAME;

    use super::*;

    const TAG_K: &str = "tagk";
    const TAG_V: &str = "tagv";
    const TAG_K1: &str = "tagk1";
    const TAG_V1: &str = "tagv1";
    const FIELD_NAME: &str = "field";
    const FIELD_NAME1: &str = "field1";
    const FIELD_VALUE_STRING: &str = "stringValue";

    // tag_names field_names write_entry
    fn generate_write_entry() -> (Schema, Vec<String>, Vec<String>, WriteSeriesEntry) {
        let tag_names = vec![TAG_K.to_string(), TAG_K1.to_string()];
        let field_names = vec![FIELD_NAME.to_string(), FIELD_NAME1.to_string()];

        let tag = Tag {
            name_index: 0,
            value: Some(Value {
                value: Some(value::Value::StringValue(TAG_V.to_string())),
            }),
        };
        let tag1 = Tag {
            name_index: 1,
            value: Some(Value {
                value: Some(value::Value::StringValue(TAG_V1.to_string())),
            }),
        };
        let tags = vec![tag, tag1];

        let field = Field {
            name_index: 0,
            value: Some(Value {
                value: Some(value::Value::Float64Value(100.0)),
            }),
        };
        let field1 = Field {
            name_index: 1,
            value: Some(Value {
                value: Some(value::Value::StringValue(FIELD_VALUE_STRING.to_string())),
            }),
        };
        let field_group = FieldGroup {
            timestamp: 1000,
            fields: vec![field],
        };
        let field_group1 = FieldGroup {
            timestamp: 2000,
            fields: vec![field1.clone()],
        };
        let field_group2 = FieldGroup {
            timestamp: 3000,
            fields: vec![field1],
        };

        let write_entry = WriteSeriesEntry {
            tags,
            field_groups: vec![field_group, field_group1, field_group2],
        };

        let schema_builder = Builder::new();
        let schema = schema_builder
            .auto_increment_column_id(true)
            .add_key_column(ColumnSchema {
                id: column_schema::COLUMN_ID_UNINIT,
                name: TIMESTAMP_COLUMN_NAME.to_string(),
                data_type: DatumKind::Timestamp,
                is_nullable: false,
                is_tag: false,
                comment: String::new(),
                escaped_name: TIMESTAMP_COLUMN_NAME.escape_debug().to_string(),
                default_value: None,
            })
            .unwrap()
            .add_key_column(ColumnSchema {
                id: column_schema::COLUMN_ID_UNINIT,
                name: TAG_K.to_string(),
                data_type: DatumKind::String,
                is_nullable: false,
                is_tag: true,
                comment: String::new(),
                escaped_name: TAG_K.escape_debug().to_string(),
                default_value: None,
            })
            .unwrap()
            .add_normal_column(ColumnSchema {
                id: column_schema::COLUMN_ID_UNINIT,
                name: TAG_K1.to_string(),
                data_type: DatumKind::String,
                is_nullable: false,
                is_tag: true,
                comment: String::new(),
                escaped_name: TAG_K1.escape_debug().to_string(),
                default_value: None,
            })
            .unwrap()
            .add_normal_column(ColumnSchema {
                id: column_schema::COLUMN_ID_UNINIT,
                name: FIELD_NAME.to_string(),
                data_type: DatumKind::Double,
                is_nullable: true,
                is_tag: false,
                comment: String::new(),
                escaped_name: FIELD_NAME.escape_debug().to_string(),
                default_value: None,
            })
            .unwrap()
            .add_normal_column(ColumnSchema {
                id: column_schema::COLUMN_ID_UNINIT,
                name: FIELD_NAME1.to_string(),
                data_type: DatumKind::String,
                is_nullable: true,
                is_tag: false,
                comment: String::new(),
                escaped_name: FIELD_NAME1.escape_debug().to_string(),
                default_value: None,
            })
            .unwrap()
            .build()
            .unwrap();
        (schema, tag_names, field_names, write_entry)
    }

    #[test]
    fn test_write_entry_to_row_group() {
        let (schema, tag_names, field_names, write_entry) = generate_write_entry();
        let rows =
            write_entry_to_rows("test_table", &schema, &tag_names, &field_names, write_entry)
                .unwrap();
        let row0 = vec![
            Datum::Timestamp(Timestamp::new(1000)),
            Datum::String(TAG_V.into()),
            Datum::String(TAG_V1.into()),
            Datum::Double(100.0),
            Datum::Null,
        ];
        let row1 = vec![
            Datum::Timestamp(Timestamp::new(2000)),
            Datum::String(TAG_V.into()),
            Datum::String(TAG_V1.into()),
            Datum::Null,
            Datum::String(FIELD_VALUE_STRING.into()),
        ];
        let row2 = vec![
            Datum::Timestamp(Timestamp::new(3000)),
            Datum::String(TAG_V.into()),
            Datum::String(TAG_V1.into()),
            Datum::Null,
            Datum::String(FIELD_VALUE_STRING.into()),
        ];

        let expect_rows = vec![
            Row::from_datums(row0),
            Row::from_datums(row1),
            Row::from_datums(row2),
        ];
        assert_eq!(rows, expect_rows);
    }
}
