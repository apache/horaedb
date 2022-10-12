// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Write handler

use std::collections::{BTreeMap, HashMap};

use ceresdbproto::storage::{value, WriteEntry, WriteMetric, WriteRequest, WriteResponse};
use common_types::{
    bytes::Bytes,
    datum::{Datum, DatumKind},
    request_id::RequestId,
    row::{Row, RowGroupBuilder},
    schema::Schema,
    time::Timestamp,
};
use http::StatusCode;
use interpreters::{context::Context as InterpreterContext, factory::Factory, interpreter::Output};
use log::debug;
use query_engine::executor::Executor as QueryExecutor;
use snafu::{ensure, OptionExt, ResultExt};
use sql::plan::{InsertPlan, Plan};
use table_engine::table::TableRef;

use crate::{
    error::{ErrNoCause, ErrWithCause, Result},
    grpc::{self, HandlerContext},
};

pub(crate) async fn handle_write<Q: QueryExecutor + 'static>(
    ctx: &HandlerContext<'_, Q>,
    req: WriteRequest,
) -> Result<WriteResponse> {
    let request_id = RequestId::next_id();

    debug!(
        "Grpc handle write begin, catalog:{}, tenant:{}, request_id:{}, first_table:{:?}, num_tables:{}",
        ctx.catalog(),
        ctx.tenant(),
        request_id,
        req.metrics
            .first()
            .map(|m| (&m.metric, &m.tag_names, &m.field_names)),
        req.metrics.len(),
    );

    let instance = &ctx.instance;
    let plan_vec = write_request_to_insert_plan(ctx, req, request_id).await?;

    let mut success = 0;
    for insert_plan in plan_vec {
        debug!(
            "Grpc handle write table begin, table:{}, row_num:{}",
            insert_plan.table.name(),
            insert_plan.rows.num_rows()
        );
        let plan = Plan::Insert(insert_plan);

        if ctx.instance.limiter.should_limit(&plan) {
            ErrNoCause {
                code: StatusCode::TOO_MANY_REQUESTS,
                msg: "Insert limited by reject list",
            }
            .fail()?;
        }

        let interpreter_ctx = InterpreterContext::builder(request_id)
            // Use current ctx's catalog and tenant as default catalog and tenant
            .default_catalog_and_schema(ctx.catalog().to_string(), ctx.tenant().to_string())
            .build();
        let interpreter_factory = Factory::new(
            instance.query_executor.clone(),
            instance.catalog_manager.clone(),
            instance.table_engine.clone(),
        );
        let interpreter = interpreter_factory.create(interpreter_ctx, plan);

        let row_num = match interpreter
            .execute()
            .await
            .map_err(|e| Box::new(e) as _)
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "Failed to execute interpreter",
            })? {
            Output::AffectedRows(n) => n,
            _ => unreachable!(),
        };

        success += row_num;
    }

    let resp = WriteResponse {
        header: Some(grpc::build_ok_header()),
        success: success as u32,
        failed: 0,
    };

    debug!(
        "Grpc handle write finished, catalog:{}, tenant:{}, resp:{:?}",
        ctx.catalog(),
        ctx.tenant(),
        resp
    );

    Ok(resp)
}

async fn write_request_to_insert_plan<Q: QueryExecutor + 'static>(
    ctx: &HandlerContext<'_, Q>,
    write_request: WriteRequest,
    request_id: RequestId,
) -> Result<Vec<InsertPlan>> {
    let mut plan_vec = Vec::with_capacity(write_request.metrics.len());

    for write_metric in write_request.metrics {
        let table_name = &write_metric.metric;
        let mut table = try_get_table(ctx, table_name)?;

        if table.is_none() {
            if let Some(config) = ctx.schema_config {
                if config.auto_create_tables {
                    create_table(ctx, &write_metric, request_id).await?;
                    // try to get table again
                    table = try_get_table(ctx, table_name)?;
                }
            }
        }

        match table {
            Some(table) => {
                let plan = write_metric_to_insert_plan(table, write_metric)?;
                plan_vec.push(plan);
            }
            None => {
                return ErrNoCause {
                    code: StatusCode::BAD_REQUEST,
                    msg: format!(
                        "Table not found, tenant:{}, table:{}",
                        ctx.tenant(),
                        table_name
                    ),
                }
                .fail();
            }
        }
    }

    Ok(plan_vec)
}

fn try_get_table<Q: QueryExecutor + 'static>(
    ctx: &HandlerContext<'_, Q>,
    table_name: &str,
) -> Result<Option<TableRef>> {
    ctx.instance
        .catalog_manager
        .catalog_by_name(ctx.catalog())
        .map_err(|e| Box::new(e) as _)
        .with_context(|| ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: format!("Failed to find catalog, catalog_name:{}", ctx.catalog()),
        })?
        .with_context(|| ErrNoCause {
            code: StatusCode::BAD_REQUEST,
            msg: format!("Catalog not found, catalog_name:{}", ctx.catalog()),
        })?
        .schema_by_name(ctx.tenant())
        .map_err(|e| Box::new(e) as _)
        .with_context(|| ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: format!("Failed to find tenant, tenant_name:{}", ctx.tenant()),
        })?
        .with_context(|| ErrNoCause {
            code: StatusCode::BAD_REQUEST,
            msg: format!("Tenant not found, tenant_name:{}", ctx.tenant()),
        })?
        .table_by_name(table_name)
        .map_err(|e| Box::new(e) as _)
        .with_context(|| ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: format!("Failed to find table, table:{}", table_name),
        })
}

async fn create_table<Q: QueryExecutor + 'static>(
    ctx: &HandlerContext<'_, Q>,
    write_metric: &WriteMetric,
    request_id: RequestId,
) -> Result<()> {
    let create_table_plan = grpc::write_metric_to_create_table_plan(ctx, write_metric)
        .map_err(|e| Box::new(e) as _)
        .with_context(|| ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: format!(
                "Failed to build creating table plan from metric, table:{}",
                write_metric.metric
            ),
        })?;

    debug!(
        "Grpc handle create table begin, table:{}, schema:{:?}",
        create_table_plan.table, create_table_plan.table_schema,
    );
    let plan = Plan::Create(create_table_plan);

    let instance = &ctx.instance;

    if instance.limiter.should_limit(&plan) {
        ErrNoCause {
            code: StatusCode::TOO_MANY_REQUESTS,
            msg: "Create table limited by reject list",
        }
        .fail()?;
    }

    let interpreter_ctx = InterpreterContext::builder(request_id)
        // Use current ctx's catalog and tenant as default catalog and tenant
        .default_catalog_and_schema(ctx.catalog().to_string(), ctx.tenant().to_string())
        .build();
    let interpreter_factory = Factory::new(
        instance.query_executor.clone(),
        instance.catalog_manager.clone(),
        instance.table_engine.clone(),
    );
    let interpreter = interpreter_factory.create(interpreter_ctx, plan);

    let _ = match interpreter
        .execute()
        .await
        .map_err(|e| Box::new(e) as _)
        .context(ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: "Failed to execute interpreter",
        })? {
        Output::AffectedRows(n) => n,
        _ => unreachable!(),
    };

    Ok(())
}

fn write_metric_to_insert_plan(table: TableRef, write_metric: WriteMetric) -> Result<InsertPlan> {
    let schema = table.schema();

    let mut rows_total = Vec::new();
    for write_entry in write_metric.entries {
        let mut rows = write_entry_to_rows(
            &write_metric.metric,
            &schema,
            &write_metric.tag_names,
            &write_metric.field_names,
            write_entry,
        )?;
        rows_total.append(&mut rows);
    }
    // The row group builder will checks nullable.
    let row_group = RowGroupBuilder::with_rows(schema, rows_total)
        .map_err(|e| Box::new(e) as _)
        .context(ErrWithCause {
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
    write_entry: WriteEntry,
) -> Result<Vec<Row>> {
    // Init all columns by null.
    let mut rows = vec![
        Row::from_datums(vec![Datum::Null; schema.num_columns()]);
        write_entry.field_groups.len()
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
    for tag in write_entry.tags {
        let name_index = tag.name_index as usize;
        ensure!(
            name_index < tag_names.len(),
            ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!(
                    "tag index {} is not found in tag_names:{:?}, table:{}",
                    name_index, tag_names, table_name,
                ),
            }
        );

        let tag_name = &tag_names[name_index];
        let tag_index_in_schema = schema.index_of(tag_name).with_context(|| ErrNoCause {
            code: StatusCode::BAD_REQUEST,
            msg: format!(
                "Can't find tag({}) in schema, table:{}",
                tag_name, table_name
            ),
        })?;

        let column_schema = schema.column(tag_index_in_schema);
        ensure!(
            column_schema.is_tag,
            ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!(
                    "column({}) is a field rather than a tag, table:{}",
                    tag_name, table_name
                ),
            }
        );

        let tag_value = tag
            .value
            .with_context(|| ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!("Tag({}) value is needed, table:{}", tag_name, table_name),
            })?
            .value
            .with_context(|| ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!(
                    "Tag({}) value type is not supported, table_name:{}",
                    tag_name, table_name
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
    for (i, field_group) in write_entry.field_groups.into_iter().enumerate() {
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
                                "Can't find field in schema, table:{}, field_name:{}",
                                table_name, field_name
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
                            "Column {} is a tag rather than a field, table:{}",
                            field_name, table_name
                        )
                    }
                );
                let field_value = field
                    .value
                    .with_context(|| ErrNoCause {
                        code: StatusCode::BAD_REQUEST,
                        msg: format!("Field({}) is needed, table:{}", field_name, table_name),
                    })?
                    .value
                    .with_context(|| ErrNoCause {
                        code: StatusCode::BAD_REQUEST,
                        msg: format!(
                            "Field({}) value type is not supported, table:{}",
                            field_name, table_name
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
                "Value type is not same, table:{}, value_name:{}, schema_type:{:?}, actual_value:{:?}",
                table_name,
                name,
                data_type,
                v
            ),
        }
        .fail(),
    }
}

#[cfg(test)]
mod test {
    use ceresdbproto::storage::{Field, FieldGroup, Tag, Value};
    use common_types::{
        column_schema::{self, ColumnSchema},
        schema::Builder,
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
    fn generate_write_entry() -> (Schema, Vec<String>, Vec<String>, WriteEntry) {
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

        let write_entry = WriteEntry {
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
