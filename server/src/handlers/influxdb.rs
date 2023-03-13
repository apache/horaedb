// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! This module implements [write][1] and [query][2] for InfluxDB.
//! [1]: https://docs.influxdata.com/influxdb/v1.8/tools/api/#write-http-endpoint
//! [2]: https://docs.influxdata.com/influxdb/v1.8/tools/api/#query-http-endpoint

use std::{collections::HashMap, sync::Arc, time::Instant};

use bytes::Bytes;
use ceresdbproto::storage::{
    value, Field, FieldGroup, Tag, Value, WriteSeriesEntry, WriteTableRequest,
};
use common_types::{request_id::RequestId, time::Timestamp};
use common_util::error::BoxError;
use handlers::{
    error::{InfluxdbHandler, Result},
    query::QueryRequest,
};
use influxdb_line_protocol::FieldValue;
use log::debug;
use query_engine::executor::Executor as QueryExecutor;
use snafu::ResultExt;
use warp::{reject, reply, Rejection, Reply};

use crate::{
    context::RequestContext, grpc::storage_service::write::WriteContext, handlers,
    instance::InstanceRef, schema_config_provider::SchemaConfigProviderRef,
};

pub struct Influxdb<Q> {
    instance: InstanceRef<Q>,
    schema_config_provider: SchemaConfigProviderRef,
}

#[derive(Debug, Default)]
pub enum Precision {
    #[default]
    Millisecond,
    // TODO: parse precision `second` from HTTP API
    #[allow(dead_code)]
    Second,
}

impl Precision {
    fn normalize(&self, ts: i64) -> i64 {
        match self {
            Self::Millisecond => ts,
            Self::Second => ts * 1000,
        }
    }
}

/// Line protocol
#[derive(Debug)]
pub struct WriteRequest {
    pub lines: String,
    pub precision: Precision,
}

impl From<Bytes> for WriteRequest {
    fn from(bytes: Bytes) -> Self {
        WriteRequest {
            lines: String::from_utf8_lossy(&bytes).to_string(),
            precision: Default::default(),
        }
    }
}

pub type WriteResponse = ();

impl<Q: QueryExecutor + 'static> Influxdb<Q> {
    pub fn new(instance: InstanceRef<Q>, schema_config_provider: SchemaConfigProviderRef) -> Self {
        Self {
            instance,
            schema_config_provider,
        }
    }

    async fn query(
        &self,
        ctx: RequestContext,
        req: QueryRequest,
    ) -> Result<handlers::query::Response> {
        handlers::query::handle_query(&ctx, self.instance.clone(), req)
            .await
            .map(handlers::query::convert_output)
    }

    async fn write(&self, ctx: RequestContext, req: WriteRequest) -> Result<WriteResponse> {
        let request_id = RequestId::next_id();
        let deadline = ctx.timeout.map(|t| Instant::now() + t);
        let catalog = &ctx.catalog;
        self.instance.catalog_manager.default_catalog_name();
        let schema = &ctx.schema;
        let schema_config = self
            .schema_config_provider
            .schema_config(schema)
            .box_err()
            .with_context(|| InfluxdbHandler {
                msg: format!("get schema config failed, schema:{schema}"),
            })?;

        let write_context =
            WriteContext::new(request_id, deadline, catalog.clone(), schema.clone());

        let plans = crate::grpc::storage_service::write::write_request_to_insert_plan(
            self.instance.clone(),
            convert_write_request(req)?,
            schema_config,
            write_context,
        )
        .await
        .box_err()
        .with_context(|| InfluxdbHandler {
            msg: "write request to insert plan",
        })?;

        let mut success = 0;
        for insert_plan in plans {
            success += crate::grpc::storage_service::write::execute_plan(
                request_id,
                catalog,
                schema,
                self.instance.clone(),
                insert_plan,
                deadline,
            )
            .await
            .box_err()
            .with_context(|| InfluxdbHandler {
                msg: "execute plan",
            })?;
        }
        debug!(
            "Remote write finished, catalog:{}, schema:{}, success:{}",
            catalog, schema, success
        );

        Ok(())
    }
}

fn convert_write_request(req: WriteRequest) -> Result<Vec<WriteTableRequest>> {
    let mut req_by_measurement = HashMap::new();
    let default_ts = Timestamp::now().as_i64();
    for line in influxdb_line_protocol::parse_lines(&req.lines) {
        let mut line = line.box_err().with_context(|| InfluxdbHandler {
            msg: "invalid line",
        })?;

        let timestamp = line
            .timestamp
            .map_or_else(|| default_ts, |ts| req.precision.normalize(ts));
        let mut tag_set = line.series.tag_set.unwrap_or_default();
        // sort by tag key
        tag_set.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        // sort by field key
        line.field_set.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        let req_for_one_measurement = req_by_measurement
            .entry(line.series.measurement.to_string())
            .or_insert_with(|| WriteTableRequest {
                table: line.series.measurement.to_string(),
                tag_names: tag_set.iter().map(|(tagk, _)| tagk.to_string()).collect(),
                field_names: line
                    .field_set
                    .iter()
                    .map(|(tagk, _)| tagk.to_string())
                    .collect(),
                entries: Vec::new(),
            });

        let tags: Vec<_> = tag_set
            .iter()
            .enumerate()
            .map(|(idx, (_, tagv))| Tag {
                name_index: idx as u32,
                value: Some(Value {
                    value: Some(value::Value::StringValue(tagv.to_string())),
                }),
            })
            .collect();
        let field_group = FieldGroup {
            timestamp,
            fields: line
                .field_set
                .iter()
                .cloned()
                .enumerate()
                .map(|(idx, (_, fieldv))| Field {
                    name_index: idx as u32,
                    value: Some(convert_influx_value(fieldv)),
                })
                .collect(),
        };
        let mut found = false;
        for entry in &mut req_for_one_measurement.entries {
            if entry.tags == tags {
                // TODO: remove clone?
                entry.field_groups.push(field_group.clone());
                found = true;
                break;
            }
        }
        if !found {
            req_for_one_measurement.entries.push(WriteSeriesEntry {
                tags,
                field_groups: vec![field_group],
            })
        }
    }

    Ok(req_by_measurement.into_values().collect())
}

/// Convert influxdb's FieldValue to ceresdbproto's Value
fn convert_influx_value(field_value: FieldValue) -> Value {
    let v = match field_value {
        FieldValue::I64(v) => value::Value::Int64Value(v),
        FieldValue::U64(v) => value::Value::Uint64Value(v),
        FieldValue::F64(v) => value::Value::Float64Value(v),
        FieldValue::String(v) => value::Value::StringValue(v.to_string()),
        FieldValue::Boolean(v) => value::Value::BoolValue(v),
    };

    Value { value: Some(v) }
}

// TODO: Request and response type don't match influxdb's API now.
pub async fn query<Q: QueryExecutor + 'static>(
    ctx: RequestContext,
    db: Arc<Influxdb<Q>>,
    req: QueryRequest,
) -> std::result::Result<impl Reply, Rejection> {
    db.query(ctx, req)
        .await
        .map_err(reject::custom)
        .map(|v| reply::json(&v))
}

// TODO: Request and response type don't match influxdb's API now.
pub async fn write<Q: QueryExecutor + 'static>(
    ctx: RequestContext,
    db: Arc<Influxdb<Q>>,
    req: WriteRequest,
) -> std::result::Result<impl Reply, Rejection> {
    db.write(ctx, req)
        .await
        .map_err(reject::custom)
        .map(|_| reply::reply())
}
