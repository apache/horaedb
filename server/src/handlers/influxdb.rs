// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! This module implements [write][1] and [query][2] for InfluxDB.
//! [1]: https://docs.influxdata.com/influxdb/v1.8/tools/api/#write-http-endpoint
//! [2]: https://docs.influxdata.com/influxdb/v1.8/tools/api/#query-http-endpoint

use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use ceresdbproto::storage::{value, FieldGroup, Tag, Value, WriteSeriesEntry, WriteTableRequest};
use common_types::time::Timestamp;
use common_util::error::BoxError;
use handlers::{
    error::{InfluxdbHandler, Result},
    query::QueryRequest,
};
use query_engine::executor::Executor as QueryExecutor;
use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};
use warp::{reject, reply, Rejection, Reply};

use crate::{
    context::RequestContext, handlers, instance::InstanceRef,
    schema_config_provider::SchemaConfigProviderRef,
};

pub struct Influxdb<Q> {
    instance: InstanceRef<Q>,
    #[allow(dead_code)]
    schema_config_provider: SchemaConfigProviderRef,
}

#[derive(Debug, Default)]
pub enum Precision {
    #[default]
    Millisecond,
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
        todo!()
    }
}

fn convert_write_req(req: WriteRequest) -> Result<Vec<WriteTableRequest>> {
    let mut req_by_measurement = HashMap::new();
    let default_ts = Timestamp::now().as_i64();
    for line in influxdb_line_protocol::parse_lines(&req.lines) {
        let mut line = line
            .box_err()
            .with_context(|| InfluxdbHandler { msg: "valid line" })?;

        let timestamp = line
            .timestamp
            .map_or_else(|| default_ts, |ts| req.precision.normalize(ts));
        let mut tag_set = line.series.tag_set.unwrap_or_default();
        // sort by tag key
        tag_set.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        // sort by field key
        line.field_set.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        req_by_measurement
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
            })
            .entries
            .push(WriteSeriesEntry {
                tags: tag_set
                    .iter()
                    .enumerate()
                    .map(|(idx, (_, tagv))| Tag {
                        name_index: idx as u32,
                        value: Some(Value {
                            value: Some(value::Value::StringValue(tagv.to_string())),
                        }),
                    })
                    .collect(),
                field_groups: line
                    .field_set
                    .iter()
                    .map(|(_, fieldv)| FieldGroup {
                        timestamp,
                        fields: vec![],
                    })
                    .collect(),
            });
    }
    todo!()
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
