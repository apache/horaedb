// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! This module implements prometheus remote storage API.
//! It converts write request to gRPC write request, and
//! translates query request to SQL for execution.

use std::{
    collections::HashMap,
    result::Result as StdResult,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use catalog::consts::DEFAULT_CATALOG;
use ceresdbproto::storage::{
    value, Field, FieldGroup, PrometheusRemoteQueryRequest, PrometheusRemoteQueryResponse, Tag,
    Value, WriteSeriesEntry, WriteTableRequest,
};
use common_types::{
    datum::DatumKind,
    request_id::RequestId,
    schema::{RecordSchema, TIMESTAMP_COLUMN, TSID_COLUMN},
};
use common_util::{error::BoxError, runtime::Runtime, time::InstantExt};
use http::StatusCode;
use interpreters::interpreter::Output;
use log::{debug, info};
use prom_remote_api::types::{
    Label, LabelMatcher, Query, QueryResult, RemoteStorage, Sample, TimeSeries, WriteRequest,
};
use prost::Message;
use query_engine::executor::{Executor as QueryExecutor, RecordBatchVec};
use query_frontend::{
    frontend::{Context, Frontend},
    provider::CatalogMetaProvider,
};
use snafu::{ensure, OptionExt, ResultExt};
use warp::reject;

use crate::{
    context::RequestContext,
    proxy::{
        error::{
            build_ok_header, ErrNoCause, ErrWithCause, Error, Internal, InternalNoCause, Result,
        },
        execute_plan,
        forward::ForwardResult,
        grpc::write::{execute_insert_plan, write_request_to_insert_plan, WriteContext},
        Proxy,
    },
};

const NAME_LABEL: &str = "__name__";
const VALUE_COLUMN: &str = "value";

impl reject::Reject for Error {}

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    /// Handle write samples to remote storage with remote storage protocol.
    async fn handle_prom_write(&self, ctx: RequestContext, req: WriteRequest) -> Result<()> {
        let request_id = RequestId::next_id();
        let deadline = ctx.timeout.map(|t| Instant::now() + t);
        let catalog = &ctx.catalog;
        self.instance.catalog_manager.default_catalog_name();
        let schema = &ctx.schema;
        let schema_config = self
            .schema_config_provider
            .schema_config(schema)
            .box_err()
            .context(Internal {
                msg: "Failed to get schema",
            })?;
        let write_context =
            WriteContext::new(request_id, deadline, catalog.clone(), schema.clone());
        let plans = write_request_to_insert_plan(
            self.instance.clone(),
            convert_write_request(req)?,
            schema_config,
            write_context,
        )
        .await
        .box_err()
        .context(Internal {
            msg: "Failed to write via gRPC",
        })?;

        let mut success = 0;
        for insert_plan in plans {
            success += execute_insert_plan(
                request_id,
                catalog,
                schema,
                self.instance.clone(),
                insert_plan,
                deadline,
            )
            .await
            .box_err()
            .context(Internal {
                msg: "Failed to write via gRPC",
            })?;
        }

        debug!(
            "Remote write finished, catalog:{}, schema:{}, success:{}",
            catalog, schema, success
        );

        Ok(())
    }

    /// Handle one query with remote storage protocol.
    async fn handle_prom_process_query(
        &self,
        ctx: &RequestContext,
        metric: String,
        query: Query,
    ) -> Result<QueryResult> {
        // Open partition table if needed.
        self.maybe_open_partition_table_if_not_exist(&ctx.catalog, &ctx.schema, &metric)
            .await?;

        let request_id = RequestId::next_id();
        let begin_instant = Instant::now();
        let deadline = ctx.timeout.map(|t| begin_instant + t);

        info!(
            "Query handler try to process request, request_id:{}, request:{:?}",
            request_id, query
        );

        // TODO(yingwen): Privilege check, cannot access data of other tenant
        // TODO(yingwen): Maybe move MetaProvider to instance
        let provider = CatalogMetaProvider {
            manager: self.instance.catalog_manager.clone(),
            default_catalog: &ctx.catalog,
            default_schema: &ctx.schema,
            function_registry: &*self.instance.function_registry,
        };
        let frontend = Frontend::new(provider);
        let mut plan_ctx = Context::new(request_id, deadline);

        let plan = frontend
            .prom_remote_query_to_plan(&mut plan_ctx, query.clone())
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!("Failed to build plan, query:{query:?}"),
            })?;

        self.instance
            .limiter
            .try_limit(&plan)
            .box_err()
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "Query is blocked",
            })?;
        let output = execute_plan(
            request_id,
            &ctx.catalog,
            &ctx.schema,
            self.instance.clone(),
            plan,
            deadline,
        )
        .await?;

        info!(
            "Query handler finished, request_id:{}, cost:{}ms, query:{:?}",
            request_id,
            begin_instant.saturating_elapsed().as_millis(),
            query
        );

        convert_query_result(metric, output)
    }

    /// This method is used to handle forwarded gRPC query from
    /// another CeresDB instance.
    pub async fn handle_prom_grpc_query(
        &self,
        timeout: Option<Duration>,
        req: PrometheusRemoteQueryRequest,
        runtime: Arc<Runtime>,
    ) -> Result<PrometheusRemoteQueryResponse> {
        let ctx = req.context.context(ErrNoCause {
            code: StatusCode::BAD_REQUEST,
            msg: "request context is missing",
        })?;
        let database = ctx.database.to_string();
        let query = Query::decode(req.query.as_ref())
            .box_err()
            .context(Internal {
                msg: "decode query failed",
            })?;
        let metric = find_metric(&query.matchers)?;
        let builder = RequestContext::builder()
            .timeout(timeout)
            .runtime(runtime)
            .schema(database)
            // TODO: support different catalog
            .catalog(DEFAULT_CATALOG.to_string());
        let ctx = builder.build().box_err().context(Internal {
            msg: "build request context failed",
        })?;

        self.handle_prom_process_query(&ctx, metric, query)
            .await
            .map(|v| PrometheusRemoteQueryResponse {
                header: Some(build_ok_header()),
                response: v.encode_to_vec(),
            })
    }
}

#[async_trait]
impl<Q: QueryExecutor + 'static> RemoteStorage for Proxy<Q> {
    type Context = RequestContext;
    type Err = Error;

    async fn write(&self, ctx: Self::Context, req: WriteRequest) -> StdResult<(), Self::Err> {
        self.handle_prom_write(ctx, req).await
    }

    async fn process_query(
        &self,
        ctx: &Self::Context,
        query: Query,
    ) -> StdResult<QueryResult, Self::Err> {
        let metric = find_metric(&query.matchers)?;
        let remote_req = PrometheusRemoteQueryRequest {
            context: Some(ceresdbproto::storage::RequestContext {
                database: ctx.schema.to_string(),
            }),
            query: query.encode_to_vec(),
        };
        if let Some(resp) = self
            .maybe_forward_prom_remote_query(metric.clone(), remote_req)
            .await
            .map_err(|e| {
                log::info!("remote_req forward error {:?}", e);
                e
            })?
        {
            match resp {
                ForwardResult::Forwarded(resp) => {
                    return resp.and_then(|v| {
                        QueryResult::decode(v.response.as_ref())
                            .box_err()
                            .context(Internal {
                                msg: "decode QueryResult failed",
                            })
                    });
                }
                ForwardResult::Local => {}
            }
        }

        self.handle_prom_process_query(ctx, metric, query).await
    }
}

/// Converter converts Arrow's RecordBatch into Prometheus's QueryResult
struct Converter {
    tsid_idx: usize,
    timestamp_idx: usize,
    value_idx: usize,
    // (column_name, index)
    tags: Vec<(String, usize)>,
}

impl Converter {
    fn try_new(schema: &RecordSchema) -> Result<Self> {
        let tsid_idx = schema.index_of(TSID_COLUMN).context(InternalNoCause {
            msg: "TSID column is missing in query response",
        })?;
        let timestamp_idx = schema.index_of(TIMESTAMP_COLUMN).context(InternalNoCause {
            msg: "Timestamp column is missing in query response",
        })?;
        let value_idx = schema.index_of(VALUE_COLUMN).context(InternalNoCause {
            msg: "Value column is missing in query response",
        })?;
        let tags = schema
            .columns()
            .iter()
            .enumerate()
            .filter(|(_, col)| col.is_tag)
            .map(|(i, col)| {
                ensure!(
                    matches!(col.data_type, DatumKind::String),
                    InternalNoCause {
                        msg: format!("Tag must be string type, current:{}", col.data_type)
                    }
                );

                Ok((col.name.to_string(), i))
            })
            .collect::<Result<Vec<_>>>()?;

        ensure!(
            matches!(schema.column(tsid_idx).data_type, DatumKind::UInt64),
            InternalNoCause {
                msg: format!(
                    "Tsid must be u64, current:{}",
                    schema.column(tsid_idx).data_type
                )
            }
        );
        ensure!(
            schema.column(timestamp_idx).data_type.is_timestamp(),
            InternalNoCause {
                msg: format!(
                    "Timestamp wrong type, current:{}",
                    schema.column(timestamp_idx).data_type
                )
            }
        );
        ensure!(
            schema.column(value_idx).data_type.is_f64_castable(),
            InternalNoCause {
                msg: format!(
                    "Value must be f64 compatible type, current:{}",
                    schema.column(value_idx).data_type
                )
            }
        );

        Ok(Converter {
            tsid_idx,
            timestamp_idx,
            value_idx,
            tags,
        })
    }

    fn convert(&self, metric: String, record_batches: RecordBatchVec) -> Result<QueryResult> {
        let mut series_by_tsid = HashMap::new();
        for batch in record_batches {
            let tsid_col = batch.column(self.tsid_idx);
            let timestamp_col = batch.column(self.timestamp_idx);
            let value_col = batch.column(self.value_idx);
            let tag_cols = self
                .tags
                .iter()
                .map(|(_, idx)| batch.column(*idx))
                .collect::<Vec<_>>();
            for row_idx in 0..batch.num_rows() {
                let tsid = tsid_col.datum(row_idx).as_u64().context(ErrNoCause {
                    msg: "value should be non-nullable i64",
                    code: StatusCode::BAD_REQUEST,
                })?;
                let sample = Sample {
                    timestamp: timestamp_col
                        .datum(row_idx)
                        .as_timestamp()
                        .context(ErrNoCause {
                            msg: "timestamp should be non-nullable timestamp",
                            code: StatusCode::BAD_REQUEST,
                        })?
                        .as_i64(),
                    value: value_col.datum(row_idx).as_f64().context(ErrNoCause {
                        msg: "value should be non-nullable f64",
                        code: StatusCode::BAD_REQUEST,
                    })?,
                };
                series_by_tsid
                    .entry(tsid)
                    .or_insert_with(|| {
                        let mut labels = self
                            .tags
                            .iter()
                            .enumerate()
                            .map(|(idx, (col_name, _))| {
                                let col_value = tag_cols[idx].datum(row_idx);
                                // for null tag value, use empty string instead
                                let col_value = col_value.as_str().unwrap_or_default();

                                Label {
                                    name: col_name.to_string(),
                                    value: col_value.to_string(),
                                }
                            })
                            .collect::<Vec<_>>();
                        labels.push(Label {
                            name: NAME_LABEL.to_string(),
                            value: metric.clone(),
                        });

                        TimeSeries {
                            labels,
                            ..Default::default()
                        }
                    })
                    .samples
                    .push(sample);
            }
        }

        Ok(QueryResult {
            timeseries: series_by_tsid.into_values().collect(),
        })
    }
}

fn find_metric(matchers: &[LabelMatcher]) -> Result<String> {
    let idx = matchers
        .iter()
        .position(|m| m.name == NAME_LABEL)
        .context(InternalNoCause {
            msg: "Metric name is not found",
        })?;

    Ok(matchers[idx].value.clone())
}

/// Separate metric from labels, and sort labels by name.
fn normalize_labels(mut labels: Vec<Label>) -> Result<(String, Vec<Label>)> {
    let metric_idx = labels
        .iter()
        .position(|label| label.name == NAME_LABEL)
        .unwrap();
    let metric = labels.swap_remove(metric_idx).value;
    labels.sort_unstable_by(|a, b| a.name.cmp(&b.name));

    Ok((metric, labels))
}

fn convert_write_request(req: WriteRequest) -> Result<Vec<WriteTableRequest>> {
    let mut req_by_metric = HashMap::new();
    for timeseries in req.timeseries {
        let (metric, labels) = normalize_labels(timeseries.labels)?;
        let (tag_names, tag_values): (Vec<_>, Vec<_>) = labels
            .into_iter()
            .map(|label| (label.name, label.value))
            .unzip();

        req_by_metric
            .entry(metric.to_string())
            .or_insert_with(|| WriteTableRequest {
                table: metric,
                tag_names,
                field_names: vec![VALUE_COLUMN.to_string()],
                entries: Vec::new(),
            })
            .entries
            .push(WriteSeriesEntry {
                tags: tag_values
                    .into_iter()
                    .enumerate()
                    .map(|(idx, v)| Tag {
                        name_index: idx as u32,
                        value: Some(Value {
                            value: Some(value::Value::StringValue(v)),
                        }),
                    })
                    .collect(),
                field_groups: timeseries
                    .samples
                    .into_iter()
                    .map(|sample| FieldGroup {
                        timestamp: sample.timestamp,
                        fields: vec![Field {
                            name_index: 0,
                            value: Some(Value {
                                value: Some(value::Value::Float64Value(sample.value)),
                            }),
                        }],
                    })
                    .collect(),
            });
    }

    Ok(req_by_metric.into_values().collect())
}

fn convert_query_result(metric: String, resp: Output) -> Result<QueryResult> {
    let record_batches = match resp {
        Output::AffectedRows(_) => {
            return InternalNoCause {
                msg: "Read response must be Rows",
            }
            .fail()
        }
        Output::Records(v) => v,
    };

    let converter = match record_batches.first() {
        None => {
            return Ok(QueryResult::default());
        }
        Some(batch) => Converter::try_new(batch.schema())?,
    };

    converter.convert(metric, record_batches)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, Float64Array, StringArray, TimestampMillisecondArray, UInt64Array},
        record_batch::RecordBatch as ArrowRecordBatch,
    };
    use common_types::{column_schema, record_batch::RecordBatch, schema};
    use prom_remote_api::types::Label;

    use super::*;

    fn make_labels(tuples: Vec<(&str, &str)>) -> Vec<Label> {
        tuples
            .into_iter()
            .map(|(name, value)| Label {
                name: name.to_string(),
                value: value.to_string(),
            })
            .collect()
    }

    fn make_samples(tuples: Vec<(i64, f64)>) -> Vec<Sample> {
        tuples
            .into_iter()
            .map(|(timestamp, value)| Sample { timestamp, value })
            .collect()
    }

    #[test]
    fn test_normailze_labels() {
        let labels = make_labels(vec![
            ("aa", "va"),
            ("zz", "vz"),
            (NAME_LABEL, "cpu"),
            ("yy", "vy"),
        ]);

        let (metric, labels) = normalize_labels(labels).unwrap();
        assert_eq!("cpu", metric);
        assert_eq!(
            make_labels(vec![("aa", "va"), ("yy", "vy"), ("zz", "vz")]),
            labels
        );

        assert!(normalize_labels(vec![]).is_err());
    }

    // Build a schema with
    // - 2 tags(tag1, tag2)
    // - 1 field(value)
    fn build_schema() -> schema::Schema {
        schema::Builder::new()
            .auto_increment_column_id(true)
            .add_key_column(
                column_schema::Builder::new(TSID_COLUMN.to_string(), DatumKind::UInt64)
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .add_key_column(
                column_schema::Builder::new(TIMESTAMP_COLUMN.to_string(), DatumKind::Timestamp)
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .add_normal_column(
                column_schema::Builder::new(VALUE_COLUMN.to_string(), DatumKind::Double)
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .add_normal_column(
                column_schema::Builder::new("tag1".to_string(), DatumKind::String)
                    .is_tag(true)
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .add_normal_column(
                column_schema::Builder::new("tag2".to_string(), DatumKind::String)
                    .is_tag(true)
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .build()
            .unwrap()
    }

    fn build_record_batch(schema: &schema::Schema) -> RecordBatchVec {
        let tsid: ArrayRef = Arc::new(UInt64Array::from(vec![1, 1, 2, 3, 3]));
        let timestamp: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![
            11111111, 11111112, 11111113, 11111111, 11111112,
        ]));
        let values: ArrayRef =
            Arc::new(Float64Array::from(vec![100.0, 101.0, 200.0, 300.0, 301.0]));
        let tag1: ArrayRef = Arc::new(StringArray::from(vec!["a", "a", "b", "c", "c"]));
        let tag2: ArrayRef = Arc::new(StringArray::from(vec!["x", "x", "y", "z", "z"]));

        let batch = ArrowRecordBatch::try_new(
            schema.to_arrow_schema_ref(),
            vec![tsid, timestamp, values, tag1, tag2],
        )
        .unwrap();

        vec![RecordBatch::try_from(batch).unwrap()]
    }

    #[test]
    fn test_convert_records_to_query_result() {
        let metric = "cpu";
        let schema = build_schema();
        let batches = build_record_batch(&schema);
        let record_schema = schema.to_record_schema();
        let converter = Converter::try_new(&record_schema).unwrap();
        let mut query_result = converter.convert(metric.to_string(), batches).unwrap();

        query_result
            .timeseries
            // sort time series by first label's value(tag1 in this case)
            .sort_unstable_by(|a, b| a.labels[0].value.cmp(&b.labels[0].value));

        assert_eq!(
            QueryResult {
                timeseries: vec![
                    TimeSeries {
                        labels: make_labels(vec![
                            ("tag1", "a"),
                            ("tag2", "x"),
                            (NAME_LABEL, metric)
                        ]),
                        samples: make_samples(vec![(11111111, 100.0), (11111112, 101.0),]),
                        ..Default::default()
                    },
                    TimeSeries {
                        labels: make_labels(vec![
                            ("tag1", "b"),
                            ("tag2", "y"),
                            (NAME_LABEL, metric)
                        ]),
                        samples: make_samples(vec![(11111113, 200.0)]),
                        ..Default::default()
                    },
                    TimeSeries {
                        labels: make_labels(vec![
                            ("tag1", "c"),
                            ("tag2", "z"),
                            (NAME_LABEL, metric)
                        ]),
                        samples: make_samples(vec![(11111111, 300.0), (11111112, 301.0),]),
                        ..Default::default()
                    },
                ]
            },
            query_result
        );
    }
}
