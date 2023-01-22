// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! This module implements prometheus remote storage API.
//! It converts write request to gRPC write request, and
//! translates query request to SQL for execution.

use std::{collections::HashMap, time::Instant};

use async_trait::async_trait;
use ceresdbproto::storage::{
    value, Field, FieldGroup, Tag, Value, WriteEntry, WriteMetric, WriteRequest as WriteRequestPb,
};
use common_types::{
    datum::DatumKind,
    request_id::RequestId,
    schema::{RecordSchema, TIMESTAMP_COLUMN, TSID_COLUMN},
};
use interpreters::interpreter::Output;
use log::debug;
use prom_remote_api::types::{
    label_matcher, Label, Query, QueryResult, RemoteStorage, Sample, TimeSeries, WriteRequest,
};
use query_engine::executor::{Executor as QueryExecutor, RecordBatchVec};
use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};
use warp::reject;

use crate::{
    context::RequestContext, handlers, instance::InstanceRef,
    schema_config_provider::SchemaConfigProviderRef,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Metric name is not found.\nBacktrace:\n{}", backtrace))]
    MissingName { backtrace: Backtrace },

    #[snafu(display("Invalid matcher type, value:{}.\nBacktrace:\n{}", value, backtrace))]
    InvalidMatcherType { value: i32, backtrace: Backtrace },

    #[snafu(display("Read response must be Rows.\nBacktrace:\n{}", backtrace))]
    ResponseMustRows { backtrace: Backtrace },

    #[snafu(display("TSID column is missing in query response.\nBacktrace:\n{}", backtrace))]
    MissingTSID { backtrace: Backtrace },

    #[snafu(display(
        "Timestamp column is missing in query response.\nBacktrace:\n{}",
        backtrace
    ))]
    MissingTimestamp { backtrace: Backtrace },

    #[snafu(display(
        "Value column is missing in query response.\nBacktrace:\n{}",
        backtrace
    ))]
    MissingValue { backtrace: Backtrace },

    #[snafu(display("Handle sql failed, err:{}.", source))]
    SqlHandle {
        source: Box<crate::handlers::error::Error>,
    },

    #[snafu(display("Tsid must be u64, current:{}.\nBacktrace:\n{}", kind, backtrace))]
    TsidMustU64 {
        kind: DatumKind,
        backtrace: Backtrace,
    },

    #[snafu(display("Timestamp wrong type, current:{}.\nBacktrace:\n{}", kind, backtrace))]
    MustTimestamp {
        kind: DatumKind,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Value must be f64 compatible type, current:{}.\nBacktrace:\n{}",
        kind,
        backtrace
    ))]
    F64Castable {
        kind: DatumKind,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Tag must be string type, current:{}.\nBacktrace:\n{}",
        kind,
        backtrace
    ))]
    TagMustString {
        kind: DatumKind,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to write via gRPC, source:{}.", source))]
    GRPCWriteError {
        source: crate::grpc::storage_service::error::Error,
    },

    #[snafu(display("Failed to get schema, source:{}.", source))]
    SchemaError {
        source: crate::schema_config_provider::Error,
    },
}

define_result!(Error);

impl reject::Reject for Error {}

const NAME_LABEL: &str = "__name__";
const VALUE_COLUMN: &str = "value";

pub struct CeresDBStorage<Q: QueryExecutor + 'static> {
    instance: InstanceRef<Q>,
    schema_config_provider: SchemaConfigProviderRef,
}

impl<Q: QueryExecutor + 'static> CeresDBStorage<Q> {
    pub fn new(instance: InstanceRef<Q>, schema_config_provider: SchemaConfigProviderRef) -> Self {
        Self {
            instance,
            schema_config_provider,
        }
    }
}

impl<Q: QueryExecutor + 'static> CeresDBStorage<Q> {
    /// This function will separate measurement from labels, and sort labels by
    /// name.
    fn normalize_labels(labels: Vec<Label>) -> Result<(String, Vec<Label>)> {
        let mut new_labels = Vec::with_capacity(labels.len());
        let mut measurement = None;
        for label in labels {
            if label.name == NAME_LABEL {
                measurement = Some(label.value);
                continue;
            }

            new_labels.push(label);
        }
        let measurement = measurement.context(MissingName)?;
        new_labels.sort_unstable_by(|a, b| a.name.cmp(&b.name));

        Ok((measurement, new_labels))
    }

    fn convert_write_request(req: WriteRequest) -> Result<WriteRequestPb> {
        let mut req_by_metric = HashMap::new();
        for timeseries in req.timeseries {
            let (measurement, labels) = Self::normalize_labels(timeseries.labels)?;
            let (tag_names, tag_values): (Vec<_>, Vec<_>) = labels
                .into_iter()
                .map(|label| (label.name, label.value))
                .unzip();

            req_by_metric
                .entry(measurement.to_string())
                .or_insert_with(|| WriteMetric {
                    metric: measurement,
                    tag_names,
                    field_names: vec![VALUE_COLUMN.to_string()],
                    entries: Vec::new(),
                })
                .entries
                .push(WriteEntry {
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

        Ok(WriteRequestPb {
            metrics: req_by_metric.into_values().collect(),
        })
    }

    fn convert_query_result(measurement: String, resp: Output) -> Result<QueryResult> {
        let record_batches = match resp {
            Output::AffectedRows(_) => return ResponseMustRows {}.fail(),
            Output::Records(v) => v,
        };

        let converter = match record_batches.first() {
            None => {
                return Ok(QueryResult::default());
            }
            Some(batch) => Converter::try_new(batch.schema())?,
        };

        converter.convert(measurement, record_batches)
    }
}

#[async_trait]
impl<Q: QueryExecutor + 'static> RemoteStorage for CeresDBStorage<Q> {
    type Context = RequestContext;
    type Err = Error;

    /// Write samples to remote storage
    async fn write(&self, ctx: Self::Context, req: WriteRequest) -> Result<()> {
        let request_id = RequestId::next_id();
        let deadline = ctx.timeout.map(|t| Instant::now() + t);
        let catalog = &ctx.catalog;
        self.instance.catalog_manager.default_catalog_name();
        let schema = &ctx.schema;
        let schema_config = self
            .schema_config_provider
            .schema_config(schema)
            .context(SchemaError)?;
        let plans = crate::grpc::storage_service::write::write_request_to_insert_plan(
            request_id,
            catalog,
            schema,
            self.instance.clone(),
            Self::convert_write_request(req)?,
            schema_config,
            deadline,
        )
        .await
        .context(GRPCWriteError)?;

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
            .context(GRPCWriteError)?;
        }
        debug!(
            "Remote write finished, catalog:{}, schema:{}, success:{}",
            catalog, schema, success
        );

        Ok(())
    }

    /// Process one query within ReadRequest.
    async fn process_query(&self, ctx: &Self::Context, q: Query) -> Result<QueryResult> {
        let mut filters = Vec::with_capacity(q.matchers.len());
        filters.push(format!(
            "{} between {} AND {}",
            TIMESTAMP_COLUMN, q.start_timestamp_ms, q.end_timestamp_ms
        ));
        let mut measurement = None;
        for m in &q.matchers {
            if m.name == NAME_LABEL {
                measurement = Some(m.value.to_string());
                continue;
            }

            let filter = match m.r#type() {
                label_matcher::Type::Eq => format!("{} = '{}'", m.name, m.value),
                label_matcher::Type::Neq => format!("{} != '{}'", m.name, m.value),
                // https://github.com/prometheus/prometheus/blob/2ce94ac19673a3f7faf164e9e078a79d4d52b767/model/labels/regexp.go#L29
                label_matcher::Type::Re => format!("{} ~ '^(?:{})'", m.name, m.value),
                label_matcher::Type::Nre => format!("{} !~ '^(?:{})'", m.name, m.value),
            };
            filters.push(filter)
        }

        let measurement = measurement.context(MissingName).unwrap();
        let sql = format!(
            "select * from {} where {} order by {}, {}",
            measurement,
            filters.join(" and "),
            TSID_COLUMN,
            TIMESTAMP_COLUMN
        );

        let result = handlers::sql::handle_sql(ctx, self.instance.clone(), sql.into())
            .await
            .map_err(Box::new)
            .context(SqlHandle)?;

        Self::convert_query_result(measurement, result)
    }
}

/// Converter convert Arrow's RecordBatch into Prometheus's QueryResult
struct Converter {
    tsid_idx: usize,
    timestamp_idx: usize,
    value_idx: usize,
    // (column_name, index)
    tags: Vec<(String, usize)>,
}

impl Converter {
    fn try_new(schema: &RecordSchema) -> Result<Self> {
        let tsid_idx = schema.index_of(TSID_COLUMN).context(MissingTSID)?;
        let timestamp_idx = schema
            .index_of(TIMESTAMP_COLUMN)
            .context(MissingTimestamp)?;
        let value_idx = schema.index_of(VALUE_COLUMN).context(MissingValue)?;
        let tags = schema
            .columns()
            .iter()
            .enumerate()
            .filter(|(_, col)| col.is_tag)
            .map(|(i, col)| {
                ensure!(
                    matches!(col.data_type, DatumKind::String),
                    TagMustString {
                        kind: col.data_type
                    }
                );

                Ok((col.name.to_string(), i))
            })
            .collect::<Result<Vec<_>>>()?;

        ensure!(
            matches!(schema.column(tsid_idx).data_type, DatumKind::UInt64),
            TsidMustU64 {
                kind: schema.column(tsid_idx).data_type
            }
        );
        ensure!(
            schema.column(timestamp_idx).data_type.is_timestamp(),
            MustTimestamp {
                kind: schema.column(timestamp_idx).data_type
            }
        );
        ensure!(
            schema.column(value_idx).data_type.is_f64_castable(),
            F64Castable {
                kind: schema.column(value_idx).data_type
            }
        );

        Ok(Converter {
            tsid_idx,
            timestamp_idx,
            value_idx,
            tags,
        })
    }

    fn convert(&self, measurement: String, record_batches: RecordBatchVec) -> Result<QueryResult> {
        let mut series_by_tsid = HashMap::new();
        debug!("convert query result, tags:{:?}.", self.tags);
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
                let tsid = tsid_col
                    .datum(row_idx)
                    .as_u64()
                    .expect("checked in try_new");
                series_by_tsid
                    .entry(tsid)
                    .or_insert_with(|| {
                        let mut labels = self
                            .tags
                            .iter()
                            .enumerate()
                            .map(|(idx, (col_name, _))| {
                                let col_value = tag_cols[idx].datum(row_idx);
                                let col_value = col_value.as_str().expect("checked in try_new");
                                Label {
                                    name: col_name.to_string(),
                                    value: col_value.to_string(),
                                }
                            })
                            .collect::<Vec<_>>();
                        labels.push(Label {
                            name: NAME_LABEL.to_string(),
                            value: measurement.clone(),
                        });

                        TimeSeries {
                            labels,
                            ..Default::default()
                        }
                    })
                    .samples
                    .push(Sample {
                        timestamp: timestamp_col
                            .datum(row_idx)
                            .as_timestamp()
                            .expect("checked in try_new")
                            .as_i64(),
                        value: value_col
                            .datum(row_idx)
                            .as_f64()
                            .expect("checked in try_new"),
                    });
            }
        }

        Ok(QueryResult {
            timeseries: series_by_tsid.into_values().collect(),
        })
    }
}
