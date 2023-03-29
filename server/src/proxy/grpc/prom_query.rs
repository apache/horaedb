// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Instant,
};

use ceresdbproto::{
    common::ResponseHeader,
    prometheus::{Label, Sample, TimeSeries},
    storage::{PrometheusQueryRequest, PrometheusQueryResponse},
};
use common_types::{
    datum::DatumKind,
    record_batch::RecordBatch,
    request_id::RequestId,
    schema::{RecordSchema, TSID_COLUMN},
};
use common_util::error::BoxError;
use http::StatusCode;
use interpreters::{context::Context as InterpreterContext, factory::Factory, interpreter::Output};
use log::{error, info};
use query_engine::executor::{Executor as QueryExecutor, RecordBatchVec};
use snafu::{ensure, OptionExt, ResultExt};
use sql::{
    frontend::{Context as SqlContext, Error as FrontendError, Frontend},
    promql::ColumnNames,
    provider::CatalogMetaProvider,
};

use crate::proxy::{
    error,
    error::{ErrNoCause, ErrWithCause, Error, Result},
    Context, Proxy,
};

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    pub async fn handle_prom_query(
        &self,
        ctx: Context,
        req: PrometheusQueryRequest,
    ) -> PrometheusQueryResponse {
        self.hotspot_recorder.inc_promql_reqs(&req);
        match self.handle_prom_query_internal(ctx, req).await {
            Err(e) => {
                error!("Failed to handle prom query, err:{e}");
                PrometheusQueryResponse {
                    header: Some(error::build_err_header(e)),
                    ..Default::default()
                }
            }
            Ok(v) => v,
        }
    }

    async fn handle_prom_query_internal(
        &self,
        ctx: Context,
        req: PrometheusQueryRequest,
    ) -> Result<PrometheusQueryResponse> {
        let request_id = RequestId::next_id();
        let begin_instant = Instant::now();
        let deadline = ctx.timeout.map(|t| begin_instant + t);
        let req_ctx = req.context.unwrap();
        let schema = req_ctx.database;
        let catalog = self.instance.catalog_manager.default_catalog_name();

        info!(
            "Grpc handle prom query begin, catalog:{catalog}, schema:{schema}, request_id:{request_id}",
        );

        let provider = CatalogMetaProvider {
            manager: self.instance.catalog_manager.clone(),
            default_catalog: catalog,
            default_schema: &schema,
            function_registry: &*self.instance.function_registry,
        };
        let frontend = Frontend::new(provider);

        let mut sql_ctx = SqlContext::new(request_id, deadline);
        let expr = frontend
            .parse_promql(&mut sql_ctx, req.expr)
            .box_err()
            .context(ErrWithCause {
                code: StatusCode::BAD_REQUEST,
                msg: "Invalid request",
            })?;

        let (plan, column_name) =
            frontend
                .promql_expr_to_plan(&mut sql_ctx, expr)
                .map_err(|e| {
                    let code = if is_table_not_found_error(&e) {
                        StatusCode::NOT_FOUND
                    } else {
                        StatusCode::INTERNAL_SERVER_ERROR
                    };
                    Error::ErrWithCause {
                        code,
                        msg: "Failed to create plan".to_string(),
                        source: Box::new(e),
                    }
                })?;

        self.instance
            .limiter
            .try_limit(&plan)
            .box_err()
            .context(ErrWithCause {
                code: StatusCode::FORBIDDEN,
                msg: "Query is blocked",
            })?;

        // Execute in interpreter
        let interpreter_ctx = InterpreterContext::builder(request_id, deadline)
            // Use current ctx's catalog and schema as default catalog and schema
            .default_catalog_and_schema(catalog.to_string(), schema)
            .build();
        let interpreter_factory = Factory::new(
            self.instance.query_executor.clone(),
            self.instance.catalog_manager.clone(),
            self.instance.table_engine.clone(),
            self.instance.table_manipulator.clone(),
        );
        let interpreter = interpreter_factory
            .create(interpreter_ctx, plan)
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "Failed to create interpreter",
            })?;

        let output = if let Some(deadline) = deadline {
            tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                interpreter.execute(),
            )
            .await
            .box_err()
            .context(ErrWithCause {
                code: StatusCode::REQUEST_TIMEOUT,
                msg: "Query timeout",
            })?
        } else {
            interpreter.execute().await
        }
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: "Failed to execute interpreter",
        })?;

        let resp = convert_output(output, column_name)
            .box_err()
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "Failed to convert output",
            })?;

        Ok(resp)
    }
}

fn is_table_not_found_error(e: &FrontendError) -> bool {
    matches!(&e, FrontendError::CreatePlan { source }
             if matches!(source, sql::planner::Error::BuildPromPlanError { source }
                         if matches!(source, sql::promql::Error::TableNotFound { .. })))
}

fn convert_output(
    output: Output,
    column_name: Arc<ColumnNames>,
) -> Result<PrometheusQueryResponse> {
    match output {
        Output::Records(records) => convert_records(records, column_name),
        _ => unreachable!(),
    }
}

fn convert_records(
    records: RecordBatchVec,
    column_name: Arc<ColumnNames>,
) -> Result<PrometheusQueryResponse> {
    if records.is_empty() {
        return Ok(empty_ok_resp());
    }

    let mut tsid_to_tags = HashMap::new();
    let mut tsid_to_samples = HashMap::new();

    // TODO(chenxiang): benchmark iterator by columns
    for record_batch in records {
        let converter = RecordConverter::try_new(&column_name, record_batch.schema())?;

        for (tsid, samples) in converter.convert_to_samples(record_batch, &mut tsid_to_tags) {
            tsid_to_samples
                .entry(tsid)
                .or_insert_with(Vec::new)
                .extend(samples)
        }
    }

    let series_set = tsid_to_samples
        .into_iter()
        .map(|(tsid, samples)| {
            let tags = tsid_to_tags
                .get(&tsid)
                .expect("ensured in convert_to_samples");
            let labels = tags
                .iter()
                .map(|(k, v)| Label {
                    name: k.clone(),
                    value: v.clone(),
                })
                .collect::<Vec<_>>();

            TimeSeries { labels, samples }
        })
        .collect::<Vec<_>>();

    let mut resp = empty_ok_resp();
    resp.timeseries = series_set;
    Ok(resp)
}

fn empty_ok_resp() -> PrometheusQueryResponse {
    let header = ResponseHeader {
        code: StatusCode::OK.as_u16() as u32,
        ..Default::default()
    };

    PrometheusQueryResponse {
        header: Some(header),
        ..Default::default()
    }
}

/// RecordConverter convert RecordBatch to time series format required by PromQL
struct RecordConverter {
    tsid_idx: usize,
    timestamp_idx: usize,
    tags_idx: BTreeMap<String, usize>, // tag_key -> column_index
    field_idx: usize,
}

impl RecordConverter {
    fn try_new(column_name: &ColumnNames, record_schema: &RecordSchema) -> Result<Self> {
        let tsid_idx = record_schema
            .index_of(TSID_COLUMN)
            .with_context(|| ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: "Failed to find Tsid column",
            })?;
        let timestamp_idx = record_schema
            .index_of(&column_name.timestamp)
            .context(ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: "Failed to find Timestamp column",
            })?;
        ensure!(
            record_schema.column(timestamp_idx).data_type == DatumKind::Timestamp,
            ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: "Timestamp column should be timestamp type"
            }
        );
        let field_idx = record_schema
            .index_of(&column_name.field)
            .with_context(|| ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!("Failed to find {} column", column_name.field),
            })?;
        let field_type = record_schema.column(field_idx).data_type;
        ensure!(
            field_type.is_f64_castable(),
            ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!("Field type must be f64-compatibile type, current:{field_type}")
            }
        );

        let tags_idx: BTreeMap<_, _> = column_name
            .tag_keys
            .iter()
            .filter_map(|tag_key| {
                record_schema
                    .index_of(tag_key)
                    .map(|idx| (tag_key.to_string(), idx))
            })
            .collect();

        Ok(Self {
            tsid_idx,
            timestamp_idx,
            tags_idx,
            field_idx,
        })
    }

    fn convert_to_samples(
        &self,
        record_batch: RecordBatch,
        tsid_to_tags: &mut HashMap<u64, BTreeMap<String, String>>,
    ) -> HashMap<u64, Vec<Sample>> {
        let mut tsid_to_samples = HashMap::new();

        let tsid_cols = record_batch.column(self.tsid_idx);
        let timestamp_cols = record_batch.column(self.timestamp_idx);
        let field_cols = record_batch.column(self.field_idx);
        for row_idx in 0..record_batch.num_rows() {
            let timestamp = timestamp_cols
                .datum(row_idx)
                .as_timestamp()
                .expect("checked in try_new")
                .as_i64();
            let field = field_cols
                .datum(row_idx)
                .as_f64()
                .expect("checked in try_new");
            let tsid = tsid_cols
                .datum(row_idx)
                .as_u64()
                .expect("checked in try_new");

            tsid_to_tags.entry(tsid).or_insert_with(|| {
                self.tags_idx
                    .iter()
                    .filter_map(|(tag_key, col_idx)| {
                        // TODO(chenxiang): avoid clone?
                        record_batch
                            .column(*col_idx)
                            .datum(row_idx)
                            .as_str()
                            .and_then(|tag_value| {
                                // filter empty tag value out, since Prometheus don't allow it.
                                if tag_value.is_empty() {
                                    None
                                } else {
                                    Some((tag_key.clone(), tag_value.to_string()))
                                }
                            })
                    })
                    .collect::<BTreeMap<_, _>>()
            });

            let samples = tsid_to_samples.entry(tsid).or_insert_with(Vec::new);
            let sample = Sample {
                value: field,
                timestamp,
            };
            samples.push(sample);
        }

        tsid_to_samples
    }
}

#[cfg(test)]
mod tests {

    use common_types::{
        column::{ColumnBlock, ColumnBlockBuilder},
        column_schema,
        datum::{Datum, DatumKind},
        row::Row,
        schema,
        string::StringBytes,
        time::Timestamp,
    };

    use super::*;

    fn build_schema() -> schema::Schema {
        schema::Builder::new()
            .auto_increment_column_id(true)
            .add_key_column(
                column_schema::Builder::new("timestamp".to_string(), DatumKind::Timestamp)
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .add_key_column(
                column_schema::Builder::new(TSID_COLUMN.to_string(), DatumKind::UInt64)
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .add_normal_column(
                column_schema::Builder::new("field1".to_string(), DatumKind::Double)
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
            .build()
            .unwrap()
    }

    fn build_column_block() -> Vec<ColumnBlock> {
        let build_row = |ts: i64, tsid: u64, field1: f64, field2: &str| -> Row {
            let datums = vec![
                Datum::Timestamp(Timestamp::new(ts)),
                Datum::UInt64(tsid),
                Datum::Double(field1),
                Datum::String(StringBytes::from(field2)),
            ];

            Row::from_datums(datums)
        };

        let rows = vec![
            build_row(1000001, 1, 10.0, "v5"),
            build_row(1000002, 1, 11.0, "v5"),
            build_row(1000000, 2, 10.0, "v4"),
            build_row(1000000, 3, 10.0, "v3"),
        ];

        let mut builder = ColumnBlockBuilder::with_capacity(&DatumKind::Timestamp, 2);
        for row in &rows {
            builder.append(row[0].clone()).unwrap();
        }
        let timestamp_block = builder.build();

        let mut builder = ColumnBlockBuilder::with_capacity(&DatumKind::UInt64, 2);
        for row in &rows {
            builder.append(row[1].clone()).unwrap();
        }
        let tsid_block = builder.build();

        let mut builder = ColumnBlockBuilder::with_capacity(&DatumKind::Double, 2);
        for row in &rows {
            builder.append(row[2].clone()).unwrap();
        }
        let field_block = builder.build();

        let mut builder = ColumnBlockBuilder::with_capacity(&DatumKind::String, 2);
        for row in &rows {
            builder.append(row[3].clone()).unwrap();
        }
        let tag_block = builder.build();

        vec![timestamp_block, tsid_block, field_block, tag_block]
    }

    fn make_sample(timestamp: i64, value: f64) -> Sample {
        Sample { value, timestamp }
    }

    fn make_tags(tags: Vec<(String, String)>) -> BTreeMap<String, String> {
        tags.into_iter().collect::<BTreeMap<_, _>>()
    }

    #[test]
    fn test_record_convert() {
        let schema = build_schema();
        let record_schema = schema.to_record_schema();
        let column_blocks = build_column_block();
        let record_batch = RecordBatch::new(record_schema, column_blocks).unwrap();

        let column_name = ColumnNames {
            timestamp: "timestamp".to_string(),
            tag_keys: vec!["tag1".to_string()],
            field: "field1".to_string(),
        };
        let converter = RecordConverter::try_new(&column_name, &schema.to_record_schema()).unwrap();
        let mut tsid_to_tags = HashMap::new();
        let tsid_to_samples = converter.convert_to_samples(record_batch, &mut tsid_to_tags);

        assert_eq!(
            tsid_to_samples.get(&1).unwrap().clone(),
            vec![make_sample(1000001, 10.0), make_sample(1000002, 11.0)]
        );
        assert_eq!(
            tsid_to_samples.get(&2).unwrap().clone(),
            vec![make_sample(1000000, 10.0)]
        );
        assert_eq!(
            tsid_to_samples.get(&3).unwrap().clone(),
            vec![make_sample(1000000, 10.0)]
        );
        assert_eq!(
            tsid_to_tags.get(&1).unwrap().clone(),
            make_tags(vec![("tag1".to_string(), "v5".to_string())])
        );
        assert_eq!(
            tsid_to_tags.get(&2).unwrap().clone(),
            make_tags(vec![("tag1".to_string(), "v4".to_string())])
        );
        assert_eq!(
            tsid_to_tags.get(&3).unwrap().clone(),
            make_tags(vec![("tag1".to_string(), "v3".to_string())])
        );
    }
}
