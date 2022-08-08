// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use ceresdbproto::{
    common::ResponseHeader,
    prometheus::{Label, PrometheusQueryRequest, PrometheusQueryResponse, Sample, TimeSeries},
};
use common_types::{
    datum::DatumKind,
    record_batch::RecordBatch,
    request_id::RequestId,
    schema::{RecordSchema, TSID_COLUMN},
};
use interpreters::{context::Context as InterpreterContext, factory::Factory, interpreter::Output};
use log::debug;
use query_engine::executor::{Executor as QueryExecutor, RecordBatchVec};
use snafu::{ensure, OptionExt, ResultExt};
use sql::{
    frontend::{Context as SqlContext, Error as FrontendError, Frontend},
    promql::ColumnNames,
    provider::CatalogMetaProvider,
};

use crate::{
    error::{ErrNoCause, ErrWithCause, Result, ServerError, StatusCode},
    grpc::HandlerContext,
};

fn is_table_not_found_error(e: &FrontendError) -> bool {
    matches!(&e, FrontendError::CreatePlan { source }
             if matches!(source, sql::planner::Error::BuildPromPlanError { source }
                         if matches!(source, sql::promql::Error::TableNotFound { .. })))
}

pub async fn handle_query<Q>(
    ctx: &HandlerContext<'_, Q>,
    req: PrometheusQueryRequest,
) -> Result<PrometheusQueryResponse>
where
    Q: QueryExecutor + 'static,
{
    let request_id = RequestId::next_id();

    debug!(
        "Grpc handle query begin, catalog:{}, tenant:{}, request_id:{}, request:{:?}",
        ctx.catalog(),
        ctx.tenant(),
        request_id,
        req,
    );

    let instance = &ctx.instance;
    // We use tenant as schema
    // TODO(yingwen): Privilege check, cannot access data of other tenant
    // TODO(yingwen): Maybe move MetaProvider to instance
    let provider = CatalogMetaProvider {
        manager: instance.catalog_manager.clone(),
        default_catalog: ctx.catalog(),
        default_schema: ctx.tenant(),
        function_registry: &*instance.function_registry,
    };
    let frontend = Frontend::new(provider);

    let mut sql_ctx = SqlContext::new(request_id);
    let expr = frontend
        .parse_promql(&mut sql_ctx, req)
        .map_err(|e| Box::new(e) as _)
        .context(ErrWithCause {
            code: StatusCode::InvalidArgument,
            msg: "Invalid request",
        })?;

    let (plan, column_name) = frontend
        .promql_expr_to_plan(&mut sql_ctx, expr)
        .map_err(|e| {
            let code = if is_table_not_found_error(&e) {
                StatusCode::NotFound
            } else {
                StatusCode::InternalError
            };
            ServerError::ErrWithCause {
                code,
                msg: "Failed to create plan".to_string(),
                source: Box::new(e),
            }
        })?;

    if ctx.instance.limiter.should_limit(&plan) {
        ErrNoCause {
            code: StatusCode::TooManyRequests,
            msg: "Query limited by reject list",
        }
        .fail()?;
    }

    // Execute in interpreter
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

    let output = interpreter
        .execute()
        .await
        .map_err(|e| Box::new(e) as _)
        .with_context(|| ErrWithCause {
            code: StatusCode::InternalError,
            msg: "Failed to execute interpreter",
        })?;

    let resp = convert_output(output, column_name)
        .map_err(|e| Box::new(e) as _)
        .with_context(|| ErrWithCause {
            code: StatusCode::InternalError,
            msg: "Failed to convert output",
        })?;

    Ok(resp)
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

    let mut resp = empty_ok_resp();
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
            let mut timeseries = TimeSeries::new();
            timeseries.set_labels(
                tags.iter()
                    .map(|(k, v)| {
                        let mut label = Label::new();
                        label.set_name(k.clone());
                        label.set_value(v.clone());
                        label
                    })
                    .collect::<Vec<_>>()
                    .into(),
            );
            timeseries.set_samples(samples.into());
            timeseries
        })
        .collect::<Vec<_>>();

    resp.set_timeseries(series_set.into());
    Ok(resp)
}

fn empty_ok_resp() -> PrometheusQueryResponse {
    let mut header = ResponseHeader::new();
    header.code = StatusCode::Ok.as_u32();

    let mut resp = PrometheusQueryResponse::new();
    resp.set_header(header);

    resp
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
                code: StatusCode::InvalidArgument,
                msg: "Failed to find Tsid column".to_string(),
            })?;
        let timestamp_idx = record_schema
            .index_of(&column_name.timestamp)
            .with_context(|| ErrNoCause {
                code: StatusCode::InvalidArgument,
                msg: "Failed to find Timestamp column".to_string(),
            })?;
        ensure!(
            record_schema.column(timestamp_idx).data_type == DatumKind::Timestamp,
            ErrNoCause {
                code: StatusCode::InvalidArgument,
                msg: "Timestamp column should be timestamp type"
            }
        );
        let field_idx = record_schema
            .index_of(&column_name.field)
            .with_context(|| ErrNoCause {
                code: StatusCode::InvalidArgument,
                msg: format!("Failed to find {} column", column_name.field),
            })?;
        let field_type = record_schema.column(field_idx).data_type;
        ensure!(
            field_type.is_f64_castable(),
            ErrNoCause {
                code: StatusCode::InvalidArgument,
                msg: format!(
                    "Field type must be f64-compatibile type, current:{}",
                    field_type
                )
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
            let mut sample = Sample::new();
            sample.set_value(field);
            sample.set_timestamp(timestamp);
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
        datum::Datum,
        row::Row,
        schema,
        string::StringBytes,
        time::Timestamp,
    };

    use super::*;

    fn build_schema() -> schema::Schema {
        schema::Builder::new()
            .auto_increment_column_id(true)
            .enable_tsid_primary_key(true)
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
        let mut sample = Sample::new();
        sample.set_value(value);
        sample.set_timestamp(timestamp);
        sample
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
