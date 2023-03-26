// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! This module implements [write][1] and [query][2] for InfluxDB.
//! [1]: https://docs.influxdata.com/influxdb/v1.8/tools/api/#write-http-endpoint
//! [2]: https://docs.influxdata.com/influxdb/v1.8/tools/api/#query-http-endpoint

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Instant,
};

use bytes::Bytes;
use ceresdbproto::storage::{
    value, Field, FieldGroup, Tag, Value, WriteSeriesEntry, WriteTableRequest,
};
use common_types::{
    column_schema::ColumnSchema, datum::Datum, record_batch::RecordBatch, request_id::RequestId,
    schema::RecordSchema, time::Timestamp,
};
use common_util::error::BoxError;
use handlers::{
    error::{InfluxDbHandlerNoCause, InfluxDbHandlerWithCause, Result},
    query::QueryRequest,
};
use influxdb_line_protocol::FieldValue;
use interpreters::interpreter::Output;
use log::debug;
use query_engine::executor::Executor as QueryExecutor;
use serde::Serialize;
use snafu::{ensure, ResultExt};
use sql::influxql::planner::CERESDB_MEASUREMENT_COLUMN_NAME;
use warp::{reject, reply, Rejection, Reply};

use crate::{
    context::RequestContext,
    handlers,
    instance::InstanceRef,
    proxy::grpc::write::{execute_insert_plan, write_request_to_insert_plan, WriteContext},
    schema_config_provider::SchemaConfigProviderRef,
};

pub struct InfluxDb<Q> {
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

/// Influxql response organized in the same way with influxdb.
///
/// The basic example:
/// ```json
/// {"results":[{"statement_id":0,"series":[{"name":"mymeas",
///                                          "columns":["time","myfield","mytag1","mytag2"],
///                                          "values":[["2017-03-01T00:16:18Z",33.1,null,null],
///                                                    ["2017-03-01T00:17:18Z",12.4,"12","14"]]}]}]}
/// ```
/// More details refer to:
///   https://docs.influxdata.com/influxdb/v1.8/tools/api/#query-data-with-a-select-statement
#[derive(Debug, Serialize)]
pub struct InfluxqlResponse {
    pub results: Vec<OneInfluxqlResult>,
}

#[derive(Debug, Serialize)]
pub struct OneInfluxqlResult {
    statement_id: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    series: Option<Vec<OneSeries>>,
}

#[derive(Debug, Serialize)]
struct OneSeries {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    tags: Option<BTreeMap<String, String>>,
    columns: Vec<String>,
    values: Vec<Vec<Datum>>,
}

/// [InfluxqlResult] builder
#[derive(Default)]
pub struct InfluxqlResultBuilder {
    /// Query id for [multiple queries](https://docs.influxdata.com/influxdb/v1.8/tools/api/#request-multiple-queries)
    statement_id: u32,

    /// Schema of influxql query result
    ///
    /// Its format is like:
    /// measurement |
    /// tag_1..tag_n(columns in `group by`) |
    /// time |
    /// column_1..column_n(column in `projection` but not in `group by`)
    column_schemas: Vec<ColumnSchema>,

    /// Tags part in schema
    group_by_tag_col_idxs: Vec<usize>,

    /// Columns part in schema(include `time`)
    value_col_idxs: Vec<usize>,

    /// Mapping group key(`measurement` + `tag values`) to column values,
    ///
    /// NOTE: because tag keys in `group by` clause are same in each sub result,
    /// we just use the `measurement` + `tag values` to distinguish them.
    group_key_to_idx: HashMap<GroupKey, usize>,

    /// Column values grouped by [GroupKey]
    value_groups: Vec<RowGroup>,
}

type Row = Vec<Datum>;
type RowGroup = Vec<Row>;

impl InfluxqlResultBuilder {
    pub fn new(record_schema: &RecordSchema, statement_id: u32) -> Result<Self> {
        let column_schemas = record_schema.columns().to_owned();
        ensure!(
            !column_schemas.is_empty(),
            InfluxDbHandlerNoCause {
                msg: "empty schema",
            }
        );

        // Find the tags part and columns part from schema.
        let mut group_by_col_idxs = Vec::new();
        let mut value_col_idxs = Vec::new();

        // The following index searching logic is derived from the fixed format
        // described when introducing `column_schemas`.
        let mut col_iter = column_schemas.iter().enumerate();
        // The first column may be measurement column in normal.
        ensure!(col_iter.next().unwrap().1.name == CERESDB_MEASUREMENT_COLUMN_NAME, InfluxDbHandlerNoCause {
            msg: format!("invalid schema whose first column is not measurement column, schema:{column_schemas:?}"),
        });

        // The group by tags will be placed after measurement and before time column.
        let mut searching_group_by_tags = true;
        for (idx, col) in col_iter {
            if col.data_type.is_timestamp() {
                searching_group_by_tags = false;
            }

            if searching_group_by_tags {
                group_by_col_idxs.push(idx);
            } else {
                value_col_idxs.push(idx);
            }
        }

        Ok(Self {
            statement_id,
            column_schemas,
            group_by_tag_col_idxs: group_by_col_idxs,
            value_col_idxs,
            group_key_to_idx: HashMap::new(),
            value_groups: Vec::new(),
        })
    }

    pub fn add_record_batch(&mut self, record_batch: RecordBatch) -> Result<()> {
        // Check schema's compatibility.
        ensure!(
            record_batch.schema().columns() == self.column_schemas,
            InfluxDbHandlerNoCause {
                msg: format!(
                    "conflict schema, origin:{:?}, new:{:?}",
                    self.column_schemas,
                    record_batch.schema().columns()
                ),
            }
        );

        let row_num = record_batch.num_rows();
        for row_idx in 0..row_num {
            // Get measurement + group by tags.
            let group_key = self.extract_group_key(&record_batch, row_idx)?;
            let value_group = self.extract_value_group(&record_batch, row_idx)?;

            let value_groups = if let Some(idx) = self.group_key_to_idx.get(&group_key) {
                self.value_groups.get_mut(*idx).unwrap()
            } else {
                self.value_groups.push(Vec::new());
                self.group_key_to_idx
                    .insert(group_key, self.value_groups.len() - 1);
                self.value_groups.last_mut().unwrap()
            };

            value_groups.push(value_group);
        }

        Ok(())
    }

    pub fn build(self) -> OneInfluxqlResult {
        let ordered_group_keys = {
            let mut ordered_pairs = self.group_key_to_idx.into_iter().collect::<Vec<_>>();
            ordered_pairs.sort_by(|a, b| a.1.cmp(&b.1));
            ordered_pairs
                .into_iter()
                .map(|(key, _)| key)
                .collect::<Vec<_>>()
        };

        let series = ordered_group_keys
            .into_iter()
            .zip(self.value_groups.into_iter())
            .map(|(group_key, value_group)| {
                let name = group_key.measurement;
                let tags = if group_key.group_by_tag_values.is_empty() {
                    None
                } else {
                    let tags = group_key
                        .group_by_tag_values
                        .into_iter()
                        .enumerate()
                        .map(|(tagk_idx, tagv)| {
                            let tagk_col_idx = self.group_by_tag_col_idxs[tagk_idx];
                            let tagk = self.column_schemas[tagk_col_idx].name.clone();

                            (tagk, tagv)
                        })
                        .collect::<BTreeMap<_, _>>();

                    Some(tags)
                };

                let columns = self
                    .value_col_idxs
                    .iter()
                    .map(|idx| self.column_schemas[*idx].name.clone())
                    .collect::<Vec<_>>();

                OneSeries {
                    name,
                    tags,
                    columns,
                    values: value_group,
                }
            })
            .collect();

        OneInfluxqlResult {
            series: Some(series),
            statement_id: self.statement_id,
        }
    }

    fn extract_group_key(&self, record_batch: &RecordBatch, row_idx: usize) -> Result<GroupKey> {
        let mut group_by_tag_values = Vec::with_capacity(self.group_by_tag_col_idxs.len());
        let measurement = {
            let measurement = record_batch.column(0).datum(row_idx);
            match measurement {
                Datum::String(m) => m.to_string(),
                other => {
                    return InfluxDbHandlerNoCause {
                        msg: format!("invalid measurement column, column:{other:?}"),
                    }
                    .fail()
                }
            }
        };

        for col_idx in &self.group_by_tag_col_idxs {
            let tag = {
                let tag_datum = record_batch.column(*col_idx).datum(row_idx);
                match tag_datum {
                    Datum::Null => "".to_string(),
                    Datum::String(tag) => tag.to_string(),
                    other => {
                        return InfluxDbHandlerNoCause {
                            msg: format!("invalid tag column, column:{other:?}"),
                        }
                        .fail()
                    }
                }
            };
            group_by_tag_values.push(tag);
        }

        Ok(GroupKey {
            measurement,
            group_by_tag_values,
        })
    }

    fn extract_value_group(
        &self,
        record_batch: &RecordBatch,
        row_idx: usize,
    ) -> Result<Vec<Datum>> {
        let mut value_group = Vec::with_capacity(self.value_col_idxs.len());
        for col_idx in &self.value_col_idxs {
            let value = record_batch.column(*col_idx).datum(row_idx);

            value_group.push(value);
        }

        Ok(value_group)
    }
}

#[derive(Hash, PartialEq, Eq, Clone)]
struct GroupKey {
    measurement: String,
    group_by_tag_values: Vec<String>,
}

impl<Q: QueryExecutor + 'static> InfluxDb<Q> {
    pub fn new(instance: InstanceRef<Q>, schema_config_provider: SchemaConfigProviderRef) -> Self {
        Self {
            instance,
            schema_config_provider,
        }
    }

    async fn query(&self, ctx: RequestContext, req: QueryRequest) -> Result<InfluxqlResponse> {
        let output = handlers::query::handle_query(&ctx, self.instance.clone(), req)
            .await
            .box_err()
            .context(InfluxDbHandlerWithCause {
                msg: "failed to query by influxql",
            })?;

        convert_influxql_output(output)
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
            .with_context(|| InfluxDbHandlerWithCause {
                msg: format!("get schema config failed, schema:{schema}"),
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
        .with_context(|| InfluxDbHandlerWithCause {
            msg: "write request to insert plan",
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
            .with_context(|| InfluxDbHandlerWithCause {
                msg: "execute plan",
            })?;
        }
        debug!(
            "Influxdb write finished, catalog:{}, schema:{}, success:{}",
            catalog, schema, success
        );

        Ok(())
    }
}

fn convert_write_request(req: WriteRequest) -> Result<Vec<WriteTableRequest>> {
    let mut req_by_measurement = HashMap::new();
    let default_ts = Timestamp::now().as_i64();
    for line in influxdb_line_protocol::parse_lines(&req.lines) {
        let mut line = line.box_err().with_context(|| InfluxDbHandlerWithCause {
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

fn convert_influxql_output(output: Output) -> Result<InfluxqlResponse> {
    // TODO: now, we just support one influxql in each query.
    let records = match output {
        Output::Records(records) => records,
        Output::AffectedRows(_) => {
            return InfluxDbHandlerNoCause {
                msg: "output in influxql should not be affected rows",
            }
            .fail()
        }
    };

    let influxql_result = if records.is_empty() {
        OneInfluxqlResult {
            statement_id: 0,
            series: None,
        }
    } else {
        // All record schemas in one query result should be same.
        let record_schema = records.first().unwrap().schema();
        let mut builder = InfluxqlResultBuilder::new(record_schema, 0)?;
        for record in records {
            builder.add_record_batch(record)?;
        }

        builder.build()
    };

    Ok(InfluxqlResponse {
        results: vec![influxql_result],
    })
}

// TODO: Request and response type don't match influxdb's API now.
pub async fn query<Q: QueryExecutor + 'static>(
    ctx: RequestContext,
    db: Arc<InfluxDb<Q>>,
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
    db: Arc<InfluxDb<Q>>,
    req: WriteRequest,
) -> std::result::Result<impl Reply, Rejection> {
    db.write(ctx, req)
        .await
        .map_err(reject::custom)
        .map(|_| warp::http::StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
    use common_types::{
        column::{ColumnBlock, ColumnBlockBuilder},
        column_schema,
        datum::DatumKind,
        schema,
        string::StringBytes,
    };
    use json_pretty::PrettyFormatter;

    use super::*;

    #[test]
    fn test_convert_influxdb_write_req() {
        let lines = r#"
            demo,tag1=t1,tag2=t2 field1=90,field2=100 1678675992000
            demo,tag1=t1,tag2=t2 field1=91,field2=101 1678675993000
            demo,tag1=t11,tag2=t22 field1=900,field2=1000 1678675992000
            demo,tag1=t11,tag2=t22 field1=901,field2=1001 1678675993000
            "#
        .to_string();
        let req = WriteRequest {
            lines,
            precision: Precision::Millisecond,
        };

        let pb_req = convert_write_request(req).unwrap();
        assert_eq!(1, pb_req.len());
        assert_eq!(
            pb_req[0],
            WriteTableRequest {
                table: "demo".to_string(),
                tag_names: vec!["tag1".to_string(), "tag2".to_string()],
                field_names: vec!["field1".to_string(), "field2".to_string()],
                entries: vec![
                    // First series
                    WriteSeriesEntry {
                        tags: vec![
                            Tag {
                                name_index: 0,
                                value: Some(convert_influx_value(FieldValue::String("t1".into()))),
                            },
                            Tag {
                                name_index: 1,
                                value: Some(convert_influx_value(FieldValue::String("t2".into()))),
                            },
                        ],
                        field_groups: vec![
                            FieldGroup {
                                timestamp: 1678675992000,
                                fields: vec![
                                    Field {
                                        name_index: 0,
                                        value: Some(convert_influx_value(FieldValue::F64(90.0))),
                                    },
                                    Field {
                                        name_index: 1,
                                        value: Some(convert_influx_value(FieldValue::F64(100.0))),
                                    }
                                ]
                            },
                            FieldGroup {
                                timestamp: 1678675993000,
                                fields: vec![
                                    Field {
                                        name_index: 0,
                                        value: Some(convert_influx_value(FieldValue::F64(91.0))),
                                    },
                                    Field {
                                        name_index: 1,
                                        value: Some(convert_influx_value(FieldValue::F64(101.0))),
                                    }
                                ]
                            },
                        ]
                    },
                    // Second series
                    WriteSeriesEntry {
                        tags: vec![
                            Tag {
                                name_index: 0,
                                value: Some(convert_influx_value(FieldValue::String("t11".into()))),
                            },
                            Tag {
                                name_index: 1,
                                value: Some(convert_influx_value(FieldValue::String("t22".into()))),
                            },
                        ],
                        field_groups: vec![
                            FieldGroup {
                                timestamp: 1678675992000,
                                fields: vec![
                                    Field {
                                        name_index: 0,
                                        value: Some(convert_influx_value(FieldValue::F64(900.0))),
                                    },
                                    Field {
                                        name_index: 1,
                                        value: Some(convert_influx_value(FieldValue::F64(1000.0))),
                                    }
                                ]
                            },
                            FieldGroup {
                                timestamp: 1678675993000,
                                fields: vec![
                                    Field {
                                        name_index: 0,
                                        value: Some(convert_influx_value(FieldValue::F64(901.0))),
                                    },
                                    Field {
                                        name_index: 1,
                                        value: Some(convert_influx_value(FieldValue::F64(1001.0))),
                                    }
                                ]
                            },
                        ]
                    }
                ]
            }
        );
    }

    #[test]
    fn test_influxql_result() {
        let record_schema = build_test_record_schema();
        let column_blocks = build_test_column_blocks();
        let record_batch = RecordBatch::new(record_schema, column_blocks).unwrap();

        let mut builder = InfluxqlResultBuilder::new(record_batch.schema(), 0).unwrap();
        builder.add_record_batch(record_batch).unwrap();
        let iql_results = vec![builder.build()];
        let iql_response = InfluxqlResponse {
            results: iql_results,
        };
        let iql_result_json =
            PrettyFormatter::from_str(&serde_json::to_string(&iql_response).unwrap()).pretty();
        let expected = PrettyFormatter::from_str(r#"{"results":[{"statement_id":0,"series":[{"name":"m1","tags":{"tag":"tv1"},
                            "columns":["time","field1","field2"],"values":[[10001,"fv1",1]]},
                            {"name":"m1","tags":{"tag":"tv2"},"columns":["time","field1","field2"],"values":[[100002,"fv2",2]]},
                            {"name":"m1","tags":{"tag":"tv3"},"columns":["time","field1","field2"],"values":[[10003,"fv3",3]]},
                            {"name":"m1","tags":{"tag":""},"columns":["time","field1","field2"],"values":[[10007,null,null]]},
                            {"name":"m2","tags":{"tag":"tv4"},"columns":["time","field1","field2"],"values":[[10004,"fv4",4]]},
                            {"name":"m2","tags":{"tag":"tv5"},"columns":["time","field1","field2"],"values":[[100005,"fv5",5]]},
                            {"name":"m2","tags":{"tag":"tv6"},"columns":["time","field1","field2"],"values":[[10006,"fv6",6]]}]}]}"#).pretty();
        assert_eq!(expected, iql_result_json);
    }

    fn build_test_record_schema() -> RecordSchema {
        let schema = schema::Builder::new()
            .auto_increment_column_id(true)
            .add_key_column(
                column_schema::Builder::new("time".to_string(), DatumKind::Timestamp)
                    .build()
                    .expect("should succeed build column schema"),
            )
            .unwrap()
            .add_normal_column(
                column_schema::Builder::new("tag".to_string(), DatumKind::String)
                    .is_tag(true)
                    .is_nullable(true)
                    .build()
                    .expect("should succeed build column schema"),
            )
            .unwrap()
            .add_normal_column(
                column_schema::Builder::new("field1".to_string(), DatumKind::String)
                    .is_nullable(true)
                    .build()
                    .expect("should succeed build column schema"),
            )
            .unwrap()
            .add_normal_column(
                // The data type of column is `UInt32`, and the type of default value expr is
                // `Int64`. So we use this column to cover the test, which has
                // different type.
                column_schema::Builder::new("field2".to_string(), DatumKind::UInt64)
                    .is_nullable(true)
                    .build()
                    .expect("should succeed build column schema"),
            )
            .unwrap()
            .build()
            .unwrap();

        // Record schema
        let arrow_schema = schema.to_arrow_schema_ref();
        let fields = arrow_schema.fields.to_owned();
        let measurement_field = ArrowField::new(
            "ceresdb::measurement".to_string(),
            schema::DataType::Utf8,
            false,
        );
        let project_fields = vec![
            measurement_field,
            fields[1].clone(),
            fields[0].clone(),
            fields[2].clone(),
            fields[3].clone(),
        ];
        let project_arrow_schema = Arc::new(ArrowSchema::new_with_metadata(
            project_fields,
            arrow_schema.metadata().clone(),
        ));

        RecordSchema::try_from(project_arrow_schema).unwrap()
    }

    fn build_test_column_blocks() -> Vec<ColumnBlock> {
        let mut measurement_builder = ColumnBlockBuilder::with_capacity(&DatumKind::String, 3);
        let mut tag_builder = ColumnBlockBuilder::with_capacity(&DatumKind::String, 3);
        let mut time_builder = ColumnBlockBuilder::with_capacity(&DatumKind::Timestamp, 3);
        let mut field_builder1 = ColumnBlockBuilder::with_capacity(&DatumKind::String, 3);
        let mut field_builder2 = ColumnBlockBuilder::with_capacity(&DatumKind::UInt64, 3);

        // Data in measurement1
        let measurement1 = Datum::String(StringBytes::copy_from_str("m1"));
        let tags1 = vec!["tv1".to_string(), "tv2".to_string(), "tv3".to_string()]
            .into_iter()
            .map(|v| Datum::String(StringBytes::copy_from_str(v.as_str())))
            .collect::<Vec<_>>();
        let times1 = vec![10001_i64, 100002, 10003]
            .into_iter()
            .map(|v| Datum::Timestamp(v.into()))
            .collect::<Vec<_>>();
        let fields1 = vec!["fv1".to_string(), "fv2".to_string(), "fv3".to_string()]
            .into_iter()
            .map(|v| Datum::String(StringBytes::copy_from_str(v.as_str())))
            .collect::<Vec<_>>();
        let fields2 = vec![1_u64, 2, 3]
            .into_iter()
            .map(Datum::UInt64)
            .collect::<Vec<_>>();

        let measurement2 = Datum::String(StringBytes::copy_from_str("m2"));
        let tags2 = vec!["tv4".to_string(), "tv5".to_string(), "tv6".to_string()]
            .into_iter()
            .map(|v| Datum::String(StringBytes::copy_from_str(v.as_str())))
            .collect::<Vec<_>>();
        let times2 = vec![10004_i64, 100005, 10006]
            .into_iter()
            .map(|v| Datum::Timestamp(v.into()))
            .collect::<Vec<_>>();
        let fields3 = vec!["fv4".to_string(), "fv5".to_string(), "fv6".to_string()]
            .into_iter()
            .map(|v| Datum::String(StringBytes::copy_from_str(v.as_str())))
            .collect::<Vec<_>>();
        let fields4 = vec![4_u64, 5, 6]
            .into_iter()
            .map(Datum::UInt64)
            .collect::<Vec<_>>();

        for idx in 0..3 {
            measurement_builder.append(measurement1.clone()).unwrap();
            tag_builder.append(tags1[idx].clone()).unwrap();
            time_builder.append(times1[idx].clone()).unwrap();
            field_builder1.append(fields1[idx].clone()).unwrap();
            field_builder2.append(fields2[idx].clone()).unwrap();
        }
        measurement_builder.append(measurement1).unwrap();
        tag_builder.append(Datum::Null).unwrap();
        time_builder.append(Datum::Timestamp(10007.into())).unwrap();
        field_builder1.append(Datum::Null).unwrap();
        field_builder2.append(Datum::Null).unwrap();

        for idx in 0..3 {
            measurement_builder.append(measurement2.clone()).unwrap();
            tag_builder.append(tags2[idx].clone()).unwrap();
            time_builder.append(times2[idx].clone()).unwrap();
            field_builder1.append(fields3[idx].clone()).unwrap();
            field_builder2.append(fields4[idx].clone()).unwrap();
        }

        vec![
            measurement_builder.build(),
            tag_builder.build(),
            time_builder.build(),
            field_builder1.build(),
            field_builder2.build(),
        ]
    }
}
