// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! This module implements [write][1] and [query][2] for InfluxDB.
//! [1]: https://docs.influxdata.com/influxdb/v1.8/tools/api/#write-http-endpoint
//! [2]: https://docs.influxdata.com/influxdb/v1.8/tools/api/#query-http-endpoint

use std::{collections::HashMap, sync::Arc, time::Instant};

use bytes::Bytes;
use ceresdbproto::storage::{
    value, Field, FieldGroup, Tag, Value, WriteSeriesEntry, WriteTableRequest,
};
use common_types::{
    column_schema::{self, ColumnSchema},
    datum::{Datum, DatumKind},
    record_batch::RecordBatch,
    request_id::RequestId,
    schema::RecordSchema,
    time::Timestamp,
};
use common_util::error::BoxError;
use handlers::{
    error::{InfluxDbHandler, Result},
    query::QueryRequest,
};
use influxdb_line_protocol::FieldValue;
use interpreters::interpreter::Output;
use log::debug;
use query_engine::executor::Executor as QueryExecutor;
use serde::{
    ser::{SerializeMap, SerializeSeq},
    Serialize,
};
use snafu::{ensure, OptionExt, ResultExt};
use sql::influxql::planner::CERESDB_MEASUREMENT_COLUMN_NAME;
use warp::{reject, reply, Rejection, Reply};

use crate::{
    context::RequestContext,
    handlers,
    handlers::error::InfluxDbHandlerInternal,
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

/// One result in group(defined by group by clause)
///
/// Influxdb names the result set series, so name each result in the set
/// `OneSeries` here. Its format is like:
/// {
// 	"results": [{
/// 		"statement_id": 0,
/// 		"series": [{
/// 			"name": "home",
/// 			"tags": {
/// 				"room":  "Living Room"
/// 			},
/// 			"columns": ["time", "co", "hum", "temp"],
/// 			"values": [["2022-01-01T08:00:00Z", 0, 35.9, 21.1], ... ]
/// 		}, ... ]
/// 	}, ... ]
/// }
#[derive(Debug, Serialize)]
pub struct InfluxqlResponse {
    results: Vec<InfluxqlResult>,
}

#[derive(Debug, Serialize)]
pub struct InfluxqlResult {
    statement_id: u32,
    series: Vec<OneSeries>,
}

#[derive(Debug)]
pub struct OneSeries {
    name: String,
    tags: Option<Tags>,
    columns: Vec<String>,
    values: Vec<Vec<Datum>>,
}

impl Serialize for OneSeries {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut one_series = serializer.serialize_map(Some(4))?;
        one_series.serialize_entry("name", &self.name)?;
        if let Some(tags) = &self.tags {
            one_series.serialize_entry("tags", &tags)?;
        }
        one_series.serialize_entry("columns", &self.columns)?;
        one_series.serialize_entry("values", &self.values)?;

        one_series.end()
    }
}

#[derive(Debug)]
struct Tags(Vec<(String, String)>);

impl Serialize for Tags {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut tags = serializer.serialize_map(Some(self.0.len()))?;

        for (tagk, tagv) in &self.0 {
            tags.serialize_entry(tagk, tagv)?;
        }

        tags.end()
    }
}

/// Influxql response builder
// #[derive(Serialize)]
// #[serde(rename_all = "snake_case")]
#[derive(Default)]
pub struct InfluxqlResultBuilder {
    statement_id: u32,

    /// Schema of influxql query result
    ///
    /// Its format is like: measurement | tag_1..tag_n(defined by group by
    /// clause) | time | value_column_1..value_column_n
    column_schemas: Vec<ColumnSchema>,

    /// Tags part in schema
    group_by_tag_col_idxs: Vec<usize>,

    /// Value columns part in schema(include `time`)
    value_col_idxs: Vec<usize>,

    /// Mapping series key(measurement + tag values) to column data
    group_key_to_idx: HashMap<GroupKey, usize>,

    /// Column datas
    value_groups: Vec<Vec<Vec<Datum>>>,
}

impl InfluxqlResultBuilder {
    fn new(record_schema: &RecordSchema, statement_id: u32) -> Result<Self> {
        let column_schemas = record_schema.columns().to_owned();

        // Find the tags part and columns part from schema.
        let mut group_by_col_idxs = Vec::new();
        let mut value_col_idxs = Vec::new();

        let mut col_iter = column_schemas.iter().enumerate();
        // The first column may be measurement column in normal.
        ensure!(col_iter.next().unwrap().1.name == CERESDB_MEASUREMENT_COLUMN_NAME, InfluxDbHandlerInternal {
            msg: format!("invalid schema whose first column is not measurement column, schema:{column_schemas:?}"),
        });

        // The group by tags will be placed after measurement and before time column.
        let mut searching_group_by_tags = true;
        while let Some((idx, col)) = col_iter.next() {
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

    fn add_record_batch(mut self, record_batch: RecordBatch) -> Result<Self> {
        // Check schema's compatibility.
        ensure!(
            record_batch.schema().columns() == &self.column_schemas,
            InfluxDbHandlerInternal {
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

        Ok(self)
    }

    fn build(self) -> InfluxqlResult {
        let ordered_group_keys = {
            let mut ordered_pairs = self
                .group_key_to_idx
                .clone()
                .into_iter()
                .collect::<Vec<_>>();
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
                let tags = group_key
                    .group_by_tag_values
                    .into_iter()
                    .enumerate()
                    .map(|(tagk_idx, tagv)| {
                        let tagk_col_idx = self.group_by_tag_col_idxs[tagk_idx];
                        let tagk = self.column_schemas[tagk_col_idx].name.clone();

                        (tagk, tagv)
                    })
                    .collect::<Vec<_>>();

                let columns = self
                    .value_col_idxs
                    .iter()
                    .map(|idx| self.column_schemas[*idx].name.clone())
                    .collect::<Vec<_>>();

                OneSeries {
                    name,
                    tags: Some(Tags(tags)),
                    columns,
                    values: value_group,
                }
            })
            .collect();

        InfluxqlResult {
            series,
            statement_id: self.statement_id,
        }
    }

    fn extract_group_key(&self, record_batch: &RecordBatch, row_idx: usize) -> Result<GroupKey> {
        let mut group_by_tag_values = Vec::with_capacity(self.group_by_tag_col_idxs.len());
        let measurement = {
            let measurement = record_batch.column(0).datum(row_idx);
            if let Datum::String(m) = measurement {
                m.to_string()
            } else {
                return InfluxDbHandlerInternal { msg: "" }.fail();
            }
        };

        for col_idx in &self.group_by_tag_col_idxs {
            let tag = {
                let tag_datum = record_batch.column(*col_idx).datum(row_idx);
                match tag_datum {
                    Datum::Null => "".to_string(),
                    Datum::String(tag) => tag.to_string(),
                    _ => return InfluxDbHandlerInternal { msg: "" }.fail(),
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

#[derive(Hash, PartialEq, Eq)]
struct TagKv {
    key: String,
    value: String,
}

struct Columns {
    names: Vec<String>,
    data: Vec<Datum>,
}

impl<Q: QueryExecutor + 'static> InfluxDb<Q> {
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
            .with_context(|| InfluxDbHandler {
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
        .with_context(|| InfluxDbHandler {
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
            .with_context(|| InfluxDbHandler {
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
        let mut line = line.box_err().with_context(|| InfluxDbHandler {
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

// fn convert_query_result(output: Output) -> {

// }

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
    use common_types::tests::build_schema;

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
    fn test_print() {


        let one_series = OneSeries {
            name: "test".to_string(),
            tags: None,
            columns: vec!["column1".to_string(), "column2".to_string()],
            values: vec![
                vec![Datum::Int32(1), Datum::Int32(2)],
                vec![Datum::Int32(2), Datum::Int32(2)],
            ],
        };

        println!("{}", serde_json::to_string(&one_series).unwrap());
    }
}
