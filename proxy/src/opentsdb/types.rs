// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
};

use bytes::Bytes;
use ceresdbproto::storage::{
    value, Field, FieldGroup, Tag, Value as ProtoValue, WriteSeriesEntry, WriteTableRequest,
};
use generic_error::BoxError;
use http::StatusCode;
use serde::Deserialize;
use serde_json::from_slice;
use snafu::{OptionExt, ResultExt};
use time_ext::try_to_millis;

use crate::error::{ErrNoCause, ErrWithCause, Result};

const OPENTSDB_DEFAULT_FIELD: &str = "value";

#[derive(Debug)]
pub struct PutRequest {
    pub points: Bytes,

    pub summary: Option<String>,
    pub details: Option<String>,
    pub sync: Option<String>,
    pub sync_timeout: i32,
}

impl PutRequest {
    pub fn new(points: Bytes, params: PutParams) -> Self {
        PutRequest {
            points,
            summary: params.summary,
            details: params.details,
            sync: params.sync,
            sync_timeout: params.sync_timeout,
        }
    }
}

pub type PutResponse = ();

/// Query string parameters for put api
///
/// It's derived from query string parameters of put described in
/// doc of OpenTSDB 2.4:
///     http://opentsdb.net/docs/build/html/api_http/put.html#requests
///
/// NOTE:
///     - all the params is unimplemented.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct PutParams {
    pub summary: Option<String>,
    pub details: Option<String>,
    pub sync: Option<String>,
    pub sync_timeout: i32,
}

#[derive(Debug, Deserialize)]
pub struct Point {
    pub metric: String,
    pub timestamp: i64,
    pub value: Value,
    pub tags: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum Value {
    // TODO: telegraf parse 0.0 as 0, which will confuse int and double type
    // IntegerValue(i64),
    F64Value(f64),
}

pub(crate) fn convert_put_request(req: PutRequest) -> Result<Vec<WriteTableRequest>> {
    let points = {
        // multi points represent as json array
        let parse_array = from_slice::<Vec<Point>>(&req.points);
        match parse_array {
            Ok(points) => Ok(points),
            Err(_e) => {
                // single point represent as json object
                let parse_object = from_slice::<Point>(&req.points);
                match parse_object {
                    Ok(point) => Ok(vec![point]),
                    Err(e) => Err(e),
                }
            }
        }
    };
    let points = points.box_err().with_context(|| ErrWithCause {
        code: StatusCode::BAD_REQUEST,
        msg: "Json parse error".to_string(),
    })?;
    validate(&points)?;

    let mut points_per_metric = HashMap::with_capacity(100);
    for point in points {
        points_per_metric
            .entry(point.metric.clone())
            .or_insert(Vec::new())
            .push(point);
    }

    let mut requests = Vec::with_capacity(points_per_metric.len());
    for (metric, points) in points_per_metric {
        let mut tag_names_set = HashSet::with_capacity(points[0].tags.len() * 2);
        for point in &points {
            for tag_name in point.tags.keys() {
                tag_names_set.insert(tag_name.clone());
            }
        }

        let mut tag_name_to_tag_index: HashMap<String, u32> =
            HashMap::with_capacity(tag_names_set.len());
        let mut tag_names = Vec::with_capacity(tag_names_set.len());
        for (idx, tag_name) in tag_names_set.into_iter().enumerate() {
            tag_name_to_tag_index.insert(tag_name.clone(), idx as u32);
            tag_names.push(tag_name);
        }

        let mut req = WriteTableRequest {
            table: metric,
            tag_names,
            field_names: vec![String::from(OPENTSDB_DEFAULT_FIELD)],
            entries: Vec::with_capacity(points.len()),
        };

        for point in points {
            let timestamp = point.timestamp;
            let timestamp = try_to_millis(timestamp)
                .with_context(|| ErrNoCause {
                    code: StatusCode::BAD_REQUEST,
                    msg: format!("Invalid timestamp: {}", point.timestamp),
                })?
                .as_i64();

            let mut tags = Vec::with_capacity(point.tags.len());
            for (tag_name, tag_value) in point.tags {
                let &tag_index = tag_name_to_tag_index.get(&tag_name).unwrap();
                tags.push(Tag {
                    name_index: tag_index,
                    value: Some(ProtoValue {
                        value: Some(value::Value::StringValue(tag_value)),
                    }),
                });
            }

            let value = match point.value {
                // Value::IntegerValue(v) => value::Value::Int64Value(v),
                Value::F64Value(v) => value::Value::Float64Value(v),
            };
            let fields = vec![Field {
                name_index: 0,
                value: Some(ProtoValue { value: Some(value) }),
            }];

            let field_groups = vec![FieldGroup { timestamp, fields }];

            req.entries.push(WriteSeriesEntry { tags, field_groups });
        }
        requests.push(req);
    }

    Ok(requests)
}

pub(crate) fn validate(points: &[Point]) -> Result<()> {
    for point in points {
        if point.metric.is_empty() {
            return ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: "Metric must not be empty",
            }
            .fail();
        }
        if point.tags.is_empty() {
            return ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: "At least one tag must be supplied",
            }
            .fail();
        }
        for tag_name in point.tags.keys() {
            if tag_name.is_empty() {
                return ErrNoCause {
                    code: StatusCode::BAD_REQUEST,
                    msg: "Tag name must not be empty",
                }
                .fail();
            }
        }
    }

    Ok(())
}
