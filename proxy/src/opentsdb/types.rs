// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, HashSet},
    fmt::{Debug, Display, Formatter},
    num::ParseFloatError,
};

use bytes::Bytes;
use ceresdbproto::storage::{
    value, Field, FieldGroup, Tag, Value as ProtoValue, WriteSeriesEntry, WriteTableRequest,
};
use common_util::{error::BoxError, time::try_to_millis};
use http::StatusCode;
use serde::Deserialize;
use serde_json::from_slice;
use snafu::{OptionExt, ResultExt};

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
    IntegerValue(i64),
    F64Value(f64),
    StringValue(String),
}

impl Value {
    fn try_to_f64(&self) -> std::result::Result<f64, ParseFloatError> {
        match self {
            Value::IntegerValue(v) => Ok(*v as f64),
            Value::F64Value(v) => Ok(*v),
            Value::StringValue(v) => v.parse(),
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::IntegerValue(v) => Display::fmt(v, f),
            Value::F64Value(v) => Display::fmt(v, f),
            Value::StringValue(v) => Display::fmt(v, f),
        }
    }
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

    let mut points_per_metric = HashMap::new();
    for point in points.into_iter() {
        points_per_metric
            .entry(point.metric.clone())
            .or_insert(Vec::new())
            .push(point);
    }

    let mut requests = Vec::with_capacity(points_per_metric.len());
    for (metric, points) in points_per_metric {
        let mut tag_names_set = HashSet::new();
        for point in points.iter() {
            for tag_name in point.tags.keys() {
                tag_names_set.insert(tag_name.clone());
            }
        }

        let mut tag_name_to_tag_index: HashMap<String, u32> = HashMap::new();
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

            let f64_value = point
                .value
                .try_to_f64()
                .box_err()
                .with_context(|| ErrWithCause {
                    code: StatusCode::BAD_REQUEST,
                    msg: format!("Can't parse to f64: {}", point.value),
                })?;
            let fields = vec![Field {
                name_index: 0,
                value: Some(ProtoValue {
                    value: Some(value::Value::Float64Value(f64_value)),
                }),
            }];

            let field_groups = vec![FieldGroup { timestamp, fields }];

            req.entries.push(WriteSeriesEntry { tags, field_groups });
        }
        requests.push(req);
    }

    Ok(requests)
}

pub(crate) fn validate(points: &[Point]) -> Result<()> {
    for point in points.iter() {
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
