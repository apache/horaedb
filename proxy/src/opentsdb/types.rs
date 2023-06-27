// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use ceresdbproto::storage::{
    value, Field, FieldGroup, Tag, Value, WriteSeriesEntry, WriteTableRequest,
};
use common_util::error::BoxError;
use itertools::Itertools;
use serde::Deserialize;
use serde_json::from_slice;
use snafu::ResultExt;

use crate::error::{Internal, InternalNoCause, Result};

const OPENTSDB_DEFAULT_FIELD: &str = "value";

#[derive(Debug)]
pub struct PutRequest {
    pub points: Bytes,

    pub summary: bool,
    pub details: bool,
    pub sync: bool,
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
///     - `db` is not required and default to `public` in CeresDB.
///     - `precision`'s default value is `ms` but not `ns` in CeresDB.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct PutParams {
    // TODO: How to represent a `Present` param ?
    // now use bool to represent it, so the URL must be "?summary=true/false"
    // but the `Present` datatype means "?summary" is ok.
    pub summary: bool,
    pub details: bool,
    pub sync: bool,
    pub sync_timeout: i32,
    // TODO: now these params is unimplemented.
}

#[derive(Debug, Deserialize)]
pub struct Point {
    pub metric: String,
    pub timestamp: u64,
    // TODO: OpenTSDB's value support Integer, Float, String. How to represent it?
    pub value: f64,
    pub tags: HashMap<String, String>,
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
    let points = points.box_err().with_context(|| Internal {
        msg: "json parse error",
    })?;
    validate(&points)?;

    let mut points_per_metric = HashMap::new();
    for point in points.into_iter() {
        points_per_metric
            .entry(point.metric.clone())
            .or_insert(Vec::new())
            .push(point);
    }

    let requests = points_per_metric
        .into_iter()
        .map(|(metric, points)| {
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
                entries: Vec::new(),
            };

            for point in points {
                let mut timestamp = point.timestamp;
                // TODO: Is there a util function to detect timestamp precision and convert it?
                if timestamp < 1000000000000 {
                    // seconds
                    timestamp *= 1000;
                } // else milliseconds

                let mut tags = Vec::with_capacity(point.tags.len());
                for (tag_name, tag_value) in point.tags {
                    let &tag_index = tag_name_to_tag_index.get(&tag_name).unwrap();
                    tags.push(Tag {
                        name_index: tag_index,
                        value: Some(Value {
                            value: Some(value::Value::StringValue(tag_value)),
                        }),
                    });
                }

                let fields = vec![Field {
                    name_index: 0,
                    value: Some(Value {
                        value: Some(value::Value::Float64Value(point.value)),
                    }),
                }];

                let field_groups = vec![FieldGroup {
                    timestamp: timestamp as i64,
                    fields,
                }];

                req.entries.push(WriteSeriesEntry { tags, field_groups });
            }

            req
        })
        .collect_vec();

    Ok(requests)
}

pub(crate) fn validate(points: &[Point]) -> Result<()> {
    for point in points.iter() {
        if point.metric.is_empty() {
            return InternalNoCause {
                msg: "`params` is not supported now",
            }
            .fail();
        }
        if point.tags.is_empty() {
            return InternalNoCause {
                msg: "at least one tag must be supplied",
            }
            .fail();
        }
        for tag_name in point.tags.keys() {
            if tag_name.is_empty() {
                return InternalNoCause {
                    msg: "tag name must not be empty",
                }
                .fail();
            }
        }
    }

    Ok(())
}
