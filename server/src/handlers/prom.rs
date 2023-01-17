// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use async_trait::async_trait;
use log::debug;
use prom_remote_api::types::{
    label_matcher, ReadRequest, ReadResponse, RemoteStorage, Result, WriteRequest,
};
use query_engine::executor::Executor as QueryExecutor;
use snafu::{OptionExt, Snafu};

use crate::instance::InstanceRef;

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Metric name is not found"))]
    MissingMetricName,

    #[snafu(display("Invalid matcher type, value:{}", value))]
    InvalidMatcherType { value: i32 },
}

const NAME_LABEL: &str = "__name__";
const TIMESTAMP_NAME: &str = "timestamp";
const VALUE_COLUMN: &str = "value";

pub struct CeresDBStorage<Q: QueryExecutor + 'static> {
    #[allow(dead_code)]
    instance: InstanceRef<Q>,
}

impl<Q: QueryExecutor + 'static> CeresDBStorage<Q> {
    pub fn new(instance: InstanceRef<Q>) -> Self {
        Self { instance }
    }
}

#[async_trait]
impl<Q: QueryExecutor + 'static> RemoteStorage for CeresDBStorage<Q> {
    /// Write samples to remote storage
    async fn write(&self, req: WriteRequest) -> Result<()> {
        debug!("mock write, req:{req:?}");

        unimplemented!()
    }

    /// Read samples from remote storage
    async fn read(&self, req: ReadRequest) -> Result<ReadResponse> {
        println!("mock query, req:{req:?}");
        let q = &req.queries[0];

        let mut filters = Vec::with_capacity(q.matchers.len());
        filters.push(format!(
            "{} between ({}, {})",
            TIMESTAMP_NAME, q.start_timestamp_ms, q.end_timestamp_ms
        ));
        let mut measurement = None;
        for m in &q.matchers {
            if m.name == NAME_LABEL {
                measurement = Some(m.value.to_string());
                continue;
            }

            let filter = match m.r#type() {
                label_matcher::Type::Eq => format!("{} = {}", m.name, m.value),
                label_matcher::Type::Neq => format!("{} != {}", m.name, m.value),
                label_matcher::Type::Re => format!("regexp_match({}, {})", m.name, m.value),
                label_matcher::Type::Nre => format!("not(regexp_match({}, {}))", m.name, m.value),
            };
            filters.push(filter)
        }

        let measurement = measurement.context(MissingMetricName).unwrap();
        let sql = format!(
            "select {} from {} where {}",
            VALUE_COLUMN,
            measurement,
            filters.join(" and ")
        );
        Ok(ReadResponse::default())
    }
}
