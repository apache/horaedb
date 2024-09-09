// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;

use common_types::time::TimeRange;
use wal::manager::WalManagerRef;

use crate::{
    sst::SSTable,
    types::{Predicate, Row},
    Result,
};

pub const METRICS_TABLE: &str = "__metrics__";
pub const SERIES_TABLE: &str = "__series__";
pub const INDEX_TABLE: &str = "__index__";

pub struct TableManager {
    shard_id: u32,
    // Key: table name, Value: table storage.
    // table name is fixed, so we can use &'static str.
    all_tables: HashMap<&'static str, TableStorage>,
}

/// Storage to represent different components of the system.
/// Such as: metrics, series, indexes, etc.
///
/// Columns for design:
/// metrics: {MetricName}-{MetricID}-{FieldName}
/// series: {TSID}-{SeriesKey}
/// index: {TagKey}-{TagValue}-{TSID}
pub struct TableStorage {
    name: String,
    id: u64,
    // TODO: currently not used, all writes go to memory directly.
    _wal: Option<WalManagerRef>,
    sstables: Vec<SSTable>,
}

impl TableStorage {
    pub fn new(name: String, id: u64) -> Self {
        Self {
            name,
            id,
            _wal: None,
            sstables: Vec::new(),
        }
    }

    pub async fn write(row: Row) -> Result<()> {
        unimplemented!()
    }

    pub async fn scan(range: TimeRange, predicate: Predicate) -> Result<()> {
        unimplemented!()
    }

    pub async fn compact() -> Result<()> {
        unimplemented!()
    }
}
