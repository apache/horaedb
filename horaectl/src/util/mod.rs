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

use std::sync::Mutex;

use chrono::{TimeZone, Utc};
use lazy_static::lazy_static;
use prettytable::{Cell, Row, Table};

lazy_static! {
    pub static ref META_ADDR: Mutex<String> = Mutex::new(String::new());
    pub static ref CLUSTER_NAME: Mutex<String> = Mutex::new(String::new());
}

pub const HTTP: &str = "http://";
pub const API: &str = "/api/v1";
pub const DEBUG: &str = "/debug";
pub const CLUSTERS: &str = "/clusters";
pub static CLUSTERS_LIST_HEADER: [&str; 7] = [
    "ID",
    "Name",
    "ShardTotal",
    "TopologyType",
    "ProcedureExecutingBatchSize",
    "CreatedAt",
    "ModifiedAt",
];
pub static CLUSTERS_DIAGNOSE_HEADER: [&str; 4] = [
    "unregistered_shards",
    "unready_shards:shard_id",
    "unready_shards:node_name",
    "unready_shards:status",
];
pub static CLUSTERS_ENABLE_SCHEDULE_HEADER: [&str; 1] = ["enable_schedule"];

pub fn table_writer(header: &[&str]) -> Table {
    let mut table = Table::new();
    let header_row = Row::from_iter(header.iter().map(|&entry| Cell::new(entry)));
    table.add_row(header_row);
    table
}

pub fn format_time_milli(milli: i64) -> String {
    let datetime = Utc.timestamp_millis_opt(milli).single().unwrap();
    datetime.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}
