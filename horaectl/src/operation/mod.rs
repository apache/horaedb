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

pub mod cluster;

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
#[derive(Deserialize, Debug)]
pub struct Cluster {
    #[serde(rename = "ID")]
    id: u32,
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "ShardTotal")]
    shard_total: u32,
    #[serde(rename = "TopologyType")]
    topology_type: String,
    #[serde(rename = "ProcedureExecutingBatchSize")]
    procedure_executing_batch_size: u32,
    #[serde(rename = "CreatedAt")]
    created_at: i64,
    #[serde(rename = "ModifiedAt")]
    modified_at: i64,
}

#[derive(Deserialize, Debug)]
pub struct ClusterResponse {
    #[allow(unused)]
    status: String,
    data: Vec<Cluster>,
}

#[derive(Deserialize, Debug)]
pub struct DiagnoseShardStatus {
    #[serde(rename = "nodeName")]
    node_name: String,
    status: String,
}

#[derive(Deserialize, Debug)]
pub struct DiagnoseShard {
    #[serde(rename = "unregisteredShards")]
    unregistered_shards: Vec<u32>,
    #[serde(rename = "unreadyShards")]
    unready_shards: HashMap<u32, DiagnoseShardStatus>,
}

#[derive(Deserialize, Debug)]
pub struct DiagnoseShardResponse {
    #[allow(unused)]
    status: String,
    data: DiagnoseShard,
}

#[derive(Serialize)]
pub struct EnableScheduleRequest {
    enable: bool,
}

#[derive(Deserialize)]
pub struct EnableScheduleResponse {
    #[allow(unused)]
    status: String,
    data: Option<bool>,
}
