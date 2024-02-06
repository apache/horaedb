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

use prettytable::row;

use crate::{
    operation::{
        ClusterResponse, DiagnoseShardResponse, EnableScheduleRequest, EnableScheduleResponse,
    },
    util::{
        format_time_milli, table_writer, API, CLUSTERS, CLUSTERS_DIAGNOSE_HEADER,
        CLUSTERS_ENABLE_SCHEDULE_HEADER, CLUSTERS_LIST_HEADER, CLUSTER_NAME, DEBUG, HTTP,
        META_ADDR,
    },
};

fn list_url() -> String {
    HTTP.to_string() + META_ADDR.get().unwrap() + API + CLUSTERS
}

fn diagnose_url() -> String {
    HTTP.to_string()
        + META_ADDR.get().unwrap()
        + DEBUG
        + "/diagnose"
        + "/"
        + CLUSTER_NAME.get().unwrap()
        + "/shards"
}

fn schedule_url() -> String {
    HTTP.to_string()
        + META_ADDR.get().unwrap()
        + DEBUG
        + CLUSTERS
        + "/"
        + CLUSTER_NAME.get().unwrap()
        + "/enableSchedule"
}

pub async fn clusters_list() {
    let res = match reqwest::get(list_url()).await {
        Ok(res) => res,
        Err(e) => {
            println!("{}", e);
            return;
        }
    };
    let response: ClusterResponse = match res.json().await {
        Ok(res) => res,
        Err(e) => {
            println!("{}", e);
            return;
        }
    };

    let mut table = table_writer(&CLUSTERS_LIST_HEADER);
    for cluster in response.data {
        table.add_row(row![
            cluster.id,
            cluster.name,
            cluster.shard_total.to_string(),
            cluster.topology_type,
            cluster.procedure_executing_batch_size.to_string(),
            format_time_milli(cluster.created_at),
            format_time_milli(cluster.modified_at)
        ]);
    }
    table.printstd();
}

pub async fn clusters_diagnose() {
    let res = match reqwest::get(diagnose_url()).await {
        Ok(res) => res,
        Err(e) => {
            println!("{}", e);
            return;
        }
    };
    let response: DiagnoseShardResponse = match res.json().await {
        Ok(res) => res,
        Err(e) => {
            println!("{}", e);
            return;
        }
    };
    let mut table = table_writer(&CLUSTERS_DIAGNOSE_HEADER);
    table.add_row(row![response
        .data
        .unregistered_shards
        .iter()
        .map(|shard_id| shard_id.to_string())
        .collect::<Vec<_>>()
        .join(", ")]);
    for (shard_id, data) in response.data.unready_shards {
        table.add_row(row!["", shard_id, data.node_name, data.status]);
    }
    table.printstd();
}

pub async fn clusters_schedule_get() {
    let res = match reqwest::get(schedule_url()).await {
        Ok(res) => res,
        Err(e) => {
            println!("{}", e);
            return;
        }
    };
    let response: EnableScheduleResponse = match res.json().await {
        Ok(res) => res,
        Err(e) => {
            println!("{}", e);
            return;
        }
    };
    let mut table = table_writer(&CLUSTERS_ENABLE_SCHEDULE_HEADER);
    let row = match response.data {
        Some(data) => row![data],
        None => row!["topology should in dynamic mode"],
    };
    table.add_row(row);
    table.printstd();
}

pub async fn clusters_schedule_set(enable: bool) {
    let request = EnableScheduleRequest { enable };

    let res = match reqwest::Client::new()
        .put(schedule_url())
        .json(&request)
        .send()
        .await
    {
        Ok(res) => res,
        Err(e) => {
            println!("{}", e);
            return;
        }
    };
    let response: EnableScheduleResponse = match res.json().await {
        Ok(res) => res,
        Err(e) => {
            println!("{}", e);
            return;
        }
    };
    let mut table = table_writer(&CLUSTERS_ENABLE_SCHEDULE_HEADER);
    let row = match response.data {
        Some(data) => row![data],
        None => row!["topology should in dynamic mode"],
    };
    table.add_row(row);
    table.printstd();
}
