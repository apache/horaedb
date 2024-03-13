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

use clap::Subcommand;

use crate::{
    cmd::cluster_schedule::{schedule_resolve, ScheduleCommands},
    operation::cluster::{clusters_diagnose, clusters_list},
    util::CLUSTER_NAME,
};

#[derive(Subcommand)]
pub enum ClusterCommands {
    #[clap(about = "Cluster list", long_about = None)]
    #[clap(alias = "l")]
    List,

    #[clap(about = "Cluster diagnose", long_about = None)]
    #[clap(alias = "d")]
    Diagnose,

    #[clap(about = "Cluster schedule", long_about = None)]
    #[clap(alias = "s")]
    Schedule {
        #[clap(subcommand)]
        commands: Option<ScheduleCommands>,
    },
}

pub async fn cluster_resolve(cluster_name: Option<String>, command: Option<ClusterCommands>) {
    if let Some(name) = cluster_name {
        let mut cluster_name = CLUSTER_NAME.lock().unwrap();
        *cluster_name = name;
    }
    match command {
        Some(ClusterCommands::List) => clusters_list().await,
        Some(ClusterCommands::Diagnose) => clusters_diagnose().await,
        Some(ClusterCommands::Schedule { commands }) => schedule_resolve(commands).await,
        None => {}
    }
}
