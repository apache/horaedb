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

use clap::{ArgMatches, Command};

use crate::{
    cmd::{
        cluster_diagnose::diagnose,
        cluster_list::list,
        cluster_schedule::{schedule, schedule_resolve},
    },
    operation::cluster::{clusters_diagnose, clusters_list},
};

pub fn cluster() -> Command {
    Command::new("cluster")
        .about("Operations on cluster")
        .alias("c")
        .subcommand(list())
        .subcommand(diagnose())
        .subcommand(schedule())
}

pub async fn cluster_resolve(arg_matches: &ArgMatches) {
    match arg_matches.subcommand() {
        Some(("list", _)) => clusters_list().await,
        Some(("diagnose", _)) => clusters_diagnose().await,
        Some(("schedule", sub_matches)) => schedule_resolve(sub_matches).await,
        _ => {}
    }
}
