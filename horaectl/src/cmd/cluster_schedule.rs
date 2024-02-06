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

use clap::{Arg, ArgMatches, Command};

use crate::operation::cluster::{clusters_schedule_get, clusters_schedule_set};

pub fn schedule() -> Command {
    Command::new("schedule")
        .about("Cluster schedule")
        .alias("s")
        .subcommand(Command::new("get").about("Get the schedule status"))
        .subcommand(
            Command::new("set").about("Set the schedule status").arg(
                Arg::new("enable")
                    .help("Enable or disable schedule")
                    .long("enable")
                    .short('e')
                    .default_value("false")
                    .value_parser(clap::value_parser!(bool)),
            ),
        )
}

pub async fn schedule_resolve(arg_matches: &ArgMatches) {
    match arg_matches.subcommand() {
        Some(("get", _)) => clusters_schedule_get().await,
        Some(("set", sub_matches)) => {
            let enable = sub_matches.get_one::<bool>("enable").copied().unwrap();
            clusters_schedule_set(enable).await;
        }
        _ => {}
    }
}
