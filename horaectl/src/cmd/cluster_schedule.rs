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

use crate::operation::cluster::{clusters_schedule_get, clusters_schedule_set};

#[derive(Subcommand)]
pub enum ScheduleCommands {
    #[clap(about = "Get the schedule status", long_about = None)]
    Get,

    #[clap(about = "Set the schedule status", long_about = None)]
    Set {
        #[clap(long = "enable", short = 'e', default_value = "false", value_parser = clap::value_parser!(bool))]
        enable: bool,
    },
}

pub async fn schedule_resolve(command: Option<ScheduleCommands>) {
    match command {
        Some(ScheduleCommands::Get) => clusters_schedule_get().await,
        Some(ScheduleCommands::Set { enable }) => {
            clusters_schedule_set(enable).await;
        }
        None => {}
    }
}
