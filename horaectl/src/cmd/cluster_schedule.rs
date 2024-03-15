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

use anyhow::Result;
use clap::Subcommand;

use crate::operation::cluster::{clusters_schedule_get, clusters_schedule_set};

#[derive(Subcommand)]
pub enum ScheduleCommands {
    /// Get the schedule status
    Get,

    /// Set the schedule status
    Set {
        #[clap(long, short, default_value = "false", value_parser = clap::value_parser!(bool))]
        enable: bool,
    },
}

pub async fn run(command: ScheduleCommands) -> Result<()> {
    match command {
        ScheduleCommands::Get => clusters_schedule_get().await,
        ScheduleCommands::Set { enable } => clusters_schedule_set(enable).await,
    }
}
