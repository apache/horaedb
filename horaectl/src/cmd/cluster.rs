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

use crate::operation::cluster::ClusterOp;

#[derive(Subcommand)]
pub enum ClusterCommand {
    /// List cluster
    List,

    /// Diagnose cluster
    Diagnose,

    /// Schedule cluster
    Schedule {
        #[clap(subcommand)]
        cmd: Option<ScheduleCommand>,
    },
}

#[derive(Subcommand)]
pub enum ScheduleCommand {
    /// Get the schedule status
    Get,

    /// Enable schedule
    On,

    /// Disable schedule
    Off,
}

pub async fn run(cmd: ClusterCommand) -> Result<()> {
    let op = ClusterOp::try_new()?;
    match cmd {
        ClusterCommand::List => op.list().await,
        ClusterCommand::Diagnose => op.diagnose().await,
        ClusterCommand::Schedule { cmd } => {
            if let Some(cmd) = cmd {
                match cmd {
                    ScheduleCommand::Get => op.get_schedule_status().await,
                    ScheduleCommand::On => op.update_schedule_status(true).await,
                    ScheduleCommand::Off => op.update_schedule_status(false).await,
                }
            } else {
                op.get_schedule_status().await
            }
        }
    }
}
