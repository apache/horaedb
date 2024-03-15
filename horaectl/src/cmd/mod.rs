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

mod cluster;
mod cluster_schedule;
use std::{io, io::Write};

use anyhow::Result;
use clap::{Args, Parser, Subcommand};

use crate::{
    cmd::cluster::ClusterCommands,
    util::{CLUSTER_NAME, META_ADDR},
};

#[derive(Parser)]
#[clap(name = "horaectl")]
#[clap(about = "HoraeCTL is a command line tool for HoraeDB", long_about = None)]
pub struct App {
    #[clap(flatten)]
    pub global_opts: GlobalOpts,

    /// Enter interactive mode
    #[clap(short, long, default_value_t = false)]
    pub interactive: bool,

    #[clap(subcommand)]
    pub command: Option<SubCommand>,
}

#[derive(Debug, Args)]
pub struct GlobalOpts {
    /// Meta addr
    #[clap(
        short,
        long = "meta",
        global = true,
        env = "HORAECTL_META_ADDR",
        default_value = "127.0.0.1:8080"
    )]
    pub meta_addr: String,

    /// Cluster name
    #[clap(
        short,
        long = "cluster",
        global = true,
        env = "HORAECTL_CLUSTER",
        default_value = "defaultCluster"
    )]
    pub cluster_name: String,
}

#[derive(Subcommand)]
pub enum SubCommand {
    /// Operations on cluster
    #[clap(alias = "c")]
    Cluster {
        #[clap(subcommand)]
        commands: ClusterCommands,
    },
}

pub async fn run_command(cmd: SubCommand) -> Result<()> {
    match cmd {
        SubCommand::Cluster { commands } => cluster::run(commands).await,
    }
}

pub async fn repl_loop() {
    loop {
        print_prompt(
            META_ADDR.lock().unwrap().as_str(),
            CLUSTER_NAME.lock().unwrap().as_str(),
        );

        let args = match read_args() {
            Ok(args) => args,
            Err(e) => {
                println!("Read input failed, err:{}", e);
                continue;
            }
        };

        if let Some(cmd) = args.get(1) {
            if ["quit", "exit", "q"].iter().any(|v| v == cmd) {
                break;
            }
        }

        match App::try_parse_from(args) {
            Ok(horaectl) => {
                if let Some(cmd) = horaectl.command {
                    if let Err(e) = match cmd {
                        SubCommand::Cluster { commands } => cluster::run(commands).await,
                    } {
                        println!("Run command failed, err:{e}");
                    }
                }
            }
            Err(e) => {
                println!("Parse command failed, err:{e}");
            }
        }
    }
}

fn read_args() -> Result<Vec<String>, String> {
    io::stdout().flush().unwrap();
    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .map_err(|e| e.to_string())?;

    let input = input.trim();
    if input.is_empty() {
        return Err("No arguments provided".into());
    }

    let mut args = vec!["horaectl".to_string()];
    args.extend(shell_words::split(input).map_err(|e| e.to_string())?);
    Ok(args)
}

fn print_prompt(address: &str, cluster: &str) {
    print!("{}({}) > ", address, cluster);
}
