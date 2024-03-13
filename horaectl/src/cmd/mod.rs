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

use clap::{Parser, Subcommand};

use crate::{
    cmd::cluster::{cluster_resolve, ClusterCommands},
    util::{CLUSTER_NAME, META_ADDR},
};

#[derive(Parser)]
#[clap(name = "horaectl")]
#[clap(about = "horaectl is a command line tool for HoraeDB", long_about = None)]
pub struct Horaectl {
    #[clap(
        short = 'm',
        long = "meta",
        default_value = "127.0.0.1:8080",
        help = "meta addr is used to connect to meta server"
    )]
    pub meta_addr: String,

    #[clap(
        short = 'c',
        long = "cluster",
        default_value = "defaultCluster",
        help = "Cluster to connect to"
    )]
    pub cluster_name: String,

    #[clap(subcommand)]
    pub commands: Option<Commands>,
}

#[derive(Subcommand)]
pub enum Commands {
    #[clap(about = "Quit horaectl", long_about = None)]
    #[clap(alias = "q")]
    #[clap(alias = "exit")]
    Quit,

    #[clap(about = "Operations on cluster", long_about = None)]
    #[clap(alias = "c")]
    Cluster {
        #[clap(short = 'c', long = "cluster", help = "Cluster to connect to")]
        cluster_name: Option<String>,

        #[clap(subcommand)]
        commands: Option<ClusterCommands>,
    },
}

pub async fn execute() {
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

        match Horaectl::try_parse_from(args) {
            Ok(horaectl) => match horaectl.commands {
                Some(Commands::Quit) => {
                    println!("bye");
                    break;
                }
                Some(Commands::Cluster {
                    cluster_name,
                    commands,
                }) => cluster_resolve(cluster_name, commands).await,
                None => {}
            },
            Err(e) => {
                println!("{}", e)
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
