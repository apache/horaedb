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
mod cluster_diagnose;
mod cluster_list;
mod cluster_schedule;
pub mod quit;

use std::{io, io::Write};

use clap::{Arg, Command};

use crate::{
    cmd::{
        cluster::{cluster, cluster_resolve},
        quit::quit,
    },
    util::{CLUSTER_NAME, META_ADDR},
};

fn cmd() -> Command {
    let horaectl = Command::new("horaectl")
        .about("horaectl is a command line tool for HoraeDB")
        .arg(
            Arg::new("meta_addr")
                .short('m')
                .long("meta")
                .value_name("META_ADDR")
                .default_value("127.0.0.1:8080")
                .help("meta addr is used to connect to meta server"),
        )
        .arg(
            Arg::new("cluster_name")
                .short('c')
                .long("cluster")
                .value_name("CLUSTER_NAME")
                .default_value("defaultCluster")
                .help("Cluster to connect to"),
        );

    let matches = horaectl.clone().get_matches();
    META_ADDR
        .set(matches.get_one::<String>("meta_addr").unwrap().to_string())
        .unwrap();
    CLUSTER_NAME
        .set(
            matches
                .get_one::<String>("cluster_name")
                .unwrap()
                .to_string(),
        )
        .unwrap();

    horaectl.subcommand(cluster()).subcommand(quit())
}

pub async fn execute() {
    let horaectl = cmd();
    loop {
        print_prompt(META_ADDR.get().unwrap(), CLUSTER_NAME.get().unwrap());

        let args = match read_args() {
            Ok(args) => args,
            Err(e) => {
                println!("{}", e);
                continue;
            }
        };

        match horaectl.clone().try_get_matches_from(args) {
            Ok(arg_matches) => match arg_matches.subcommand() {
                Some(("quit", _)) => {
                    println!("bye");
                    break;
                }
                Some(("cluster", sub_matches)) => cluster_resolve(sub_matches).await,
                _ => {}
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
