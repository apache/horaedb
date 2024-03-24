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

mod cmd;
mod operation;
mod util;

use clap::{CommandFactory, Parser};

use crate::{
    cmd::{repl_loop, run_command, App},
    util::{CLUSTER_NAME, META_ADDR},
};

#[tokio::main]
async fn main() {
    let app = App::parse();
    {
        let mut meta_addr = META_ADDR.lock().unwrap();
        *meta_addr = app.global_opts.meta_addr;
    }
    {
        let mut cluster_name = CLUSTER_NAME.lock().unwrap();
        *cluster_name = app.global_opts.cluster_name;
    }

    if app.interactive {
        repl_loop().await;
        return;
    }

    if let Some(cmd) = app.command {
        if let Err(e) = run_command(cmd).await {
            println!("Run command failed, err:{e}");
            std::process::exit(1);
        }
    } else {
        App::command().print_help().expect("print help failed");
    }
}
