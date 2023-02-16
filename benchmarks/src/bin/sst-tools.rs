// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use benchmarks::{
    sst_tools::{self, MergeSstConfig, RebuildSstConfig},
    util,
};
use clap::{App, Arg};
use common_util::toml;
use log::info;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(default)]
struct Config {
    runtime_thread_num: usize,
    rebuild_sst: Option<RebuildSstConfig>,
    merge_sst: Option<MergeSstConfig>,
}

impl Default for Config {
    fn default() -> Config {
        Self {
            runtime_thread_num: 1,
            rebuild_sst: None,
            merge_sst: None,
        }
    }
}

fn config_from_path(path: &str) -> Config {
    let mut toml_buf = String::new();
    toml::parse_toml_from_path(path, &mut toml_buf).expect("Failed to parse config.")
}

fn main() {
    env_logger::init();

    let matches = App::new("SST Tools")
        .arg(
            Arg::with_name("config")
                .short('c')
                .long("config")
                .required(true)
                .takes_value(true)
                .help("Set configuration file, eg: \"/path/server.toml\""),
        )
        .get_matches();

    let config_path = matches
        .value_of("config")
        .expect("Config file is required.");
    let config = config_from_path(config_path);

    info!("sst tools start, config:{:?}", config);

    let runtime = Arc::new(util::new_runtime(config.runtime_thread_num));

    let rt = runtime.clone();
    runtime.block_on(async {
        if let Some(rebuild_sst) = config.rebuild_sst {
            sst_tools::rebuild_sst(rebuild_sst, rt.clone()).await;
        }

        if let Some(merge_sst) = config.merge_sst {
            sst_tools::merge_sst(merge_sst, rt).await;
        }
    });
}
