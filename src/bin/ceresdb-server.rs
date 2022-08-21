// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! The main entry point to start the server

use std::{env, net::SocketAddr};

use ceresdb::setup;
use clap::{App, Arg};
use common_util::{panic, toml};
use log::info;
use server::config::Config;

/// The ip address of current node.
const NODE_ADDR: &str = "CSE_CERES_META_NODE_ADDR";
const CLUSTER_NAME: &str = "CLUSTER_NAME";

fn fetch_version() -> String {
    let build_version = option_env!("VERGEN_BUILD_SEMVER").unwrap_or("NONE");
    let git_branch = option_env!("VERGEN_GIT_BRANCH").unwrap_or("NONE");
    let git_commit_id = option_env!("VERGEN_GIT_SHA_SHORT").unwrap_or("NONE");
    let build_time = option_env!("VERGEN_BUILD_TIMESTAMP").unwrap_or("NONE");

    format!(
        "\nCeresDB Version: {}\nGit branch: {}\nGit commit: {}\nBuild: {}",
        build_version, git_branch, git_commit_id, build_time
    )
}

// Parse the raw addr and panic if it is invalid.
fn parse_node_addr_or_fail(raw_addr: &str) -> (String, u16) {
    let socket_addr: SocketAddr = raw_addr.parse().expect("invalid node addr");
    (socket_addr.ip().to_string(), socket_addr.port())
}

fn main() {
    let version = fetch_version();
    let matches = App::new("CeresDB Server")
        .version(version.as_str())
        .arg(
            Arg::with_name("config")
                .short("c")
                .long("config")
                .required(false)
                .takes_value(true)
                .help("Set configuration file, eg: \"/path/server.toml\""),
        )
        .get_matches();

    let mut config = match matches.value_of("config") {
        Some(path) => {
            let mut toml_buf = String::new();
            toml::parse_toml_from_path(path, &mut toml_buf).expect("Failed to parse config.")
        }
        None => Config::default(),
    };

    if let Ok(node_addr) = env::var(NODE_ADDR) {
        let (ip, port) = parse_node_addr_or_fail(&node_addr);
        config.cluster.node.addr = ip;
        config.cluster.node.port = port;
    }
    if let Ok(cluster) = env::var(CLUSTER_NAME) {
        config.cluster.meta_client.cluster_name = cluster;
    }

    // Setup log.
    let _runtime_level = setup::setup_log(&config);
    // Setup tracing.
    let _writer_guard = setup::setup_tracing(&config);

    panic::set_panic_hook(false);

    // Log version.
    info!("version:{}", version);

    setup::run_server(config);
}
