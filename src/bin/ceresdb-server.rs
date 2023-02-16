// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! The main entry point to start the server

use std::{
    env,
    net::{IpAddr, SocketAddr},
};

use ceresdb::{
    config::{ClusterDeployment, Config},
    setup,
};
use clap::{App, Arg};
use common_util::{panic, toml};
use log::info;

/// The ip address of current node.
const NODE_ADDR: &str = "CSE_CERES_META_NODE_ADDR";
const CLUSTER_NAME: &str = "CLUSTER_NAME";

fn fetch_version() -> String {
    let build_version = option_env!("VERGEN_BUILD_SEMVER").unwrap_or("NONE");
    let git_branch = option_env!("VERGEN_GIT_BRANCH").unwrap_or("NONE");
    let git_commit_id = option_env!("VERGEN_GIT_SHA_SHORT").unwrap_or("NONE");
    let build_time = option_env!("VERGEN_BUILD_TIMESTAMP").unwrap_or("NONE");

    format!(
        "\nCeresDB Version: {build_version}\nGit branch: {git_branch}\nGit commit: {git_commit_id}\nBuild: {build_time}"
    )
}

// Parse the raw addr and panic if it is invalid.
fn parse_node_addr_or_fail(raw_addr: &str) -> IpAddr {
    let socket_addr: SocketAddr = raw_addr.parse().expect("invalid node addr");
    socket_addr.ip()
}

fn main() {
    let version = fetch_version();
    let matches = App::new("CeresDB Server")
        .version(version.as_str())
        .arg(
            Arg::with_name("config")
                .short('c')
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
        let ip = parse_node_addr_or_fail(&node_addr);
        config.node.addr = ip.to_string();
    }
    if let Ok(cluster) = env::var(CLUSTER_NAME) {
        if let Some(ClusterDeployment::WithMeta(v)) = &mut config.cluster_deployment {
            v.meta_client.cluster_name = cluster;
        }
    }

    println!("CeresDB server tries starting with config:{config:?}");

    // Setup log.
    let runtime_level = setup::setup_logger(&config);

    // Setup tracing.
    let _writer_guard = setup::setup_tracing(&config);

    panic::set_panic_hook(false);

    // Log version.
    info!("version:{}", version);

    setup::run_server(config, runtime_level);
}
