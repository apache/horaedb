// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The main entry point to start the server

use std::env;

use ceresdb::{
    config::{ClusterDeployment, Config},
    setup,
};
use clap::{App, Arg};
use logger::info;

/// By this environment variable, the address of current node can be overridden.
/// And it could be domain name or ip address, but no port follows it.
const NODE_ADDR: &str = "CERESDB_SERVER_ADDR";
/// By this environment variable, the cluster name of current node can be
/// overridden.
const CLUSTER_NAME: &str = "CLUSTER_NAME";

/// Default value for version information is not found from environment
const UNKNOWN: &str = "Unknown";

fn fetch_version() -> String {
    let version = option_env!("CARGO_PKG_VERSION").unwrap_or(UNKNOWN);
    let git_branch = option_env!("VERGEN_GIT_BRANCH").unwrap_or(UNKNOWN);
    let git_commit_id = option_env!("VERGEN_GIT_SHA").unwrap_or(UNKNOWN);
    let build_time = option_env!("VERGEN_BUILD_TIMESTAMP").unwrap_or(UNKNOWN);
    let rustc_version = option_env!("VERGEN_RUSTC_SEMVER").unwrap_or(UNKNOWN);
    let opt_level = option_env!("VERGEN_CARGO_OPT_LEVEL").unwrap_or(UNKNOWN);
    let target = option_env!("VERGEN_CARGO_TARGET_TRIPLE").unwrap_or(UNKNOWN);

    [
        ("\nVersion", version),
        ("Git commit", git_commit_id),
        ("Git branch", git_branch),
        ("Opt level", opt_level),
        ("Rustc version", rustc_version),
        ("Target", target),
        ("Build date", build_time),
    ]
    .iter()
    .map(|(label, value)| format!("{label}: {value}"))
    .collect::<Vec<_>>()
    .join("\n")
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
            toml_ext::parse_toml_from_path(path, &mut toml_buf).expect("Failed to parse config.")
        }
        None => Config::default(),
    };

    if let Ok(node_addr) = env::var(NODE_ADDR) {
        config.node.addr = node_addr;
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

    panic_ext::set_panic_hook(false);

    // Log version.
    info!("version:{}", version);

    setup::run_server(config, runtime_level);
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_env_reader() {
        assert!(option_env!("CARGO_PKG_VERSION").is_some());
        assert!(option_env!("VERGEN_GIT_SHA").is_some());
        assert!(option_env!("VERGEN_BUILD_TIMESTAMP").is_some());
        assert!(option_env!("VERGEN_RUSTC_SEMVER").is_some());
        assert!(option_env!("VERGEN_CARGO_OPT_LEVEL").is_some());
        assert!(option_env!("VERGEN_CARGO_TARGET_TRIPLE").is_some());
    }
}
