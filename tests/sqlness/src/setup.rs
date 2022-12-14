// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    env,
    fs::File,
    process::{Child, Command},
};

use async_trait::async_trait;
use ceresdb_client_rs::db_client::{Builder, Mode};
use sqlness::Environment as SqlnessEnv;

use crate::client::Client;

const BINARY_PATH_ENV: &str = "CERESDB_BINARY_PATH";
const CONFIG_PATH_ENV: &str = "CERESDB_CONFIG_PATH";
const SERVER_ENDPOINT_ENV: &str = "CERESDB_SERVER_ENDPOINT";
const CERESDB_STDOUT_FILE: &str = "CERESDB_STDOUT_FILE";
const CERESDB_STDERR_FILE: &str = "CERESDB_STDERR_FILE";

pub struct CeresDBEnv {
    _server_process: Child,
}

#[async_trait]
impl SqlnessEnv for CeresDBEnv {
    type DB = Client;

    async fn start(&self, _mode: &str, _config: Option<String>) -> Self::DB {
        let endpoint = env::var(SERVER_ENDPOINT_ENV).unwrap_or_else(|_| {
            panic!(
                "Cannot read server endpoint from env {:?}",
                SERVER_ENDPOINT_ENV
            )
        });

        let client = Builder::new(endpoint, Mode::Standalone).build();
        Client::new(client)
    }

    /// Stop one [`Database`].
    async fn stop(&self, _mode: &str, _database: Self::DB) {
        // self._server_process.kill().unwrap()
    }
}

impl CeresDBEnv {
    pub fn start_server() -> Self {
        let bin = env::var(BINARY_PATH_ENV).expect("Cannot parse binary path env");
        let config = env::var(CONFIG_PATH_ENV).expect("Cannot parse config path env");
        let stdout = env::var(CERESDB_STDOUT_FILE).expect("Cannot parse stdout env");
        let stderr = env::var(CERESDB_STDERR_FILE).expect("Cannot parse stderr env");

        let stdout = File::create(stdout).expect("Cannot create stdout");
        let stderr = File::create(stderr).expect("Cannot create stderr");
        let server_process = Command::new(&bin)
            .args(["--config", &config])
            .stdout(stdout)
            .stderr(stderr)
            .spawn()
            .unwrap_or_else(|_| panic!("Failed to start server at {:?}", bin));

        // Wait for a while
        std::thread::sleep(std::time::Duration::from_secs(5));

        Self {
            _server_process: server_process,
        }
    }
}
