// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    env,
    process::{Child, Command, Stdio},
};

use ceresdb_client_rs::{client::Client, Builder};

const BINARY_PATH_ENV: &str = "CERESDB_BINARY_PATH";
const CONFIG_PATH_ENV: &str = "CERESDB_CONFIG_PATH";
const SERVER_ENDPOINT_ENV: &str = "CERESDB_SERVER_ENDPOINT";
const CASE_ROOT_PATH_ENV: &str = "CERESDB_TEST_CASE_PATH";

pub struct Environment {
    server_process: Child,
}

impl Environment {
    pub fn start_server() -> Self {
        let bin = env::var(BINARY_PATH_ENV).expect("Cannot parse binary path env");
        let config = env::var(CONFIG_PATH_ENV).expect("Cannot parse config path env");

        // TODO: support config stdout/stderr
        let server_process = Command::new(&bin)
            .args(["--config", &config])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|_| panic!("Failed to start server at {:?}", bin));
        println!("Server from {:?} is starting ...", bin);

        // Wait for a while
        std::thread::sleep(std::time::Duration::from_secs(5));

        Self { server_process }
    }

    pub fn build_client(&self) -> Client {
        let endpoint = env::var(SERVER_ENDPOINT_ENV).unwrap_or_else(|_| {
            panic!(
                "Cannot read server endpoint from env {:?}",
                SERVER_ENDPOINT_ENV
            )
        });

        Builder::new(endpoint).build()
    }

    pub fn get_case_path(&self) -> String {
        env::var(CASE_ROOT_PATH_ENV).unwrap_or_else(|_| {
            panic!(
                "Cannot read path of test cases from env {:?}",
                CASE_ROOT_PATH_ENV
            )
        })
    }

    pub fn stop_server(mut self) {
        self.server_process.kill().unwrap();
    }
}
