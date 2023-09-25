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

use std::{
    collections::HashMap, env, fmt::Display, fs::File, process::Child, sync::Arc, time::Duration,
};

use async_trait::async_trait;
use ceresdb_client::{
    db_client::{Builder, DbClient, Mode},
    model::sql_query::{display::CsvFormatter, Request},
    RpcContext,
};
use reqwest::{ClientBuilder, StatusCode, Url};
use sqlness::{Database, QueryContext};

const SERVER_GRPC_ENDPOINT_ENV: &str = "CERESDB_SERVER_GRPC_ENDPOINT";
const SERVER_HTTP_ENDPOINT_ENV: &str = "CERESDB_SERVER_HTTP_ENDPOINT";
const CERESDB_BINARY_PATH_ENV: &str = "CERESDB_BINARY_PATH";
const CERESDB_STDOUT_FILE_ENV: &str = "CERESDB_STDOUT_FILE";
const CERESDB_CONFIG_FILE_ENV: &str = "CERESDB_CONFIG_FILE";

const CERESMETA_BINARY_PATH_ENV: &str = "CERESMETA_BINARY_PATH";
const CERESMETA_CONFIG_ENV: &str = "CERESMETA_CONFIG_PATH";
const CERESMETA_STDOUT_FILE_ENV: &str = "CERESMETA_STDOUT_FILE";
const CERESDB_CONFIG_FILE_0_ENV: &str = "CERESDB_CONFIG_FILE_0";
const CERESDB_CONFIG_FILE_1_ENV: &str = "CERESDB_CONFIG_FILE_1";
const CLUSTER_CERESDB_STDOUT_FILE_0_ENV: &str = "CLUSTER_CERESDB_STDOUT_FILE_0";
const CLUSTER_CERESDB_STDOUT_FILE_1_ENV: &str = "CLUSTER_CERESDB_STDOUT_FILE_1";
const CLUSTER_CERESDB_HEALTH_CHECK_INTERVAL_SECONDS: usize = 5;

const CERESDB_SERVER_ADDR: &str = "CERESDB_SERVER_ADDR";

// Used to access CeresDB by http service.
#[derive(Clone)]
struct HttpClient {
    client: reqwest::Client,
    endpoint: String,
}

impl HttpClient {
    fn new(endpoint: String) -> Self {
        let client = ClientBuilder::new()
            .build()
            .expect("should succeed to build http client");
        Self { client, endpoint }
    }
}

#[async_trait]
pub trait Backend {
    fn start() -> Self;
    async fn wait_for_ready(&self);
    fn stop(&mut self);
}

pub struct CeresDBServer {
    server_process: Child,
}

pub struct CeresDBCluster {
    server0: CeresDBServer,
    server1: CeresDBServer,
    ceresmeta_process: Child,

    /// Used in meta health check
    db_client: Arc<dyn DbClient>,
    meta_stable_check_sql: String,
}

impl CeresDBServer {
    fn spawn(bin: String, config: String, stdout: String) -> Self {
        let local_ip = local_ip_address::local_ip()
            .expect("fail to get local ip")
            .to_string();
        println!("Start server at {bin} with config {config} and stdout {stdout}, with local ip:{local_ip}");

        let stdout = File::create(stdout).expect("Failed to create stdout file");
        let server_process = std::process::Command::new(&bin)
            .env(CERESDB_SERVER_ADDR, local_ip)
            .args(["--config", &config])
            .stdout(stdout)
            .spawn()
            .unwrap_or_else(|_| panic!("Failed to start server at {bin:?}"));
        Self { server_process }
    }
}

#[async_trait]
impl Backend for CeresDBServer {
    fn start() -> Self {
        let config = env::var(CERESDB_CONFIG_FILE_ENV).expect("Cannot parse ceresdb config env");
        let bin = env::var(CERESDB_BINARY_PATH_ENV).expect("Cannot parse binary path env");
        let stdout = env::var(CERESDB_STDOUT_FILE_ENV).expect("Cannot parse stdout env");
        Self::spawn(bin, config, stdout)
    }

    async fn wait_for_ready(&self) {
        tokio::time::sleep(Duration::from_secs(10)).await
    }

    fn stop(&mut self) {
        self.server_process.kill().expect("Failed to kill server");
    }
}

impl CeresDBCluster {
    async fn check_meta_stable(&self) -> bool {
        let query_ctx = RpcContext {
            database: Some("public".to_string()),
            timeout: None,
        };

        let query_req = Request {
            tables: vec![],
            sql: self.meta_stable_check_sql.clone(),
        };

        let result = self.db_client.sql_query(&query_ctx, &query_req).await;
        result.is_ok()
    }
}

#[async_trait]
impl Backend for CeresDBCluster {
    fn start() -> Self {
        let ceresmeta_bin =
            env::var(CERESMETA_BINARY_PATH_ENV).expect("Cannot parse ceresdb binary path env");
        let ceresmeta_config =
            env::var(CERESMETA_CONFIG_ENV).expect("Cannot parse ceresmeta config path env");
        let ceresmeta_stdout =
            env::var(CERESMETA_STDOUT_FILE_ENV).expect("Cannot parse ceresmeta stdout env");
        println!("Start ceresmeta at {ceresmeta_bin} with config {ceresmeta_config} and stdout {ceresmeta_stdout}");

        let ceresmeta_stdout =
            File::create(ceresmeta_stdout).expect("Cannot create ceresmeta stdout");
        let ceresmeta_process = std::process::Command::new(&ceresmeta_bin)
            .args(["--config", &ceresmeta_config])
            .stdout(ceresmeta_stdout)
            .spawn()
            .expect("Failed to spawn process to start server");

        println!("wait for ceresmeta ready...\n");
        std::thread::sleep(Duration::from_secs(10));

        let ceresdb_bin =
            env::var(CERESDB_BINARY_PATH_ENV).expect("Cannot parse ceresdb binary path env");
        let ceresdb_config_0 =
            env::var(CERESDB_CONFIG_FILE_0_ENV).expect("Cannot parse ceresdb0 config env");
        let ceresdb_config_1 =
            env::var(CERESDB_CONFIG_FILE_1_ENV).expect("Cannot parse ceresdb1 config env");
        let stdout0 =
            env::var(CLUSTER_CERESDB_STDOUT_FILE_0_ENV).expect("Cannot parse ceresdb0 stdout env");
        let stdout1 =
            env::var(CLUSTER_CERESDB_STDOUT_FILE_1_ENV).expect("Cannot parse ceresdb1 stdout env");

        let server0 = CeresDBServer::spawn(ceresdb_bin.clone(), ceresdb_config_0, stdout0);
        let server1 = CeresDBServer::spawn(ceresdb_bin, ceresdb_config_1, stdout1);

        // Meta stable check context
        let endpoint = env::var(SERVER_GRPC_ENDPOINT_ENV).unwrap_or_else(|_| {
            panic!("Cannot read server endpoint from env {SERVER_GRPC_ENDPOINT_ENV:?}")
        });
        let db_client = Builder::new(endpoint, Mode::Proxy).build();

        let meta_stable_check_sql = format!(
            r#"CREATE TABLE `stable_check_{}`
            (`name` string TAG, `value` double NOT NULL, `t` timestamp NOT NULL, TIMESTAMP KEY(t))"#,
            uuid::Uuid::new_v4()
        );

        Self {
            server0,
            server1,
            ceresmeta_process,
            db_client,
            meta_stable_check_sql,
        }
    }

    async fn wait_for_ready(&self) {
        println!("wait for cluster service initialized...");
        tokio::time::sleep(Duration::from_secs(20_u64)).await;

        println!("wait for cluster service stable begin...");
        let mut wait_cnt = 0;
        let wait_max = 6;
        loop {
            if wait_cnt >= wait_max {
                println!(
                    "wait too long for cluster service stable, maybe somethings went wrong..."
                );
                return;
            }

            if self.check_meta_stable().await {
                println!("wait for cluster service stable finished...");
                return;
            }

            wait_cnt += 1;
            let has_waited = wait_cnt * CLUSTER_CERESDB_HEALTH_CHECK_INTERVAL_SECONDS;
            println!("waiting for cluster service stable, has_waited:{has_waited}s");
            tokio::time::sleep(Duration::from_secs(
                CLUSTER_CERESDB_HEALTH_CHECK_INTERVAL_SECONDS as u64,
            ))
            .await;
        }
    }

    fn stop(&mut self) {
        self.server0.stop();
        self.server1.stop();
        self.ceresmeta_process
            .kill()
            .expect("Failed to kill ceresmeta");
    }
}

pub struct CeresDB<T> {
    backend: T,
    db_client: Arc<dyn DbClient>,
    // FIXME: Currently, the new protocol does not support by the dbclient but is exposed by http
    // service. And remove this client when the new protocol is supported by the dbclient.
    http_client: HttpClient,
}

#[derive(Debug, Clone, Copy)]
enum Protocol {
    Sql,
    InfluxQL,
}

impl TryFrom<&str> for Protocol {
    type Error = String;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let protocol = match s {
            "influxql" => Protocol::InfluxQL,
            "sql" => Protocol::Sql,
            _ => return Err(format!("unknown protocol:{s}")),
        };

        Ok(protocol)
    }
}

#[derive(Debug, Clone, Copy)]
enum Command {
    Flush,
}

impl TryFrom<&str> for Command {
    type Error = String;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let cmd = match s {
            "flush" => Self::Flush,
            _ => return Err(format!("Unknown command:{s}")),
        };

        Ok(cmd)
    }
}

struct ProtocolParser;

impl ProtocolParser {
    fn parse_from_ctx(&self, ctx: &HashMap<String, String>) -> Result<Protocol, String> {
        ctx.get("protocol")
            .map(|s| Protocol::try_from(s.as_str()))
            .unwrap_or(Ok(Protocol::Sql))
    }
}

#[async_trait]
impl<T: Send + Sync> Database for CeresDB<T> {
    async fn query(&self, context: QueryContext, query: String) -> Box<dyn Display> {
        let protocol = ProtocolParser
            .parse_from_ctx(&context.context)
            .expect("parse protocol");

        if let Some(pre_cmd) = Self::parse_pre_cmd(&context.context) {
            let cmd = pre_cmd.expect("parse command");
            match cmd {
                Command::Flush => {
                    println!("Flush memtable...");
                    if let Err(e) = self.execute_flush().await {
                        panic!("Execute flush command failed, err:{e}");
                    }
                }
            }
        }

        match protocol {
            Protocol::Sql => Self::execute_sql(query, self.db_client.clone()).await,
            Protocol::InfluxQL => {
                let http_client = self.http_client.clone();
                Self::execute_influxql(query, http_client, context.context).await
            }
        }
    }
}

impl<T: Backend> CeresDB<T> {
    pub async fn create() -> CeresDB<T> {
        let backend = T::start();
        backend.wait_for_ready().await;

        let endpoint = env::var(SERVER_GRPC_ENDPOINT_ENV).unwrap_or_else(|_| {
            panic!("Cannot read server endpoint from env {SERVER_GRPC_ENDPOINT_ENV:?}")
        });
        let db_client = Builder::new(endpoint, Mode::Proxy).build();
        let http_endpoint = env::var(SERVER_HTTP_ENDPOINT_ENV).unwrap_or_else(|_| {
            panic!("Cannot read server endpoint from env {SERVER_HTTP_ENDPOINT_ENV:?}")
        });

        CeresDB {
            backend,
            db_client,
            http_client: HttpClient::new(http_endpoint),
        }
    }

    pub fn stop(&mut self) {
        self.backend.stop();
    }
}

impl<T> CeresDB<T> {
    fn parse_pre_cmd(ctx: &HashMap<String, String>) -> Option<Result<Command, String>> {
        ctx.get("pre_cmd").map(|s| Command::try_from(s.as_str()))
    }

    async fn execute_flush(&self) -> Result<(), String> {
        let url = format!("http://{}/debug/flush_memtable", self.http_client.endpoint);
        let resp = self.http_client.client.post(url).send().await.unwrap();

        if resp.status() == StatusCode::OK {
            return Ok(());
        }

        Err(resp.text().await.unwrap_or_else(|e| format!("{e:?}")))
    }

    async fn execute_influxql(
        query: String,
        http_client: HttpClient,
        params: HashMap<String, String>,
    ) -> Box<dyn Display> {
        let url = format!("http://{}/influxdb/v1/query", http_client.endpoint);
        let resp = match params.get("method") {
            Some(v) if v == "get" => {
                let url = Url::parse_with_params(&url, &[("q", query)]).unwrap();
                http_client.client.get(url).send().await.unwrap()
            }
            _ => http_client
                .client
                .post(url)
                .form(&[("q", query)])
                .send()
                .await
                .unwrap(),
        };
        let query_res = match resp.text().await {
            Ok(text) => text,
            Err(e) => format!("Failed to do influxql query, err:{e:?}"),
        };
        Box::new(query_res)
    }

    async fn execute_sql(query: String, client: Arc<dyn DbClient>) -> Box<dyn Display> {
        let query_ctx = RpcContext {
            database: Some("public".to_string()),
            timeout: None,
        };

        let query_req = Request {
            tables: vec![],
            sql: query,
        };

        let result = client.sql_query(&query_ctx, &query_req).await;

        Box::new(match result {
            Ok(resp) => {
                if resp.rows.is_empty() {
                    format!("affected_rows: {}", resp.affected_rows)
                } else {
                    format!("{}", CsvFormatter { resp })
                }
            }
            Err(e) => format!("Failed to execute query, err: {e:?}"),
        })
    }
}
