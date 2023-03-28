// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    env,
    fmt::Display,
    fs::File,
    process::{Child, Command},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use ceresdb_client::{
    db_client::{Builder, DbClient, Mode},
    model::sql_query::{display::CsvFormatter, Request},
    RpcContext,
};
use reqwest::ClientBuilder;
use sql::{
    ast::{Statement, TableName},
    parser::Parser,
};
use sqlness::{Database, QueryContext};
use sqlparser::ast::{SetExpr, Statement as SqlStatement, TableFactor};

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

pub trait Backend {
    fn start() -> Self;
    fn wait_for_ready(&self);
    fn stop(&mut self);
}

pub struct CeresDBServer {
    server_process: Child,
}

pub struct CeresDBCluster {
    server0: CeresDBServer,
    server1: CeresDBServer,
    ceresmeta_process: Child,
}

impl CeresDBServer {
    fn spawn(bin: String, config: String, stdout: String) -> Self {
        let local_ip = local_ip_address::local_ip()
            .expect("fail to get local ip")
            .to_string();
        println!("Start server at {bin} with config {config} and stdout {stdout}, with local ip:{local_ip}");

        let stdout = File::create(stdout).expect("Failed to create stdout file");
        let server_process = Command::new(&bin)
            .env(CERESDB_SERVER_ADDR, local_ip)
            .args(["--config", &config])
            .stdout(stdout)
            .spawn()
            .unwrap_or_else(|_| panic!("Failed to start server at {bin:?}"));
        Self { server_process }
    }
}

impl Backend for CeresDBServer {
    fn start() -> Self {
        let config = env::var(CERESDB_CONFIG_FILE_ENV).expect("Cannot parse ceresdb config env");
        let bin = env::var(CERESDB_BINARY_PATH_ENV).expect("Cannot parse binary path env");
        let stdout = env::var(CERESDB_STDOUT_FILE_ENV).expect("Cannot parse stdout env");
        Self::spawn(bin, config, stdout)
    }

    fn wait_for_ready(&self) {
        std::thread::sleep(Duration::from_secs(5));
    }

    fn stop(&mut self) {
        self.server_process.kill().expect("Failed to kill server");
    }
}

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
        let ceresmeta_process = Command::new(&ceresmeta_bin)
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

        Self {
            server0,
            server1,
            ceresmeta_process,
        }
    }

    fn wait_for_ready(&self) {
        println!("wait for cluster service ready...\n");
        std::thread::sleep(Duration::from_secs(20));
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

        match protocol {
            Protocol::Sql => Self::execute_sql(query, self.db_client.clone()).await,
            Protocol::InfluxQL => {
                let http_client = self.http_client.clone();
                Self::execute_influxql(query, http_client).await
            }
        }
    }
}

impl<T: Backend> CeresDB<T> {
    pub fn create() -> CeresDB<T> {
        let backend = T::start();
        backend.wait_for_ready();

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
    async fn execute_influxql(query: String, http_client: HttpClient) -> Box<dyn Display> {
        let query = format!("q={query}");
        let url = format!("http://{}/influxdb/v1/query", http_client.endpoint);
        let resp = http_client
            .client
            .post(url)
            .body(query)
            .send()
            .await
            .unwrap();
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

        let table_name = Self::parse_table_name(&query);

        let query_req = match table_name {
            Some(table_name) => Request {
                tables: vec![table_name],
                sql: query,
            },
            None => Request {
                tables: vec![],
                sql: query,
            },
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

    fn parse_table_name(query: &str) -> Option<String> {
        let statements = Parser::parse_sql(query).unwrap();

        match &statements[0] {
            Statement::Standard(s) => match *s.clone() {
                SqlStatement::Insert { table_name, .. } => {
                    Some(TableName::from(table_name).to_string())
                }
                SqlStatement::Explain { statement, .. } => {
                    if let SqlStatement::Query(q) = *statement {
                        match *q.body {
                            SetExpr::Select(select) => {
                                if select.from.len() != 1 {
                                    None
                                } else if let TableFactor::Table { name, .. } =
                                    &select.from[0].relation
                                {
                                    Some(TableName::from(name.clone()).to_string())
                                } else {
                                    None
                                }
                            }
                            _ => None,
                        }
                    } else {
                        None
                    }
                }
                SqlStatement::Query(q) => match *q.body {
                    SetExpr::Select(select) => {
                        if select.from.len() != 1 {
                            None
                        } else if let TableFactor::Table { name, .. } = &select.from[0].relation {
                            Some(TableName::from(name.clone()).to_string())
                        } else {
                            None
                        }
                    }
                    _ => None,
                },
                _ => None,
            },
            Statement::Create(s) => Some(s.table_name.to_string()),
            Statement::Drop(s) => Some(s.table_name.to_string()),
            Statement::Describe(s) => Some(s.table_name.to_string()),
            Statement::AlterModifySetting(s) => Some(s.table_name.to_string()),
            Statement::AlterAddColumn(s) => Some(s.table_name.to_string()),
            Statement::ShowCreate(s) => Some(s.table_name.to_string()),
            Statement::ShowTables(_s) => None,
            Statement::ShowDatabases => None,
            Statement::Exists(s) => Some(s.table_name.to_string()),
        }
    }
}
