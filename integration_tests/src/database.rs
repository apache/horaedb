// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{env, fmt::Display, fs, fs::File, path::Path, process::{Child, Command}, sync::Arc};
use std::borrow::Cow;

use async_trait::async_trait;
use ceresdb_client_rs::{
    db_client::{Builder, DbClient, Mode},
    model::sql_query::{display::CsvFormatter, Request},
    RpcContext,
};
use sql::{
    ast::{Statement, TableName},
    parser::Parser,
};
use sqlness::Database;
use sqlparser::ast::{SetExpr, Statement as SqlStatement, TableFactor};

const BINARY_PATH_ENV: &str = "CERESDB_BINARY_PATH";
const SERVER_ENDPOINT_ENV: &str = "CERESDB_SERVER_ENDPOINT";
const CLUSTER_SERVER_ENDPOINT_ENV: &str = "CERESDB_CLUSTER_SERVER_ENDPOINT";
const CERESDB_STDOUT_FILE: &str = "CERESDB_STDOUT_FILE";
const CERESDB_STDERR_FILE: &str = "CERESDB_STDERR_FILE";

#[derive(Debug, Clone, Copy)]
pub enum DeployMode {
    Standalone,
    Cluster,
}

pub struct CeresDB {
    server_process: Option<Child>,
    db_client: Arc<dyn DbClient>,
}

#[async_trait]
impl Database for CeresDB {
    async fn query(&self, query: String) -> Box<dyn Display> {
        Self::execute(query, self.db_client.clone()).await
    }
}

impl CeresDB {
    pub fn new(config: Option<&Path>, mode: DeployMode) -> Self {
        let config = config.unwrap().to_string_lossy();
        let bin = env::var(BINARY_PATH_ENV).expect("Cannot parse binary path env");
        let stdout_file = env::var(CERESDB_STDOUT_FILE).expect("Cannot parse stdout env");
        let stderr_file = env::var(CERESDB_STDERR_FILE).expect("Cannot parse stderr env");
        let stdout = File::create(stdout_file.clone()).expect("Cannot create stdout");
        let stderr = File::create(stderr_file.clone()).expect("Cannot create stderr");

        println!("Start {bin} with {config}...");

        match mode {
            DeployMode::Standalone => {
                let server_process = Self::start_standalone(stdout, stderr, bin, config);
                // Wait for a while
                std::thread::sleep(std::time::Duration::from_secs(5));
                let endpoint = env::var(SERVER_ENDPOINT_ENV).unwrap_or_else(|_| {
                    panic!("Cannot read server endpoint from env {SERVER_ENDPOINT_ENV:?}")
                });
                let db_client = Builder::new(endpoint, Mode::Proxy).build();
                CeresDB {
                    server_process: Some(server_process),
                    db_client,
                }
            }
            DeployMode::Cluster => {
                Self::start_cluster(stdout, stderr);
                // Wait for a while
                std::thread::sleep(std::time::Duration::from_secs(10));
                let stdout_content = fs::read_to_string(Path::new(&stdout_file))
                    .expect("Should have been able to read the file");
                let stderr_content = fs::read_to_string(Path::new(&stderr_file))
                    .expect("Should have been able to read the file");
                let endpoint = env::var(CLUSTER_SERVER_ENDPOINT_ENV).unwrap_or_else(|_| {
                    panic!("Cannot read server endpoint from env {SERVER_ENDPOINT_ENV:?}")
                });
                let db_client = Builder::new(endpoint, Mode::Proxy).build();
                CeresDB {
                    server_process: None,
                    db_client,
                }
            }
        }
    }

    pub fn stop(self, mode: DeployMode) {
        match mode {
            DeployMode::Standalone => self.stop_standalone(),
            DeployMode::Cluster => {
                let stdout = env::var(CERESDB_STDOUT_FILE).expect("Cannot parse stdout env");
                let stderr = env::var(CERESDB_STDERR_FILE).expect("Cannot parse stderr env");
                let stdout = File::open(stdout).expect("Cannot create stdout");
                let stderr = File::open(stderr).expect("Cannot create stderr");
                Self::stop_cluster(stdout, stderr)
            }
        }
    }

    async fn execute(query: String, client: Arc<dyn DbClient>) -> Box<dyn Display> {
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

    fn start_standalone(stdout: File, stderr: File, bin: String,config: Cow<str>) -> Child{
         let server_process = Command::new(&bin)
            .args(["--config", &config])
            .stdout(stdout)
            .stderr(stderr)
            .spawn()
            .unwrap_or_else(|_| panic!("Failed to start server at {bin:?}"));
        server_process
    }

    fn stop_standalone(self){
        self.server_process.unwrap().kill().unwrap()
    }

    fn start_cluster(stdout: File, stderr: File){
        Command::new("docker-compose")
            .args(["up", "-d"])
            .stdout(stdout)
            .stderr(stderr)
            .spawn()
            .unwrap_or_else(|_| panic!("Failed to start server"));
    }

    fn stop_cluster(stdout: File, stderr: File){
        Command::new("docker-compose")
            .args(["rm", "-fsv"])
            .stdout(stdout)
            .stderr(stderr)
            .spawn()
            .unwrap_or_else(|_| panic!("Failed to stop server"));
    }

    fn parse_table_name(query: &str) -> Option<String> {
        let statements = Parser::parse_sql(query).unwrap();

        match &statements[0] {
            Statement::Standard(s) => match *s.clone() {
                SqlStatement::Insert {
                    table_name,
                    ..
                } => Some(TableName::from(table_name).to_string()),
                SqlStatement::Explain {
                    statement,
                    ..
                } => {
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
