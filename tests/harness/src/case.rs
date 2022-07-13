use std::{fmt::Display, path::Path};

use anyhow::{Context, Result};
use ceresdb_client_rs::{
    client::{Client, RpcContext},
    model::{display::CsvFormatter, request::QueryRequest, QueriedRows},
    DbClient,
};
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader},
};

const INTERCEPTOR_PREFIX: &'static str = "-- CERESDB";
const COMMENT_PREFIX: &'static str = "--";

pub struct TestCase {
    name: String,
    queries: Vec<Query>,
}

impl TestCase {
    pub async fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path.as_ref())
            .await
            .with_context(|| format!("Cannot open file {:?}", path.as_ref()))?;

        let mut queries = vec![];
        let mut query = Query::default();

        let mut lines = BufReader::new(file).lines();
        while let Some(line) = lines
            .next_line()
            .await
            .with_context(|| format!("Failed to read from file {:?}", path.as_ref()))?
        {
            // intercept command start with INTERCEPTOR_PREFIX
            if line.starts_with(INTERCEPTOR_PREFIX) {
                query.push_interceptor(line);
                continue;
            }

            // ignore comment and empty line
            if line.starts_with(COMMENT_PREFIX) || line.is_empty() {
                continue;
            }

            query.append_query_line(&line);

            // SQL statement ends with ';'
            if line.ends_with(';') {
                queries.push(query);
                query = Query::default();
            }
        }

        Ok(Self {
            name: path.as_ref().to_str().unwrap().to_string(),
            queries,
        })
    }

    pub async fn execute<W>(&self, client: &Client, writer: &mut W) -> Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        for query in &self.queries {
            query
                .execute(client, writer)
                .await
                .with_context(|| format!("Error while executing {:?}", query.query_lines))?;
        }

        Ok(())
    }
}

impl Display for TestCase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.name)
    }
}

#[derive(Default)]
struct Query {
    query_lines: Vec<String>,
    interceptors: Vec<String>,
}

impl Query {
    fn push_interceptor(&mut self, post_process: String) {
        self.interceptors.push(post_process);
    }

    fn append_query_line(&mut self, line: &str) {
        self.query_lines.push(line.to_string());
    }

    async fn execute<W>(&self, client: &Client, writer: &mut W) -> Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        let query_ctx = RpcContext {
            tenant: "public".to_string(),
            token: "".to_string(),
        };
        let query_req = QueryRequest {
            metrics: vec![],
            ql: self.build_query_from_lines(),
        };
        let result = client.query(&query_ctx, &query_req).await;
        match result {
            Ok(rows) => {
                self.write_result(writer, format!("{}", CsvFormatter { rows }))
                    .await?
            }
            Err(e) => {
                self.write_result(writer, format!("Failed to execute query, err: {:?}", e))
                    .await?
            }
        }

        Ok(())
    }

    // TODO: implement query interceptor.
    #[allow(dead_code)]
    fn intercept(&self, _result: QueriedRows) -> QueriedRows {
        todo!()
    }

    fn build_query_from_lines(&self) -> String {
        self.query_lines
            .iter()
            .fold(String::new(), |query, str| query + " " + str)
    }

    async fn write_result<W>(&self, writer: &mut W, result: String) -> Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        for interceptor in &self.interceptors {
            writer.write_all(interceptor.as_bytes()).await?;
        }
        for line in &self.query_lines {
            writer.write_all(line.as_bytes()).await?;
        }
        writer.write("\n\n".as_bytes()).await?;
        writer.write_all(result.as_bytes()).await?;
        writer.write("\n\n".as_bytes()).await?;

        Ok(())
    }
}
