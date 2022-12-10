use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use ceresdb_client_rs::{
    db_client::DbClient,
    model::{display::CsvFormatter, request::QueryRequest},
    RpcContext,
};
use sqlness::Database;

pub struct Client {
    db_client: Arc<dyn DbClient>,
}

#[async_trait]
impl Database for Client {
    async fn query(&self, query: String) -> Box<dyn Display> {
        Self::execute(query, self.db_client.clone()).await
    }
}

impl Client {
    pub fn new(db_client: Arc<dyn DbClient>) -> Self {
        Client { db_client }
    }

    async fn execute(query: String, client: Arc<dyn DbClient>) -> Box<dyn Display> {
        let query_ctx = RpcContext {
            tenant: "public".to_string(),
            token: "".to_string(),
        };
        let query_req = QueryRequest {
            metrics: vec![],
            ql: query,
        };
        let result = client.query(&query_ctx, &query_req).await;

        Box::new(match result {
            Ok(resp) => {
                if resp.has_schema() {
                    format!("{}", CsvFormatter { resp })
                } else {
                    format!("affected_rows: {}", resp.affected_rows)
                }
            }
            Err(e) => format!("Failed to execute query, err: {:?}", e),
        })
    }
}
