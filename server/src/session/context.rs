// Copyright 2023 Greptime Team
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
    fmt::{Display, Formatter},
    net::SocketAddr,
    sync::Arc,
};

use arc_swap::ArcSwap;
use catalog::consts::{DEFAULT_CATALOG, DEFAULT_SCHEMA};
use derive_builder::Builder;
use sqlparser::dialect::{Dialect, MySqlDialect, PostgreSqlDialect};

use crate::session::{build_db_string, parse_catalog_and_schema_from_db_string};

pub type QueryContextRef = Arc<QueryContext>;
pub type ConnInfoRef = Arc<ConnInfo>;

#[derive(Debug, Builder)]
#[builder(pattern = "owned")]
#[builder(build_fn(skip))]
pub struct QueryContext {
    current_catalog: String,
    current_schema: String,
    sql_dialect: Box<dyn Dialect + Send + Sync>,
}

impl Display for QueryContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "QueryContext{{catalog: {}, schema: {}}}",
            self.current_catalog(),
            self.current_schema()
        )
    }
}

impl QueryContext {
    pub fn arc() -> QueryContextRef {
        QueryContextBuilder::default().build()
    }

    pub fn with(catalog: &str, schema: &str) -> QueryContextRef {
        QueryContextBuilder::default()
            .current_catalog(catalog.to_string())
            .current_schema(schema.to_string())
            .build()
    }

    pub fn with_db_name(db_name: Option<&String>) -> QueryContextRef {
        let (catalog, schema) = db_name
            .map(|db| {
                let (catalog, schema) = parse_catalog_and_schema_from_db_string(db);
                (catalog.to_string(), schema.to_string())
            })
            .unwrap_or_else(|| (DEFAULT_CATALOG.to_string(), DEFAULT_SCHEMA.to_string()));
        QueryContextBuilder::default()
            .current_catalog(catalog)
            .current_schema(schema)
            .build()
    }

    #[inline]
    pub fn current_schema(&self) -> &str {
        &self.current_schema
    }

    #[inline]
    pub fn current_catalog(&self) -> &str {
        &self.current_catalog
    }

    #[inline]
    pub fn sql_dialect(&self) -> &(dyn Dialect + Send + Sync) {
        &*self.sql_dialect
    }

    pub fn get_db_string(&self) -> String {
        let catalog = self.current_catalog();
        let schema = self.current_schema();
        build_db_string(catalog, schema)
    }
}

impl QueryContextBuilder {
    pub fn build(self) -> QueryContextRef {
        Arc::new(QueryContext {
            current_catalog: self
                .current_catalog
                .unwrap_or_else(|| DEFAULT_CATALOG.to_string()),
            current_schema: self
                .current_schema
                .unwrap_or_else(|| DEFAULT_SCHEMA.to_string()),
            sql_dialect: self
                .sql_dialect
                .unwrap_or_else(|| Box::new(MySqlDialect {})),
        })
    }
}

#[derive(Debug)]
pub struct ConnInfo {
    pub client_addr: Option<SocketAddr>,
    pub channel: Channel,
}

impl Display for ConnInfo {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}[{}]",
            self.channel,
            self.client_addr
                .map(|addr| addr.to_string())
                .as_deref()
                .unwrap_or("unknown client addr")
        )
    }
}

impl ConnInfo {
    pub fn new(client_addr: Option<SocketAddr>, channel: Channel) -> Self {
        Self {
            client_addr,
            channel,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Channel {
    Mysql,
    Postgres,
}

impl Channel {
    pub fn dialect(&self) -> Box<dyn Dialect + Send + Sync> {
        match self {
            Channel::Mysql => Box::new(MySqlDialect {}),
            Channel::Postgres => Box::new(PostgreSqlDialect {}),
        }
    }
}

impl Display for Channel {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Channel::Mysql => write!(f, "mysql"),
            Channel::Postgres => write!(f, "postgres"),
        }
    }
}

#[cfg(test)]
mod test {
    use arrow::util::pretty;
    use interpreters::RecordBatchVec;
    use super::*;
    use crate::session::Session;



    #[test]
    fn test_session() {
        let session = Session::new(Some("127.0.0.1:9000".parse().unwrap()), Channel::Mysql);

        // test channel
        assert_eq!(session.conn_info().channel, Channel::Mysql);
        let client_addr = session.conn_info().client_addr.as_ref().unwrap();
        assert_eq!(client_addr.ip().to_string(), "127.0.0.1");
        assert_eq!(client_addr.port(), 9000);

        assert_eq!("mysql[127.0.0.1:9000]", session.conn_info().to_string());
    }

    #[test]
    fn test_context_db_string() {
        let context = QueryContext::with("a0b1c2d3", "test");
        assert_eq!("a0b1c2d3-test", context.get_db_string());

        let context = QueryContext::with(DEFAULT_CATALOG, "test");
        assert_eq!("test", context.get_db_string());
    }
}
