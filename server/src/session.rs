// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Forked from https://github.com/GreptimeTeam/greptimedb/blob/ca4d690424b03806ea0f8bd5e491585224bbf220/src/session/src/lib.rs

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
use sqlparser::dialect::{Dialect, MySqlDialect, PostgreSqlDialect};

/// Session for persistent connection such as MySQL, PostgreSQL etc.
#[derive(Debug)]
pub struct Session {
    catalog: ArcSwap<String>,
    schema: ArcSwap<String>,
    conn_info: ConnInfo,
}

pub type SessionRef = Arc<Session>;

impl Session {
    pub fn new(addr: Option<SocketAddr>, channel: Channel) -> Self {
        Session {
            catalog: ArcSwap::new(Arc::new(DEFAULT_CATALOG.into())),
            schema: ArcSwap::new(Arc::new(DEFAULT_SCHEMA.into())),
            conn_info: ConnInfo::new(addr, channel),
        }
    }

    #[inline]
    #[allow(dead_code)]
    pub fn conn_info(&self) -> &ConnInfo {
        &self.conn_info
    }

    #[inline]
    pub fn catalog(&self) -> String {
        self.catalog.load().to_string()
    }

    #[inline]
    pub fn schema(&self) -> String {
        self.schema.load().to_string()
    }

    #[inline]
    pub fn set_catalog(&self, catalog: String) {
        self.catalog.store(Arc::new(catalog));
    }

    #[inline]
    pub fn set_schema(&self, schema: String) {
        self.schema.store(Arc::new(schema));
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
#[allow(dead_code)]
pub enum Channel {
    Mysql,
    Postgres,
}

impl Channel {
    #[allow(dead_code)]
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

/// Attempt to parse catalog and schema from given database name
///
/// The database name may come from different sources:
///
/// - MySQL `schema` name in MySQL protocol login request: it's optional and
///   user
/// and switch database using `USE` command
/// - Postgres `database` parameter in Postgres wire protocol, required
/// - HTTP RESTful API: the database parameter, optional
/// - gRPC: the dbname field in header, optional but has a higher priority than
/// original catalog/schema
///
/// When database name is provided, we attempt to parse catalog and schema from
/// it. We assume the format `[<catalog>-]<schema>`:
///
/// - If `[<catalog>-]` part is not provided, we use whole database name as
/// schema name
/// - if `[<catalog>-]` is provided, we split database name with `-` and use
/// `<catalog>` and `<schema>`.
pub fn parse_catalog_and_schema_from_db_string(db: &str) -> (&str, &str) {
    let parts = db.splitn(2, '-').collect::<Vec<&str>>();
    if parts.len() == 2 {
        (parts[0], parts[1])
    } else {
        (DEFAULT_CATALOG, db)
    }
}

#[cfg(test)]
mod tests {
    use arrow::util::pretty;
    use interpreters::RecordBatchVec;

    use super::*;
    use crate::session::Session;

    #[test]
    fn test_db_string() {
        assert_eq!("test", build_db_string(DEFAULT_CATALOG, "test"));
        assert_eq!("a0b1c2d3-test", build_db_string("a0b1c2d3", "test"));
    }

    #[test]
    fn test_parse_catalog_and_schema() {
        assert_eq!(
            (DEFAULT_CATALOG, "fullschema"),
            parse_catalog_and_schema_from_db_string("fullschema")
        );

        assert_eq!(
            ("catalog", "schema"),
            parse_catalog_and_schema_from_db_string("catalog-schema")
        );

        assert_eq!(
            ("catalog", "schema1-schema2"),
            parse_catalog_and_schema_from_db_string("catalog-schema1-schema2")
        );
    }

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
