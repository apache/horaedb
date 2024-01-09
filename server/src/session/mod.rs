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

pub mod context;

use std::{net::SocketAddr, sync::Arc};

use arc_swap::ArcSwap;
use catalog::consts::{DEFAULT_CATALOG, DEFAULT_SCHEMA};
use context::{Channel, ConnInfo, QueryContextBuilder, QueryContextRef};

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
    pub fn new_query_context(&self) -> QueryContextRef {
        QueryContextBuilder::default()
            .current_catalog(self.catalog.load().to_string())
            .current_schema(self.schema.load().to_string())
            .sql_dialect(self.conn_info.channel.dialect())
            .build()
    }

    #[inline]
    pub fn conn_info(&self) -> &ConnInfo {
        &self.conn_info
    }

    #[inline]
    pub fn mut_conn_info(&mut self) -> &mut ConnInfo {
        &mut self.conn_info
    }

    #[inline]
    pub fn set_catalog(&self, catalog: String) {
        self.catalog.store(Arc::new(catalog));
    }

    #[inline]
    pub fn set_schema(&self, schema: String) {
        self.schema.store(Arc::new(schema));
    }

    pub fn get_db_string(&self) -> String {
        build_db_string(self.catalog.load().as_ref(), self.schema.load().as_ref())
    }
}

/// Build db name from catalog and schema string
pub fn build_db_string(catalog: &str, schema: &str) -> String {
    if catalog == DEFAULT_CATALOG {
        schema.to_string()
    } else {
        format!("{catalog}-{schema}")
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
    use super::*;

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
}
