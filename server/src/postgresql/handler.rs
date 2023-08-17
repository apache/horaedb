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

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use common_types::datum::{Datum, DatumKind};
use datafusion::parquet::data_type::AsBytes;
use futures::stream;
use interpreters::interpreter::Output;
use log::error;
use pgwire::{
    api::{
        query::SimpleQueryHandler,
        results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag},
        ClientInfo, Type,
    },
    error::{PgWireError, PgWireResult},
};
use proxy::{context::RequestContext, http::sql::Request, Proxy};
use query_engine::{executor::Executor as QueryExecutor, physical_planner::PhysicalPlanner};
use snafu::ResultExt;

use crate::postgresql::error::{CreateContext, Result};
pub struct PostgresqlHandler {
    pub(crate) proxy: Arc<Proxy>,
    pub(crate) timeout: Option<Duration>,
}

#[async_trait]
impl SimpleQueryHandler for PostgresqlHandler {
    async fn do_query<'a, C>(&self, _client: &C, sql: &'a str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let ctx = self
            .create_ctx()
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        let req = Request {
            query: sql.to_string(),
        };
        let results = self
            .proxy
            .handle_http_sql_query(&ctx, req)
            .await
            .map_err(|e| {
                error!("PostgreSQL service Failed to handle sql, err: {}", e);
                PgWireError::ApiError(Box::new(e))
            })?;

        Ok(vec![into_pg_reponse(results)?])
    }
}

impl PostgresqlHandler {
    fn create_ctx(&self) -> Result<RequestContext> {
        let default_catalog = self
            .proxy
            .instance()
            .catalog_manager
            .default_catalog_name()
            .to_string();
        let default_schema = self
            .proxy
            .instance()
            .catalog_manager
            .default_schema_name()
            .to_string();

        RequestContext::builder()
            .catalog(default_catalog)
            .schema(default_schema)
            .timeout(self.timeout)
            .build()
            .context(CreateContext)
    }
}

fn into_pg_reponse<'a>(out: Output) -> PgWireResult<Response<'a>> {
    match out {
        Output::AffectedRows(0) => Ok(Response::EmptyQuery),
        Output::AffectedRows(count) => {
            Ok(Response::Execution(Tag::new_for_execution("", Some(count))))
        }
        Output::Records(rows) => {
            if rows.is_empty() {
                return Ok(Response::EmptyQuery);
            }
            let schema = Arc::new(
                rows[0]
                    .schema()
                    .columns()
                    .iter()
                    .map(|c| {
                        FieldInfo::new(
                            c.name.clone(),
                            None,
                            Some(c.id as i16),
                            convert_data_type(&c.data_type),
                            FieldFormat::Text,
                        )
                    })
                    .collect::<Vec<_>>(),
            );

            let mut data_rows = vec![];

            for row in rows {
                let num_cols = row.num_columns();
                let num_rows = row.num_rows();
                for row_idx in 0..num_rows {
                    let mut encoder = DataRowEncoder::new(schema.clone());
                    for col_idx in 0..num_cols {
                        encode_data(&mut encoder, row.column(col_idx).datum(row_idx))?;
                    }
                    data_rows.push(encoder.finish());
                }
            }

            Ok(Response::Query(QueryResponse::new(
                schema,
                stream::iter(data_rows.into_iter()),
            )))
        }
    }
}

fn convert_data_type(data_type: &DatumKind) -> Type {
    match data_type {
        DatumKind::Null => Type::NAME,
        DatumKind::Timestamp => Type::TIMESTAMP,
        DatumKind::Double => Type::FLOAT8,
        DatumKind::Float => Type::FLOAT4,
        DatumKind::Varbinary => Type::BYTEA,
        DatumKind::String => Type::TEXT,
        DatumKind::Int64 | DatumKind::UInt64 => Type::INT8,
        DatumKind::Int32 | DatumKind::UInt32 => Type::INT4,
        DatumKind::Int16 | DatumKind::UInt16 => Type::INT2,
        DatumKind::Int8 | DatumKind::UInt8 => Type::CHAR,
        DatumKind::Boolean => Type::BOOL,
        DatumKind::Date => Type::DATE,
        DatumKind::Time => Type::TIME,
    }
}

fn encode_data(encoder: &mut DataRowEncoder, val: Datum) -> PgWireResult<()> {
    match val {
        Datum::Null => encoder.encode_field(&None::<i8>),
        Datum::Timestamp(t) => encoder.encode_field(&t.as_i64()),
        Datum::Double(d) => encoder.encode_field(&d),
        Datum::Float(f) => encoder.encode_field(&f),
        Datum::Varbinary(v) => encoder.encode_field(&v.as_bytes()),
        Datum::String(s) => encoder.encode_field(&s.as_str()),
        Datum::Int16(i) => encoder.encode_field(&i),
        Datum::Int8(i) => encoder.encode_field(&i),
        Datum::Int32(i) => encoder.encode_field(&i),
        Datum::Int64(i) => encoder.encode_field(&i),
        Datum::UInt32(i) => encoder.encode_field(&i),
        Datum::Boolean(b) => encoder.encode_field(&b),
        Datum::Date(v) => encoder.encode_field(&v),
        Datum::Time(v) => encoder.encode_field(&v),
        // FIXME: PostgreSQL does not support unsigned integers in the wire protocol.
        // Maybe we should return decimal or numeric instead?
        Datum::UInt64(v) => encoder.encode_field(&format!("{v}")),
        Datum::UInt16(v) => encoder.encode_field(&format!("{v}")),
        Datum::UInt8(v) => encoder.encode_field(&format!("{v}")),
    }
}
