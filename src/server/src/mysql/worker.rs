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

use std::{marker::PhantomData, net::SocketAddr, sync::Arc, time::Duration};

use generic_error::BoxError;
use interpreters::interpreter::Output;
use logger::{error, info};
use opensrv_mysql::{
    AsyncMysqlShim, ErrorKind, InitWriter, QueryResultWriter, StatementMetaWriter,
};
use proxy::{auth::ADMIN_TENANT, context::RequestContext, http::sql::Request, Proxy};
use snafu::ResultExt;

use crate::{
    federated,
    mysql::{
        error::{CreateContext, HandleSql, Result},
        writer::MysqlQueryResultWriter,
    },
    session::{parse_catalog_and_schema_from_db_string, Channel, Session, SessionRef},
};

pub struct MysqlWorker<W: std::io::Write + Send + Sync> {
    generic_hold: PhantomData<W>,
    proxy: Arc<Proxy>,
    session: SessionRef,
    timeout: Option<Duration>,
}

impl<W> MysqlWorker<W>
where
    W: std::io::Write + Send + Sync,
{
    pub fn new(proxy: Arc<Proxy>, add: SocketAddr, timeout: Option<Duration>) -> Self {
        Self {
            generic_hold: PhantomData,
            proxy,
            session: Arc::new(Session::new(Some(add), Channel::Mysql)),
            timeout,
        }
    }
}

#[async_trait::async_trait]
impl<W> AsyncMysqlShim<W> for MysqlWorker<W>
where
    W: std::io::Write + Send + Sync,
{
    type Error = crate::mysql::error::Error;

    async fn on_prepare<'a>(
        &'a mut self,
        _query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> Result<()> {
        info.error(
            ErrorKind::ER_NOT_SUPPORTED_YET,
            "Prepare is not supported in HoraeDB".as_bytes(),
        )?;
        Ok(())
    }

    async fn on_execute<'a>(
        &'a mut self,
        _id: u32,
        _param: opensrv_mysql::ParamParser<'a>,
        writer: QueryResultWriter<'a, W>,
    ) -> Result<()> {
        writer.error(
            ErrorKind::ER_NOT_SUPPORTED_YET,
            "Execute is not supported in HoraeDB".as_bytes(),
        )?;
        Ok(())
    }

    async fn on_close(&mut self, id: u32) {
        info!("client(id={}) wishes to deallocate resources associated with a previously prepared statement.", id)
    }

    async fn on_query<'a>(
        &'a mut self,
        sql: &'a str,
        writer: QueryResultWriter<'a, W>,
    ) -> Result<()> {
        match self.do_query(sql).await {
            Ok(res) => {
                let mut writer = MysqlQueryResultWriter::create(writer);
                writer.write(res)
            }
            Err(error) => {
                error!("MysqlWorker on_query failed. err:{}", error);
                let error_msg = error.to_string();
                writer.error(ErrorKind::ER_UNKNOWN_ERROR, error_msg.as_bytes())?;
                Ok(())
            }
        }
    }

    async fn on_init<'a>(&'a mut self, database: &'a str, w: InitWriter<'a, W>) -> Result<()> {
        let (catalog, schema) = parse_catalog_and_schema_from_db_string(database);

        self.session.set_catalog(catalog.into());
        self.session.set_schema(schema.into());

        w.ok().map_err(|e| e.into())
    }
}

impl<W> MysqlWorker<W>
where
    W: std::io::Write + Send + Sync,
{
    async fn do_query<'a>(&'a mut self, sql: &'a str) -> Result<Output> {
        if let Some(output) = federated::check(sql, self.session.clone()) {
            return Ok(output);
        }

        let req = Request {
            query: sql.to_string(),
        };
        let ctx = self.create_ctx(self.session.clone())?;
        self.proxy
            .handle_http_sql_query(&ctx, req)
            .await
            .map_err(|e| {
                error!("Mysql service Failed to handle sql, err: {}", e);
                e
            })
            .box_err()
            .context(HandleSql {
                sql: sql.to_string(),
            })
    }

    fn create_ctx(&self, session: SessionRef) -> Result<RequestContext> {
        RequestContext::builder()
            .catalog(session.catalog().to_string())
            .schema(session.schema().to_string())
            .timeout(self.timeout)
            .tenant(Some(ADMIN_TENANT.to_string()))
            .build()
            .context(CreateContext)
    }
}
