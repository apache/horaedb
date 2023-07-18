// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{marker::PhantomData, sync::Arc, time::Duration};

use generic_error::BoxError;
use interpreters::interpreter::Output;
use log::{error, info};
use opensrv_mysql::{AsyncMysqlShim, ErrorKind, QueryResultWriter, StatementMetaWriter};
use proxy::{context::RequestContext, http::sql::Request, Proxy};
use query_engine::executor::Executor as QueryExecutor;
use snafu::ResultExt;

use crate::mysql::{
    error::{CreateContext, HandleSql, Result},
    writer::MysqlQueryResultWriter,
};

pub struct MysqlWorker<W: std::io::Write + Send + Sync, Q> {
    generic_hold: PhantomData<W>,
    proxy: Arc<Proxy<Q>>,
    timeout: Option<Duration>,
}

impl<W, Q> MysqlWorker<W, Q>
where
    W: std::io::Write + Send + Sync,
    Q: QueryExecutor + 'static,
{
    pub fn new(proxy: Arc<Proxy<Q>>, timeout: Option<Duration>) -> Self {
        Self {
            generic_hold: PhantomData::default(),
            proxy,
            timeout,
        }
    }
}

#[async_trait::async_trait]
impl<W, Q> AsyncMysqlShim<W> for MysqlWorker<W, Q>
where
    W: std::io::Write + Send + Sync,
    Q: QueryExecutor + 'static,
{
    type Error = crate::mysql::error::Error;

    async fn on_prepare<'a>(
        &'a mut self,
        _query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> Result<()> {
        info.error(
            ErrorKind::ER_NOT_SUPPORTED_YET,
            "Prepare is not supported in CeresDB".as_bytes(),
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
            "Execute is not supported in CeresDB".as_bytes(),
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
}

impl<W, Q> MysqlWorker<W, Q>
where
    W: std::io::Write + Send + Sync,
    Q: QueryExecutor + 'static,
{
    async fn do_query<'a>(&'a mut self, sql: &'a str) -> Result<Output> {
        let ctx = self.create_ctx()?;
        let req = Request {
            query: sql.to_string(),
        };
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
            .enable_partition_table_access(false)
            .timeout(self.timeout)
            .build()
            .context(CreateContext)
    }
}
