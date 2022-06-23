// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{marker::PhantomData, sync::Arc};

use catalog::manager::Manager as CatalogManager;
use log::{error, info};
use opensrv_mysql::{AsyncMysqlShim, ErrorKind, QueryResultWriter, StatementMetaWriter};
use query_engine::executor::Executor as QueryExecutor;
use snafu::ResultExt;
use table_engine::engine::EngineRuntimes;

use crate::{
    context::RequestContext,
    handlers::{self, sql::Request},
    instance::Instance,
    mysql::{error::*, writer::MysqlQueryResultWriter},
};

pub struct MysqlWorker<W: std::io::Write + Send + Sync, C, Q> {
    generic_hold: PhantomData<W>,
    instance: Arc<Instance<C, Q>>,
    runtimes: Arc<EngineRuntimes>,
}

impl<W: std::io::Write + Send + Sync, C: CatalogManager + 'static, Q: QueryExecutor + 'static>
    MysqlWorker<W, C, Q>
{
    pub fn new(instance: Arc<Instance<C, Q>>, runtimes: Arc<EngineRuntimes>) -> Self {
        Self {
            generic_hold: PhantomData::default(),
            instance,
            runtimes,
        }
    }
}

#[async_trait::async_trait]
impl<W: std::io::Write + Send + Sync, C: CatalogManager + 'static, Q: QueryExecutor + 'static>
    AsyncMysqlShim<W> for MysqlWorker<W, C, Q>
{
    type Error = crate::mysql::error::Error;

    async fn on_prepare<'a>(
        &'a mut self,
        _query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> Result<()> {
        info.error(
            ErrorKind::ER_UNKNOWN_ERROR,
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
            ErrorKind::ER_UNKNOWN_ERROR,
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
        results: QueryResultWriter<'a, W>,
    ) -> Result<()> {
        let ctx = match self.create_ctx() {
            Ok(res) => res,
            Err(error) => return Self::err(results, error),
        };

        let req = Request::from(sql.to_string());
        let result = handlers::sql::handle_sql(ctx, self.instance.clone(), req)
            .await
            .map_err(|e| {
                error!("Mysql service Failed to handle sql, err: {}", e);
                e
            })
            .context(HandleSql {
                sql: sql.to_string(),
            });
        match result {
            Ok(res) => {
                let mut writer = MysqlQueryResultWriter::create(results);
                writer.write(res)
            }
            Err(err) => return Self::err(results, err),
        }
    }
}

impl<W: std::io::Write + Sync + Send, C: CatalogManager + 'static, Q: QueryExecutor + 'static>
    MysqlWorker<W, C, Q>
{
    fn err(writer: QueryResultWriter<'_, W>, error: Error) -> Result<()> {
        error!("{}", error);
        let error_msg = format!("{}", error);
        writer.error(ErrorKind::ER_UNKNOWN_ERROR, error_msg.as_bytes())?;
        Ok(())
    }

    fn create_ctx(&self) -> Result<RequestContext> {
        let default_catalog = self
            .instance
            .catalog_manager
            .default_catalog_name()
            .to_string();
        let default_schema = self
            .instance
            .catalog_manager
            .default_schema_name()
            .to_string();
        let runtime = self.runtimes.bg_runtime.clone();

        RequestContext::builder()
            .catalog(default_catalog)
            .tenant(default_schema)
            .runtime(runtime)
            .build()
            .context(CreateContext)
    }
}
