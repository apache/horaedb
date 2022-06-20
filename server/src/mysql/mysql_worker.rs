// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{error::Error as StdError, io, marker::PhantomData, sync::Arc};

use catalog::manager::Manager as CatalogManager;
use opensrv_mysql::{
    AsyncMysqlShim, ErrorKind, ParamParser, QueryResultWriter, StatementMetaWriter,
};
use query_engine::executor::Executor as QueryExecutor;
use snafu::{Backtrace, ResultExt, Snafu};
use table_engine::engine::EngineRuntimes;

use super::mysql_writer::MysqlQueryResultWriter;
use crate::{
    context::RequestContext,
    handlers::{
        self,
        sql::{Request, Response},
    },
    instance::Instance,
};
pub struct MysqlWorker<W: std::io::Write + Send + Sync, C, Q> {
    base: BaseWorker<W, C, Q>,
}

impl<W: std::io::Write + Send + Sync, C: CatalogManager + 'static, Q: QueryExecutor + 'static>
    MysqlWorker<W, C, Q>
{
    pub fn new(instance: Arc<Instance<C, Q>>, runtimes: Arc<EngineRuntimes>) -> Self {
        Self {
            base: BaseWorker::new(instance, runtimes),
        }
    }
}

#[async_trait::async_trait]
impl<W: std::io::Write + Send + Sync, C: CatalogManager + 'static, Q: QueryExecutor + 'static>
    AsyncMysqlShim<W> for MysqlWorker<W, C, Q>
{
    type Error = std::io::Error;

    async fn on_prepare<'a>(
        &'a mut self,
        query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> io::Result<()> {
        self.base.do_prepare(query, info).await
    }

    async fn on_execute<'a>(
        &'a mut self,
        id: u32,
        param: opensrv_mysql::ParamParser<'a>,
        writer: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        self.base.do_exec(id, param, writer).await
    }

    async fn on_close(&mut self, id: u32) {
        self.base.do_close(id).await
    }

    async fn on_query<'a>(
        &'a mut self,
        sql: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        match self.base.do_query(sql).await {
            Ok(res) => {
                let mut writer = MysqlQueryResultWriter::create(results);
                writer.write(res)
            }
            Err(err) => {
                results.error(ErrorKind::ER_UNKNOWN_ERROR, format!("{}", err).as_bytes())?;
                Err(std::io::Error::new(
                    io::ErrorKind::Other,
                    format!("{}", err),
                ))
            }
        }
    }
}

pub struct BaseWorker<W: std::io::Write + Send + Sync, C, Q> {
    generic_hold: PhantomData<W>,
    instance: Arc<Instance<C, Q>>,
    runtimes: Arc<EngineRuntimes>,
}

impl<W: std::io::Write + Sync + Send, C: CatalogManager + 'static, Q: QueryExecutor + 'static>
    BaseWorker<W, C, Q>
{
    pub fn new(instance: Arc<Instance<C, Q>>, runtimes: Arc<EngineRuntimes>) -> Self {
        Self {
            generic_hold: PhantomData::default(),
            instance,
            runtimes,
        }
    }

    async fn do_close(&mut self, _: u32) {}

    async fn do_prepare(
        &mut self,
        _: &str,
        writer: StatementMetaWriter<'_, W>,
    ) -> std::io::Result<()> {
        writer.error(
            ErrorKind::ER_UNKNOWN_ERROR,
            "Prepare is not support in ceresDB".as_bytes(),
        )?;
        Ok(())
    }

    async fn do_query(&mut self, query: &str) -> Result<Response> {
        let ctx = self.create_ctx()?;
        let req = Request::from_query(query.into());
        handlers::sql::handle_sql(ctx, self.instance.clone(), req)
            .await
            .map_err(|e| {
                log::error!("Mysql service Failed to handle sql, err: {}", e);
                e
            })
            .context(HandleSQL {
                sql: query.to_string(),
            })
    }

    async fn do_exec<'a>(
        &'a mut self,
        _: u32,
        _: ParamParser<'_>,
        writer: QueryResultWriter<'_, W>,
    ) -> std::io::Result<()> {
        writer.error(
            ErrorKind::ER_UNKNOWN_ERROR,
            "Execute is not support in ceresDB".as_bytes(),
        )?;
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

define_result!(Error);

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Mysql Server not running, err: {}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    ServerNotRunning {
        backtrace: Backtrace,
        source: std::io::Error,
    },

    #[snafu(display("Failed to create request context, err:{}", source))]
    CreateContext { source: crate::context::Error },

    #[snafu(display("Failed to handle SQL: {}, err:{}", sql, source))]
    HandleSQL {
        sql: String,
        source: crate::handlers::error::Error,
    },

    #[snafu(display(
        "Mysql Server Accept a new Connection fail, err:{}.\nBacktrace:\n{},",
        source,
        backtrace
    ))]
    AcceptConnection {
        backtrace: Backtrace,
        source: std::io::Error,
    },

    #[snafu(display("Missing runtimes to build service.\nBacktrace:\n{}", backtrace))]
    MissingRuntimes { backtrace: Backtrace },

    #[snafu(display("Missing instance to build service.\nBacktrace:\n{}", backtrace))]
    MissingInstance { backtrace: Backtrace },

    #[snafu(display(
        "Fail to do heap profiling, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    ProfileHeap {
        source: profile::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Fail to join async task, err:{}.", source))]
    JoinAsyncTask { source: common_util::runtime::Error },

    #[snafu(display(
        "Failed to parse ip addr, ip:{}, err:{}.\nBacktrace:\n{}",
        ip,
        source,
        backtrace
    ))]
    ParseIpAddr {
        ip: String,
        source: std::net::AddrParseError,
        backtrace: Backtrace,
    },

    #[snafu(display("Internal err:{}.", source))]
    Internal {
        source: Box<dyn StdError + Send + Sync>,
    },
}
