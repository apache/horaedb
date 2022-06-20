// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{io, marker::PhantomData, sync::Arc};

use catalog::manager::Manager as CatalogManager;
use opensrv_mysql::{
    AsyncMysqlShim, ErrorKind, ParamParser, QueryResultWriter, StatementMetaWriter,
};
use query_engine::executor::Executor as QueryExecutor;

use crate::instance::Instance;
pub struct MysqlWorker<W: std::io::Write + Send + Sync, C, Q> {
    base: BaseWorker<W>,
    _instance: Arc<Instance<C, Q>>,
}

impl<W: std::io::Write + Send + Sync, C, Q> MysqlWorker<W, C, Q> {
    pub fn new(instance: Arc<Instance<C, Q>>) -> Self {
        Self {
            base: BaseWorker::new(),
            _instance: instance,
        }
    }
}

#[async_trait::async_trait]
impl<W: std::io::Write + Send + Sync, C: CatalogManager, Q: QueryExecutor> AsyncMysqlShim<W>
    for MysqlWorker<W, C, Q>
{
    type Error = std::io::Error;

    async fn on_prepare<'a>(
        &'a mut self,
        _: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> io::Result<()> {
        info.reply(42, &[], &[])
    }

    async fn on_execute<'a>(
        &'a mut self,
        id: u32,
        param: opensrv_mysql::ParamParser<'a>,
        writer: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        self.base.do_exec(id, param, writer).await
    }

    async fn on_close(&mut self, _: u32) {}

    async fn on_query<'a>(
        &'a mut self,
        _sql: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        let cols = [];
        let mut rw = results.start(&cols)?;
        rw.write_col(42)?;
        rw.write_col("b's value")?;
        rw.finish()
    }
}

pub struct BaseWorker<W: std::io::Write + Send + Sync> {
    generic_hold: PhantomData<W>,
}

impl<W: std::io::Write + Sync + Send> BaseWorker<W> {
    pub fn new() -> Self {
        Self {
            generic_hold: PhantomData::default(),
        }
    }

    async fn do_exec<'a>(
        &'a mut self,
        _: u32,
        _: ParamParser<'_>,
        writer: QueryResultWriter<'_, W>,
    ) -> std::io::Result<()> {
        writer.error(
            ErrorKind::ER_UNKNOWN_ERROR,
            "Mysql Execute not support".as_bytes(),
        )?;
        Ok(())
    }
}
