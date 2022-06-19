// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{io, marker::PhantomData, sync::Arc};

use catalog::manager::Manager as CatalogManager;
use msql_srv::{
    ErrorKind, InitWriter, MysqlShim, ParamParser, QueryResultWriter, StatementMetaWriter,
};
use query_engine::executor::Executor as QueryExecutor;

use crate::instance::Instance;
pub struct MysqlWorker<W: std::io::Write, C, Q> {
    base: BaseWorker<W>,
    _instance: Arc<Instance<C, Q>>,
}

impl<W: std::io::Write, C, Q> MysqlWorker<W, C, Q> {
    pub fn new(instance: Arc<Instance<C, Q>>) -> Self {
        Self {
            base: BaseWorker::new(),
            _instance: instance,
        }
    }
}

impl<W: std::io::Write, C: CatalogManager, Q: QueryExecutor> MysqlShim<W> for MysqlWorker<W, C, Q> {
    type Error = std::io::Error;

    fn on_prepare(&mut self, _: &str, info: StatementMetaWriter<W>) -> io::Result<()> {
        info.reply(42, &[], &[])
    }

    fn on_execute(
        &mut self,
        id: u32,
        param: ParamParser,
        writer: QueryResultWriter<W>,
    ) -> io::Result<()> {
        self.base.do_exec(id, param, writer)
    }

    fn on_close(&mut self, _: u32) {}

    fn on_init(&mut self, _: &str, _writer: InitWriter<W>) -> io::Result<()> {
        Ok(())
    }

    fn on_query(&mut self, _: &str, results: QueryResultWriter<W>) -> io::Result<()> {
        let cols = [];
        let mut rw = results.start(&cols)?;
        rw.write_col(42)?;
        rw.write_col("b's value")?;
        rw.finish()
    }
}

pub struct BaseWorker<W: std::io::Write> {
    generic_hold: PhantomData<W>,
}

impl<W: std::io::Write> BaseWorker<W> {
    pub fn new() -> Self {
        Self {
            generic_hold: PhantomData::default(),
        }
    }

    fn do_exec(
        &mut self,
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
