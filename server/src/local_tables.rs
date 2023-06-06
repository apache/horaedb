// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Recover tables in standalone mode

use catalog::{
    schema::{OpenOptions, OpenShardRequest, TableDef},
    table_operator::TableOperator,
};
use common_types::table::DEFAULT_SHARD_ID;
use common_util::{
    define_result,
    error::{BoxError, GenericError},
};
use snafu::{Backtrace, ResultExt, Snafu};
use table_engine::table::TableInfo;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to recover local tables with cause, msg:{}, err:{}",
        msg,
        source
    ))]
    RecoverWithCause { msg: String, source: GenericError },

    #[snafu(display(
        "Failed to recover local tables with cause, msg:{}.\nBacktrace:\n{}",
        msg,
        backtrace
    ))]
    RecoverNoCause { msg: String, backtrace: Backtrace },
}

define_result!(Error);

/// Local tables recoverer
pub struct LocalTablesRecoverer {
    table_infos: Vec<TableInfo>,
    table_operator: TableOperator,
    open_opts: OpenOptions,
}

impl LocalTablesRecoverer {
    pub fn new(
        table_infos: Vec<TableInfo>,
        table_operator: TableOperator,
        open_opts: OpenOptions,
    ) -> Self {
        Self {
            table_infos,
            table_operator,
            open_opts,
        }
    }

    pub async fn recover(&self) -> Result<()> {
        if self.table_infos.is_empty() {
            return Ok(());
        }

        let engine = self.table_infos[0].engine.clone();
        let table_defs = self
            .table_infos
            .iter()
            .map(|info| TableDef {
                catalog_name: info.catalog_name.clone(),
                schema_name: info.schema_name.clone(),
                id: info.table_id,
                name: info.table_name.clone(),
            })
            .collect();
        let request = OpenShardRequest {
            shard_id: DEFAULT_SHARD_ID,
            table_defs,
            engine,
        };
        let opts = self.open_opts.clone();

        self.table_operator
            .open_shard(request, opts)
            .await
            .box_err()
            .context(RecoverWithCause {
                msg: format!(
                    "failed to recover tables, table_info:{:?}",
                    self.table_infos
                ),
            })?;

        Ok(())
    }
}
