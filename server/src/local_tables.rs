// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Recover tables in standalone mode

use catalog::{
    schema::{OpenOptions, OpenTableRequest},
    CatalogRef,
};
use common_util::{
    define_result,
    error::{BoxError, GenericError},
};
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
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
    catalog: CatalogRef,
    open_opts: OpenOptions,
}

impl LocalTablesRecoverer {
    pub fn new(table_infos: Vec<TableInfo>, catalog: CatalogRef, open_opts: OpenOptions) -> Self {
        Self {
            table_infos,
            catalog,
            open_opts,
        }
    }

    pub async fn recover(&self) -> Result<()> {
        let opts = self.open_opts.clone();
        for table_info in &self.table_infos {
            let schema = self
                .catalog
                .schema_by_name(&table_info.schema_name)
                .box_err()
                .context(RecoverWithCause {
                    msg: format!("failed to get schema of table, table_info:{table_info:?}"),
                })?
                .with_context(|| RecoverNoCause {
                    msg: format!("schema of table not found, table_info:{table_info:?}"),
                })?;
            schema.add_opening_table(&table_info.table_name);
            let open_request = OpenTableRequest::from(table_info.clone());
            schema
                .open_table(open_request.clone(), opts.clone())
                .await
                .box_err()
                .context(RecoverWithCause {
                    msg: format!("failed to open table, open_request:{open_request:?}"),
                })?
                .with_context(|| RecoverNoCause {
                    msg: format!("no table is opened, open_request:{open_request:?}"),
                })?;
            schema.remove_opening_table(&table_info.table_name);
        }

        Ok(())
    }
}
