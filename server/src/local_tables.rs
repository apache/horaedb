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

//! Recover tables in standalone mode

use catalog::{
    schema::{OpenOptions, OpenShardRequest, TableDef},
    table_operator::TableOperator,
};
use common_types::table::DEFAULT_SHARD_ID;
use generic_error::{BoxError, GenericError};
use macros::define_result;
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
