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

//! Replay bench.

use std::sync::Arc;

use analytic_engine::RecoverMode;
use runtime::Runtime;
use util::{OpenTablesMethod, RocksDBEngineBuildContext, TestContext, TestEnv};
use wal::rocksdb_impl::manager::RocksDBWalsOpener;

use crate::{config::ReplayConfig, table::FixedSchemaTable, util};

pub struct ReplayBench {
    runtime: Arc<Runtime>,
    test_ctx: TestContext<RocksDBWalsOpener>,
    table: FixedSchemaTable,
    batch_size: usize,
}

impl ReplayBench {
    pub fn new(config: ReplayConfig) -> Self {
        let runtime = util::new_runtime(1);
        let engine_context = RocksDBEngineBuildContext::new(
            RecoverMode::TableBased,
            OpenTablesMethod::WithOpenShard,
        );
        let env: TestEnv = TestEnv::builder().build();

        let (test_ctx, fixed_schema_table) = env.block_on(async {
            let mut test_ctx = env.new_context(&engine_context);
            test_ctx.open().await;

            let fixed_schema_table = test_ctx
                .create_fixed_schema_table("test_replay_table1")
                .await;
            let _ = test_ctx
                .create_fixed_schema_table("test_replay_table2")
                .await;
            let _ = test_ctx
                .create_fixed_schema_table("test_replay_table3")
                .await;

            (test_ctx, fixed_schema_table)
        });

        ReplayBench {
            runtime: Arc::new(runtime),
            test_ctx,
            table: fixed_schema_table,
            batch_size: config.batch_size,
        }
    }

    pub fn run_bench(&mut self) {
        self.runtime.block_on(async {
            self.table.prepare_write_requests(self.batch_size);
            let rows = self.table.row_tuples();

            // Write data to table.
            let mut table_names = Vec::new();
            for (table_name, _) in self.test_ctx.name_to_tables().iter() {
                let row_group = self.table.rows_to_row_group(&rows);
                self.test_ctx
                    .write_to_table(table_name.as_str(), row_group)
                    .await;
                table_names.push(table_name.clone());
            }

            // Reopen db.
            self.test_ctx
                .reopen_with_tables(
                    table_names
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .as_slice(),
                )
                .await;
        });
    }
}
