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

use std::sync::Arc;

use analytic_engine::tests::util::{EngineBuildContext, RocksDBEngineBuildContext, TestEnv};
use catalog::{
    consts::{DEFAULT_CATALOG, DEFAULT_SCHEMA},
    manager::ManagerRef,
    table_operator::TableOperator,
};
use catalog_impls::table_based::TableBasedManager;
use common_types::request_id::RequestId;
use datafusion::execution::runtime_env::RuntimeEnv;
use query_engine::{
    datafusion_impl::physical_planner::DatafusionPhysicalPlannerImpl, executor::ExecutorImpl,
};
use query_frontend::{
    parser::Parser, plan::Plan, planner::Planner, provider::MetaProvider, tests::MockMetaProvider,
};
use table_engine::engine::TableEngineRef;

use crate::{
    context::Context,
    factory::Factory,
    interpreter::{Output, Result},
    table_manipulator::{catalog_based::TableManipulatorImpl, TableManipulatorRef},
};

async fn build_catalog_manager(analytic: TableEngineRef) -> TableBasedManager {
    // Create catalog manager, use analytic table as backend
    TableBasedManager::new(analytic.clone())
        .await
        .expect("Failed to create catalog manager")
}

fn sql_to_plan<M: MetaProvider>(meta_provider: &M, sql: &str) -> Plan {
    let planner = Planner::new(meta_provider, RequestId::next_id(), 1);
    let mut statements = Parser::parse_sql(sql).unwrap();
    assert_eq!(statements.len(), 1);
    planner.statement_to_plan(statements.remove(0)).unwrap()
}

struct Env<M>
where
    M: MetaProvider,
{
    pub engine: TableEngineRef,
    pub meta_provider: M,
    pub catalog_manager: ManagerRef,
    pub table_manipulator: TableManipulatorRef,
}

impl<M> Env<M>
where
    M: MetaProvider,
{
    fn engine(&self) -> TableEngineRef {
        self.engine.clone()
    }
}

impl<M> Env<M>
where
    M: MetaProvider,
{
    async fn build_factory(&self) -> Factory {
        Factory::new(
            Arc::new(ExecutorImpl),
            Arc::new(DatafusionPhysicalPlannerImpl::new(
                query_engine::Config::default(),
                Arc::new(RuntimeEnv::default()),
            )),
            self.catalog_manager.clone(),
            self.engine(),
            self.table_manipulator.clone(),
        )
    }

    async fn sql_to_output(&self, sql: &str) -> Result<Output> {
        let ctx = Context::builder(RequestId::next_id(), None)
            .default_catalog_and_schema(DEFAULT_CATALOG.to_string(), DEFAULT_SCHEMA.to_string())
            .build();
        self.sql_to_output_with_context(sql, ctx).await
    }

    async fn sql_to_output_with_context(&self, sql: &str, ctx: Context) -> Result<Output> {
        let plan = sql_to_plan(&self.meta_provider, sql);
        let factory = self.build_factory().await;
        let interpreter = factory.create(ctx, plan)?;
        interpreter.execute().await
    }

    async fn create_table_and_check(
        &self,
        table_name: &str,
        enable_partition_table_access: bool,
    ) -> Result<()> {
        let ctx = Context::builder(RequestId::next_id(), None)
            .default_catalog_and_schema(DEFAULT_CATALOG.to_string(), DEFAULT_SCHEMA.to_string())
            .enable_partition_table_access(enable_partition_table_access)
            .build();
        let sql= format!("CREATE TABLE IF NOT EXISTS {table_name}(c1 string tag not null,ts timestamp not null, c3 string, timestamp key(ts),primary key(c1, ts)) \
        ENGINE=Analytic WITH (ttl='70d',update_mode='overwrite',arena_block_size='1KB')");

        let output = self.sql_to_output_with_context(&sql, ctx).await?;
        assert!(
            matches!(output, Output::AffectedRows(v) if v == 0),
            "create table should success"
        );

        Ok(())
    }

    async fn insert_table_and_check(
        &self,
        table_name: &str,
        enable_partition_table_access: bool,
    ) -> Result<()> {
        let ctx = Context::builder(RequestId::next_id(), None)
            .default_catalog_and_schema(DEFAULT_CATALOG.to_string(), DEFAULT_SCHEMA.to_string())
            .enable_partition_table_access(enable_partition_table_access)
            .build();
        let sql = format!("INSERT INTO {table_name}(key1, key2, field1,field2,field3,field4) VALUES('tagk', 1638428434000,100, 'hello3','2022-10-10','10:10:10.234'),('tagk2', 1638428434000,100, 'hello3','2022-10-11','11:10:10.234');");
        let output = self.sql_to_output_with_context(&sql, ctx).await?;
        assert!(
            matches!(output, Output::AffectedRows(v) if v == 2),
            "insert table should success"
        );

        Ok(())
    }

    async fn select_table_and_check(
        &self,
        table_name: &str,
        enable_partition_table_access: bool,
    ) -> Result<()> {
        let ctx = Context::builder(RequestId::next_id(), None)
            .default_catalog_and_schema(DEFAULT_CATALOG.to_string(), DEFAULT_SCHEMA.to_string())
            .enable_partition_table_access(enable_partition_table_access)
            .build();
        let sql = format!("select * from {table_name}");
        let output = self.sql_to_output_with_context(&sql, ctx).await?;
        let records = output.try_into().unwrap();
        let expected = vec![
            "+------------+---------------------+--------+--------+------------+--------------+",
            "| key1       | key2                | field1 | field2 | field3     | field4       |",
            "+------------+---------------------+--------+--------+------------+--------------+",
            "| 7461676b   | 2021-12-02T07:00:34 | 100.0  | hello3 | 2022-10-10 | 10:10:10.234 |",
            "| 7461676b32 | 2021-12-02T07:00:34 | 100.0  | hello3 | 2022-10-11 | 11:10:10.234 |",
            "+------------+---------------------+--------+--------+------------+--------------+",
        ];
        test_util::assert_record_batches_eq(&expected, records);

        let sql = "select count(*) from test_table";
        let output = self.sql_to_output(sql).await?;
        let records = output.try_into().unwrap();
        let expected = vec![
            "+-----------------+",
            "| COUNT(UInt8(1)) |",
            "+-----------------+",
            "| 2               |",
            "+-----------------+",
        ];
        test_util::assert_record_batches_eq(&expected, records);

        Ok(())
    }

    async fn test_create_table(&self) {
        self.create_table_and_check("test_table", false)
            .await
            .unwrap();
    }

    async fn test_desc_table(&self) {
        let sql = "desc table test_table";
        let output = self.sql_to_output(sql).await.unwrap();
        let records = output.try_into().unwrap();
        let expected = vec![
            "+--------+-----------+------------+-------------+--------+---------------+",
            "| name   | type      | is_primary | is_nullable | is_tag | is_dictionary |",
            "+--------+-----------+------------+-------------+--------+---------------+",
            "| key1   | varbinary | true       | false       | false  | false         |",
            "| key2   | timestamp | true       | false       | false  | false         |",
            "| field1 | double    | false      | true        | false  | false         |",
            "| field2 | string    | false      | true        | false  | false         |",
            "| field3 | date      | false      | true        | false  | false         |",
            "| field4 | time      | false      | true        | false  | false         |",
            "+--------+-----------+------------+-------------+--------+---------------+",
        ];
        test_util::assert_record_batches_eq(&expected, records);
    }

    async fn test_exists_table(&self) {
        let sql = "exists table test_table";
        let output = self.sql_to_output(sql).await.unwrap();
        let records = output.try_into().unwrap();
        let expected = vec![
            "+--------+",
            "| result |",
            "+--------+",
            "| 1      |",
            "+--------+",
        ];
        test_util::assert_record_batches_eq(&expected, records);
    }

    async fn test_insert_table(&self) {
        self.insert_table_and_check("test_table", false)
            .await
            .unwrap();
    }

    async fn test_insert_table_with_missing_columns(&self) {
        let catalog_manager = Arc::new(build_catalog_manager(self.engine()).await);
        let ctx = Context::builder(RequestId::next_id(), None)
            .default_catalog_and_schema(DEFAULT_CATALOG.to_string(), DEFAULT_SCHEMA.to_string())
            .build();
        let table_operator = TableOperator::new(catalog_manager.clone());
        let table_manipulator = Arc::new(TableManipulatorImpl::new(table_operator));
        let insert_factory = Factory::new(
            Arc::new(ExecutorImpl),
            Arc::new(DatafusionPhysicalPlannerImpl::new(
                query_engine::Config::default(),
                Arc::new(RuntimeEnv::default()),
            )),
            catalog_manager.clone(),
            self.engine(),
            table_manipulator.clone(),
        );
        let insert_sql = "INSERT INTO test_missing_columns_table(key1, key2, field4) VALUES('tagk', 1638428434000, 1), ('tagk2', 1638428434000, 10);";

        let plan = sql_to_plan(&self.meta_provider, insert_sql);
        let interpreter = insert_factory.create(ctx, plan).unwrap();
        let output = interpreter.execute().await.unwrap();
        assert!(
            matches!(output, Output::AffectedRows(v) if v == 2),
            "insert should success"
        );

        // Check data which just insert.
        let select_sql =
            "SELECT key1, key2, field1, field2, field3, field4, field5 from test_missing_columns_table";
        let select_factory = Factory::new(
            Arc::new(ExecutorImpl),
            Arc::new(DatafusionPhysicalPlannerImpl::new(
                query_engine::Config::default(),
                Arc::new(RuntimeEnv::default()),
            )),
            catalog_manager,
            self.engine(),
            table_manipulator,
        );
        let ctx = Context::builder(RequestId::next_id(), None)
            .default_catalog_and_schema(DEFAULT_CATALOG.to_string(), DEFAULT_SCHEMA.to_string())
            .build();
        let plan = sql_to_plan(&self.meta_provider, select_sql);
        let interpreter = select_factory.create(ctx, plan).unwrap();
        let output = interpreter.execute().await.unwrap();
        let records = output.try_into().unwrap();

        #[rustfmt::skip]
        // sql: CREATE TABLE `test_missing_columns_table` (`key1` varbinary NOT NULL,
        //                                                 `key2` timestamp NOT NULL,
        //                                                 `field1` bigint NOT NULL DEFAULT 10,
        //                                                 `field2` uint32 NOT NULL DEFAULT 20,
        //                                                 `field3` uint32 NOT NULL DEFAULT 1 + 2,
        //                                                 `field4` uint32 NOT NULL,
        //                                                 `field5` uint32 NOT NULL DEFAULT field4 + 2,
        //                                                 PRIMARY KEY(key1,key2), TIMESTAMP KEY(key2)) ENGINE=Analytic
        let expected = vec![
            "+------------+---------------------+--------+--------+--------+--------+--------+",
            "| key1       | key2                | field1 | field2 | field3 | field4 | field5 |",
            "+------------+---------------------+--------+--------+--------+--------+--------+",
            "| 7461676b   | 2021-12-02T07:00:34 | 10     | 20     | 3      | 1      | 3      |",
            "| 7461676b32 | 2021-12-02T07:00:34 | 10     | 20     | 3      | 10     | 12     |",
            "+------------+---------------------+--------+--------+--------+--------+--------+",
        ];
        test_util::assert_record_batches_eq(&expected, records);
    }

    async fn test_select_table(&self) {
        self.select_table_and_check("test_table", false)
            .await
            .unwrap();
    }

    async fn test_show_create_table(&self) {
        let sql = "show create table test_table";
        let output = self.sql_to_output(sql).await.unwrap();
        let records = output.try_into().unwrap();
        let expected = vec![
            "+------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| Table      | Create Table                                                                                                                                                                                                  |",
            "+------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| test_table | CREATE TABLE `test_table` (`key1` varbinary NOT NULL, `key2` timestamp NOT NULL, `field1` double, `field2` string, `field3` date, `field4` time, PRIMARY KEY(key1,key2), TIMESTAMP KEY(key2)) ENGINE=Analytic |",
            "+------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+"
        ];
        test_util::assert_record_batches_eq(&expected, records);
    }

    async fn test_alter_table(&self) {
        let sql = "alter table test_table add column add_col string";
        let output = self.sql_to_output(sql).await.unwrap();
        assert!(
            matches!(output, Output::AffectedRows(v) if v == 0),
            "alter table should success"
        );

        let sql = "alter table test_table modify SETTING ttl='9d'";
        let output = self.sql_to_output(sql).await.unwrap();
        assert!(
            matches!(output, Output::AffectedRows(v) if v == 0),
            "alter table should success"
        );
    }

    async fn test_drop_table(&self) {
        let sql = "drop table test_table";
        let output = self.sql_to_output(sql).await.unwrap();
        assert!(
            matches!(output, Output::AffectedRows(v) if v == 0),
            "alter table should success"
        );
    }

    async fn test_enable_partition_table_access(&self) {
        // Disable partition table access, all of create, insert and select about sub
        // table(in table partition) directly will failed.
        let res = self.create_table_and_check("__test_table", false).await;
        assert!(format!("{res:?}")
            .contains("only can process sub tables in table partition directly when enable partition table access"));
        let res1 = self.insert_table_and_check("__test_table", false).await;
        assert!(format!("{res1:?}")
            .contains("only can process sub tables in table partition directly when enable partition table access"));
        let res2 = self.select_table_and_check("__test_table", false).await;
        assert!(format!("{res2:?}")
            .contains("only can process sub tables in table partition directly when enable partition table access"));

        // Enable partition table access, operations above will success.
        self.create_table_and_check("__test_table", true)
            .await
            .unwrap();
        self.insert_table_and_check("__test_table", true)
            .await
            .unwrap();
        self.select_table_and_check("__test_table", true)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn test_interpreters_rocks() {
    test_util::init_log_for_test();
    let rocksdb_ctx = RocksDBEngineBuildContext::default();
    test_interpreters(rocksdb_ctx).await;
}

async fn test_interpreters<T: EngineBuildContext>(engine_context: T) {
    let env = TestEnv::builder().build();
    let mut test_ctx = env.new_context(engine_context);
    test_ctx.open().await;
    let mock = MockMetaProvider::default();
    let engine = test_ctx.clone_engine();
    let catalog_manager = Arc::new(build_catalog_manager(engine.clone()).await);
    let table_operator = TableOperator::new(catalog_manager.clone());
    let table_manipulator = Arc::new(TableManipulatorImpl::new(table_operator));

    let env = Env {
        engine: test_ctx.clone_engine(),
        meta_provider: mock,
        catalog_manager,
        table_manipulator,
    };

    env.test_create_table().await;
    env.test_desc_table().await;
    env.test_exists_table().await;
    env.test_insert_table().await;
    env.test_select_table().await;
    env.test_show_create_table().await;
    env.test_alter_table().await;
    env.test_drop_table().await;
    env.test_insert_table_with_missing_columns().await;
    env.test_enable_partition_table_access().await;
}
