// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use analytic_engine::tests::util::{EngineContext, RocksDBEngineContext, TestEnv};
use catalog::consts::{DEFAULT_CATALOG, DEFAULT_SCHEMA};
use catalog_impls::table_based::TableBasedManager;
use common_types::request_id::RequestId;
use query_engine::{executor::ExecutorImpl, Config as QueryConfig};
use sql::{
    parser::Parser, plan::Plan, planner::Planner, provider::MetaProvider, tests::MockMetaProvider,
};
use table_engine::engine::TableEngineRef;

use crate::{
    context::Context,
    factory::Factory,
    interpreter::{Output, Result},
    table_manipulator::catalog_based::TableManipulatorImpl,
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
    async fn build_factory(&self) -> Factory<ExecutorImpl> {
        let catalog_manager = Arc::new(build_catalog_manager(self.engine()).await);
        let table_manipulator = Arc::new(TableManipulatorImpl::new(catalog_manager.clone()));
        Factory::new(
            ExecutorImpl::new(query_engine::Config::default()),
            catalog_manager,
            self.engine(),
            table_manipulator,
        )
    }

    async fn sql_to_output(&self, sql: &str) -> Result<Output> {
        let plan = sql_to_plan(&self.meta_provider, sql);

        let ctx = Context::builder(RequestId::next_id(), None)
            .default_catalog_and_schema(DEFAULT_CATALOG.to_string(), DEFAULT_SCHEMA.to_string())
            .build();

        let factory = self.build_factory().await;
        let interpreter = factory.create(ctx, plan);
        interpreter.execute().await
    }

    async fn test_create_table(&self) {
        let sql="CREATE TABLE IF NOT EXISTS test_table(c1 string tag not null,ts timestamp not null, c3 string, timestamp key(ts),primary key(c1, ts)) \
        ENGINE=Analytic WITH (ttl='70d',update_mode='overwrite',arena_block_size='1KB')";

        let output = self.sql_to_output(sql).await.unwrap();
        assert!(
            matches!(output, Output::AffectedRows(v) if v == 0),
            "create table should success"
        );
    }

    async fn test_desc_table(&self) {
        let sql = "desc table test_table";
        let output = self.sql_to_output(sql).await.unwrap();
        let records = output.try_into().unwrap();
        let expected = vec![
            "+--------+-----------+------------+-------------+--------+",
            "| name   | type      | is_primary | is_nullable | is_tag |",
            "+--------+-----------+------------+-------------+--------+",
            "| key1   | varbinary | true       | false       | false  |",
            "| key2   | timestamp | true       | false       | false  |",
            "| field1 | double    | false      | true        | false  |",
            "| field2 | string    | false      | true        | false  |",
            "+--------+-----------+------------+-------------+--------+",
        ];
        common_util::record_batch::assert_record_batches_eq(&expected, records);
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
        common_util::record_batch::assert_record_batches_eq(&expected, records);
    }

    async fn test_insert_table(&self) {
        let sql = "INSERT INTO test_table(key1, key2, field1,field2) VALUES('tagk', 1638428434000,100, 'hello3'),('tagk2', 1638428434000,100, 'hello3');";
        let output = self.sql_to_output(sql).await.unwrap();
        assert!(
            matches!(output, Output::AffectedRows(v) if v == 2),
            "insert table should success"
        );
    }

    async fn test_insert_table_with_missing_columns(&self) {
        let catalog_manager = Arc::new(build_catalog_manager(self.engine()).await);
        let ctx = Context::builder(RequestId::next_id(), None)
            .default_catalog_and_schema(DEFAULT_CATALOG.to_string(), DEFAULT_SCHEMA.to_string())
            .build();
        let table_manipulator = Arc::new(TableManipulatorImpl::new(catalog_manager.clone()));
        let insert_factory = Factory::new(
            ExecutorImpl::new(QueryConfig::default()),
            catalog_manager.clone(),
            self.engine(),
            table_manipulator.clone(),
        );
        let insert_sql = "INSERT INTO test_missing_columns_table(key1, key2, field4) VALUES('tagk', 1638428434000, 1), ('tagk2', 1638428434000, 10);";

        let plan = sql_to_plan(&self.meta_provider, insert_sql);
        let interpreter = insert_factory.create(ctx, plan);
        let output = interpreter.execute().await.unwrap();
        assert!(
            matches!(output, Output::AffectedRows(v) if v == 2),
            "insert should success"
        );

        // Check data which just insert.
        let select_sql =
            "SELECT key1, key2, field1, field2, field3, field4, field5 from test_missing_columns_table";
        let select_factory = Factory::new(
            ExecutorImpl::new(QueryConfig::default()),
            catalog_manager,
            self.engine(),
            table_manipulator,
        );
        let ctx = Context::builder(RequestId::next_id(), None)
            .default_catalog_and_schema(DEFAULT_CATALOG.to_string(), DEFAULT_SCHEMA.to_string())
            .build();
        let plan = sql_to_plan(&self.meta_provider, select_sql);
        let interpreter = select_factory.create(ctx, plan);
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
            "| 7461676b   | 2021-12-02 07:00:34 | 10     | 20     | 3      | 1      | 3      |",
            "| 7461676b32 | 2021-12-02 07:00:34 | 10     | 20     | 3      | 10     | 12     |",
            "+------------+---------------------+--------+--------+--------+--------+--------+",
        ];
        common_util::record_batch::assert_record_batches_eq(&expected, records);
    }

    async fn test_select_table(&self) {
        let sql = "select * from test_table";
        let output = self.sql_to_output(sql).await.unwrap();
        let records = output.try_into().unwrap();
        let expected = vec![
            "+------------+---------------------+--------+--------+",
            "| key1       | key2                | field1 | field2 |",
            "+------------+---------------------+--------+--------+",
            "| 7461676b   | 2021-12-02 07:00:34 | 100    | hello3 |",
            "| 7461676b32 | 2021-12-02 07:00:34 | 100    | hello3 |",
            "+------------+---------------------+--------+--------+",
        ];
        common_util::record_batch::assert_record_batches_eq(&expected, records);

        let sql = "select count(*) from test_table";
        let output = self.sql_to_output(sql).await.unwrap();
        let records = output.try_into().unwrap();
        let expected = vec![
            "+-----------------+",
            "| COUNT(UInt8(1)) |",
            "+-----------------+",
            "| 2               |",
            "+-----------------+",
        ];
        common_util::record_batch::assert_record_batches_eq(&expected, records);
    }

    async fn test_show_create_table(&self) {
        let sql = "show create table test_table";
        let output = self.sql_to_output(sql).await.unwrap();
        let records = output.try_into().unwrap();
        let expected = vec![
            "+------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| Table      | Create Table                                                                                                                                                                    |",
            "+------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| test_table | CREATE TABLE `test_table` (`key1` varbinary NOT NULL, `key2` timestamp NOT NULL, `field1` double, `field2` string, PRIMARY KEY(key1,key2), TIMESTAMP KEY(key2)) ENGINE=Analytic |",
            "+------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+"
        ];
        common_util::record_batch::assert_record_batches_eq(&expected, records);
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
}

#[tokio::test]
async fn test_interpreters_rocks() {
    let rocksdb_ctx = RocksDBEngineContext::default();
    test_interpreters(rocksdb_ctx).await;
}

async fn test_interpreters<T: EngineContext>(engine_context: T) {
    let env = TestEnv::builder().build();
    let mut test_ctx = env.new_context(engine_context);
    test_ctx.open().await;
    let mock = MockMetaProvider::default();
    let env = Env {
        engine: test_ctx.clone_engine(),
        meta_provider: mock,
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
}
