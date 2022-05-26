// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use analytic_engine::tests::util::TestEnv;
use catalog::consts::{DEFAULT_CATALOG, DEFAULT_SCHEMA};
use catalog_impls::table_based::TableBasedManager;
use common_types::request_id::RequestId;
use query_engine::executor::ExecutorImpl;
use sql::{
    parser::Parser, plan::Plan, planner::Planner, provider::MetaProvider, tests::MockMetaProvider,
};
use table_engine::engine::TableEngine;

use crate::{
    context::Context,
    factory::Factory,
    interpreter::{Output, Result},
};

async fn build_catalog_manager<E>(analytic: E) -> TableBasedManager
where
    E: TableEngine + Clone + Send + Sync + 'static,
{
    // Create catalog manager, use analytic table as backend
    TableBasedManager::new(&analytic.clone(), Arc::new(analytic))
        .await
        .unwrap_or_else(|e| {
            panic!("Failed to create catalog manager, err:{}", e);
        })
}

fn sql_to_plan<M: MetaProvider>(meta_provider: &M, sql: &str) -> Plan {
    let planner = Planner::new(meta_provider, RequestId::next_id(), 1);
    let mut statements = Parser::parse_sql(sql).unwrap();
    assert_eq!(statements.len(), 1);
    planner.statement_to_plan(statements.remove(0)).unwrap()
}

async fn build_factory<E, M>(env: &Env<E, M>) -> Factory<ExecutorImpl, TableBasedManager>
where
    E: TableEngine + Clone + Send + Sync + 'static,
    M: MetaProvider,
{
    let catalog_manager = build_catalog_manager(env.engine()).await;
    Factory::new(ExecutorImpl::new(), catalog_manager, Arc::new(env.engine()))
}

async fn sql_to_output<E, M>(env: &Env<E, M>, sql: &str) -> Result<Output>
where
    E: TableEngine + Clone + Send + Sync + 'static,
    M: MetaProvider,
{
    let plan = sql_to_plan(&env.meta_provider, sql);

    let ctx = Context::builder(RequestId::next_id())
        .default_catalog_and_schema(DEFAULT_CATALOG.to_string(), DEFAULT_SCHEMA.to_string())
        .build();

    let factory = build_factory(env).await;
    let interpreter = factory.create(ctx, plan);
    interpreter.execute().await
}

async fn test_create_table<E, M>(env: &Env<E, M>)
where
    E: TableEngine + Clone + Send + Sync + 'static,
    M: MetaProvider,
{
    let sql="CREATE TABLE IF NOT EXISTS test_table(c1 string tag not null,ts timestamp not null, c3 string, timestamp key(ts),primary key(c1, ts)) \
        ENGINE=Analytic WITH (ttl='70d',update_mode='overwrite',arena_block_size='1KB')";

    let output = sql_to_output(env, sql).await.unwrap();
    if let Output::AffectedRows(v) = output {
        assert_eq!(v, 1);
    } else {
        panic!();
    }
}

async fn test_desc_table<E, M>(env: &Env<E, M>)
where
    E: TableEngine + Clone + Send + Sync + 'static,
    M: MetaProvider,
{
    let sql = "desc table test_table";
    let output = sql_to_output(env, sql).await.unwrap();
    if let Output::Records(v) = output {
        assert_eq!(v.len(), 1);
    } else {
        panic!();
    }
}

async fn test_exists_table<E, M>(env: &Env<E, M>)
where
    E: TableEngine + Clone + Send + Sync + 'static,
    M: MetaProvider,
{
    let sql = "exists table test_table";
    let output = sql_to_output(env, sql).await.unwrap();
    if let Output::Records(v) = output {
        assert_eq!(v.len(), 1);
    } else {
        panic!();
    }
}

async fn test_insert_table<E, M>(env: &Env<E, M>)
where
    E: TableEngine + Clone + Send + Sync + 'static,
    M: MetaProvider,
{
    let sql = "INSERT INTO test_table(key1, key2, field1,field2) VALUES('tagk', 1638428434000,100, 'hello3'),('tagk2', 1638428434000,100, 'hello3');";
    let output = sql_to_output(env, sql).await.unwrap();
    if let Output::AffectedRows(v) = output {
        assert_eq!(v, 2);
    } else {
        panic!();
    }
}

async fn test_select_table<E, M>(env: &Env<E, M>)
where
    E: TableEngine + Clone + Send + Sync + 'static,
    M: MetaProvider,
{
    let sql = "select * from test_table";
    let output = sql_to_output(env, sql).await.unwrap();
    if let Output::Records(v) = output {
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].num_rows(), 2);
    } else {
        panic!();
    }

    let sql = "select count(*) from test_table";
    let output = sql_to_output(env, sql).await.unwrap();
    if let Output::Records(v) = output {
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].num_rows(), 1);
    } else {
        panic!();
    }
}

async fn test_show_create_table<E, M>(env: &Env<E, M>)
where
    E: TableEngine + Clone + Send + Sync + 'static,
    M: MetaProvider,
{
    let sql = "show create table test_table";
    let output = sql_to_output(env, sql).await.unwrap();
    if let Output::Records(v) = output {
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].num_rows(), 1);
    } else {
        panic!();
    }
}

async fn test_alter_table<E, M>(env: &Env<E, M>)
where
    E: TableEngine + Clone + Send + Sync + 'static,
    M: MetaProvider,
{
    let sql = "alter table test_table add column add_col string";
    let output = sql_to_output(env, sql).await.unwrap();
    if let Output::AffectedRows(v) = output {
        assert_eq!(v, 1);
    } else {
        panic!();
    }

    let sql = "alter table test_table modify SETTING ttl='9d'";
    let output = sql_to_output(env, sql).await.unwrap();
    if let Output::AffectedRows(v) = output {
        assert_eq!(v, 1);
    } else {
        panic!();
    }
}

async fn test_drop_table<E, M>(env: &Env<E, M>)
where
    E: TableEngine + Clone + Send + Sync + 'static,
    M: MetaProvider,
{
    let sql = "drop table test_table";
    let output = sql_to_output(env, sql).await.unwrap();
    if let Output::AffectedRows(v) = output {
        assert_eq!(v, 1);
    } else {
        panic!();
    }
}

struct Env<E, M>
where
    E: TableEngine + Clone + Send + Sync + 'static,
    M: MetaProvider,
{
    pub engine: E,
    pub meta_provider: M,
}

impl<E, M> Env<E, M>
where
    E: TableEngine + Clone + Send + Sync + 'static,
    M: MetaProvider,
{
    fn engine(&self) -> E {
        self.engine.clone()
    }
}

#[tokio::test]
async fn test_interpreters() {
    let env = TestEnv::builder().build();
    let mut test_ctx = env.new_context();
    test_ctx.open().await;
    let mock = MockMetaProvider::default();
    let env = Env {
        engine: test_ctx.engine(),
        meta_provider: mock,
    };

    test_create_table(&env).await;
    test_desc_table(&env).await;
    test_exists_table(&env).await;
    test_insert_table(&env).await;
    test_select_table(&env).await;
    test_show_create_table(&env).await;
    test_alter_table(&env).await;
    test_drop_table(&env).await;
}
