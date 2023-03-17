// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.
#![feature(let_chains)]

use std::{env, fmt::Display, path::Path};

use anyhow::Result;
use async_trait::async_trait;
use database::{Backend, CeresDB};
use sqlness::{Database, EnvController, QueryContext, Runner};

use crate::database::{CeresDBCluster, CeresDBServer};

mod database;

const CASE_ROOT_PATH_ENV: &str = "CERESDB_TEST_CASE_PATH";

struct CeresDBController;
pub trait StoppableDatabase: Database {
    fn stop(&mut self);
}

impl<T: Backend + Send + Sync> StoppableDatabase for CeresDB<T> {
    fn stop(&mut self) {
        self.stop();
    }
}

pub type DbRef = Box<dyn StoppableDatabase + Send + Sync>;

#[async_trait]
impl Database for DbRef {
    async fn query(&self, context: QueryContext, query: String) -> Box<dyn Display> {
        self.query(context, query).await
    }
}

#[async_trait]
impl EnvController for CeresDBController {
    type DB = DbRef;

    async fn start(&self, env: &str, _config: Option<&Path>) -> Self::DB {
        println!("start with env {env}");
        match env {
            "local" => Box::new(CeresDB::<CeresDBServer>::create()) as DbRef,
            "cluster" => Box::new(CeresDB::<CeresDBCluster>::create()) as DbRef,
            _ => panic!("invalid env {env}"),
        }
    }

    async fn stop(&self, env: &str, mut database: Self::DB) {
        println!("stop with env {env}");
        database.stop();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let case_dir = env::var(CASE_ROOT_PATH_ENV)?;
    let env = CeresDBController;
    let config = sqlness::ConfigBuilder::default()
        .case_dir(case_dir)
        .follow_links(true)
        .build()?;
    let runner = Runner::new_with_config(config, env).await?;
    runner.run().await?;

    Ok(())
}
