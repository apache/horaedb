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

#![feature(let_chains)]

use std::{env, fmt::Display, path::Path};

use anyhow::Result;
use async_trait::async_trait;
use database::{Backend, CeresDB};
use sqlness::{Database, EnvController, QueryContext, Runner};

use crate::database::{CeresDBCluster, CeresDBServer};

mod database;

const CASE_ROOT_PATH_ENV: &str = "CERESDB_TEST_CASE_PATH";
const ENV_FILTER_ENV: &str = "CERESDB_ENV_FILTER";

struct CeresDBController;
struct UntypedCeresDB {
    db: DbRef,
}

pub trait StoppableDatabase: Database {
    fn stop(&mut self);
}

pub type DbRef = Box<dyn StoppableDatabase + Send + Sync>;

impl<T: Backend + Send + Sync> StoppableDatabase for CeresDB<T> {
    fn stop(&mut self) {
        self.stop();
    }
}

#[async_trait]
impl Database for UntypedCeresDB {
    async fn query(&self, context: QueryContext, query: String) -> Box<dyn Display> {
        self.db.query(context, query).await
    }
}

#[async_trait]
impl EnvController for CeresDBController {
    type DB = UntypedCeresDB;

    async fn start(&self, env: &str, _config: Option<&Path>) -> Self::DB {
        println!("start with env {env}");
        let db = match env {
            "local" => Box::new(CeresDB::<CeresDBServer>::create()) as DbRef,
            "cluster" => Box::new(CeresDB::<CeresDBCluster>::create()) as DbRef,
            _ => panic!("invalid env {env}"),
        };

        UntypedCeresDB { db }
    }

    async fn stop(&self, env: &str, mut database: Self::DB) {
        println!("stop with env {env}");
        database.db.stop();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let case_dir = env::var(CASE_ROOT_PATH_ENV)?;
    let env_filter = env::var(ENV_FILTER_ENV).unwrap_or_else(|_| ".*".to_string());
    let controller = CeresDBController;
    let config = sqlness::ConfigBuilder::default()
        .case_dir(case_dir)
        .env_filter(env_filter)
        .follow_links(true)
        .build()?;
    let runner = Runner::new_with_config(config, controller).await?;
    runner.run().await?;

    Ok(())
}
