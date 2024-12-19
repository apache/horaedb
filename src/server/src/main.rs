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

#![feature(duration_constructors)]
mod config;
use std::{fs, sync::Arc, time::Duration};

use actix_web::{
    get,
    web::{self, Data},
    App, HttpResponse, HttpServer, Responder,
};
use arrow::datatypes::{DataType, Field, Schema};
use clap::Parser;
use config::{Config, StorageConfig};
use metric_engine::{
    storage::{CloudObjectStorage, CompactRequest, StorageRuntimes, TimeMergeStorageRef},
    types::{RuntimeRef, StorageOptions},
};
use object_store::local::LocalFileSystem;
use tracing::info;

#[derive(Parser, Debug)]
#[command(version, about, long_about)]
struct Args {
    /// Config file path
    #[arg(short, long)]
    config: String,
}

#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Hello world!")
}

#[get("/compact")]
async fn compact(data: web::Data<AppState>) -> impl Responder {
    if let Err(e) = data.storage.compact(CompactRequest::default()).await {
        println!("compact failed, err:{e}");
    }
    HttpResponse::Ok().body("Task submit!")
}

struct AppState {
    storage: TimeMergeStorageRef,
}

pub fn main() {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let config_body = fs::read_to_string(args.config).expect("read config file failed");
    let config: Config = toml::from_str(&config_body).unwrap();
    info!(config = ?config, "Config loaded");

    let port = config.port;
    let schema = Arc::new(Schema::new(vec![
        Field::new("pk1", DataType::Int64, true),
        Field::new("pk2", DataType::Int64, true),
        Field::new("pk3", DataType::Int64, true),
        Field::new("value", DataType::Int64, true),
    ]));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build main runtime");
    let manifest_compact_runtime = build_multi_runtime(
        "manifest-compact",
        config.metric_engine.manifest.background_thread_num,
    );
    let sst_compact_runtime = build_multi_runtime(
        "sst-compact",
        config.metric_engine.sst.background_thread_num,
    );
    let runtimes = StorageRuntimes::new(manifest_compact_runtime, sst_compact_runtime);
    let storage_config = match config.metric_engine.storage {
        StorageConfig::Local(v) => v,
        StorageConfig::S3Like(_) => panic!("S3 not support yet"),
    };
    let _ = rt.block_on(async move {
        let store = Arc::new(LocalFileSystem::new());
        let storage = Arc::new(
            CloudObjectStorage::try_new(
                storage_config.data_dir,
                Duration::from_mins(10),
                store,
                schema,
                3,
                StorageOptions::default(),
                runtimes,
            )
            .await
            .unwrap(),
        );
        let app_state = Data::new(AppState { storage });
        info!(port, "Start HoraeDB http server...");
        HttpServer::new(move || {
            App::new()
                .app_data(app_state.clone())
                .service(hello)
                .service(compact)
        })
        .workers(4)
        .bind(("127.0.0.1", port))
        .expect("Server bind failed")
        .run()
        .await
    });
}

fn build_multi_runtime(name: &str, workers: usize) -> RuntimeRef {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .thread_name(name)
        .worker_threads(workers)
        .enable_all()
        .build()
        .expect("build tokio runtime");

    Arc::new(rt)
}
