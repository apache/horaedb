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
use std::{fs, iter::repeat_with, sync::Arc, time::Duration};

use actix_web::{
    get,
    web::{self, Data},
    App, HttpResponse, HttpServer, Responder,
};
use arrow::{
    array::{Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use clap::Parser;
use config::{Config, StorageConfig};
use metric_engine::{
    config::StorageOptions,
    storage::{
        CloudObjectStorage, CompactRequest, StorageRuntimes, TimeMergeStorageRef, WriteRequest,
    },
    types::RuntimeRef,
};
use object_store::local::LocalFileSystem;
use tracing::{error, info};

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
    tracing_subscriber::fmt()
        .with_file(true)
        .with_line_number(true)
        .with_target(false)
        .with_timer(tracing_subscriber::fmt::time::LocalTime::rfc_3339())
        .init();

    let args = Args::parse();
    let config_body = fs::read_to_string(args.config).expect("read config file failed");
    let config: Config = toml::from_str(&config_body).unwrap();
    info!(config = ?config, "Config loaded");

    let port = config.port;
    let rt = build_multi_runtime("main", 1);
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
    let write_worker_num = config.test.write_worker_num;
    let enable_write = config.test.enable_write;
    let write_rt = build_multi_runtime("write", write_worker_num);
    let _ = rt.block_on(async move {
        let store = Arc::new(LocalFileSystem::new());
        let storage = Arc::new(
            CloudObjectStorage::try_new(
                storage_config.data_dir,
                Duration::from_mins(10),
                store,
                build_schema(),
                3,
                StorageOptions::default(),
                runtimes,
            )
            .await
            .unwrap(),
        );

        if enable_write {
            bench_write(write_rt.clone(), write_worker_num, storage.clone());
        }

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

fn build_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("pk1", DataType::Int64, true),
        Field::new("pk2", DataType::Int64, true),
        Field::new("pk3", DataType::Int64, true),
        Field::new("value", DataType::Int64, true),
    ]))
}

fn bench_write(rt: RuntimeRef, workers: usize, storage: TimeMergeStorageRef) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("pk1", DataType::Int64, true),
        Field::new("pk2", DataType::Int64, true),
        Field::new("pk3", DataType::Int64, true),
        Field::new("value", DataType::Int64, true),
    ]));
    for _ in 0..workers {
        let storage = storage.clone();
        let schema = schema.clone();
        rt.spawn(async move {
            loop {
                let pk1: Int64Array = repeat_with(rand::random::<i64>).take(1000).collect();
                let pk2: Int64Array = repeat_with(rand::random::<i64>).take(1000).collect();
                let pk3: Int64Array = repeat_with(rand::random::<i64>).take(1000).collect();
                let value: Int64Array = repeat_with(rand::random::<i64>).take(1000).collect();
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(pk1), Arc::new(pk2), Arc::new(pk3), Arc::new(value)],
                )
                .unwrap();
                let now = common::now();
                if let Err(e) = storage
                    .write(WriteRequest {
                        batch,
                        enable_check: false,
                        time_range: (now..now + 1).into(),
                    })
                    .await
                {
                    error!("write failed, err:{}", e);
                }
                tokio::time::sleep(Duration::from_millis(1000)).await;
            }
        });
    }
}
