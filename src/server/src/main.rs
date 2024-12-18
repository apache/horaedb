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
use std::{sync::Arc, time::Duration};

use actix_web::{
    get,
    web::{self, Data},
    App, HttpResponse, HttpServer, Responder,
};
use metric_engine::{
    storage::{CloudObjectStorage, CompactRequest, TimeMergeStorageRef},
    types::StorageOptions,
};
use object_store::local::LocalFileSystem;
use tracing::info;

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

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt::init();

    let port = 5000;
    let schema = todo!();
    let store = Arc::new(LocalFileSystem::new());
    let storage = Arc::new(
        CloudObjectStorage::try_new(
            "/tmp/test".to_string(),
            Duration::from_mins(10),
            store,
            schema,
            2,
            StorageOptions::default(),
        )
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
    .bind(("127.0.0.1", port))?
    .run()
    .await
}
