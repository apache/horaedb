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

//! evaluate the efficiency of the deadpool-backed object pool.

use std::fs;

use bytes::Bytes;
use remote_write::{pooled_parser::PooledParser, pooled_types::POOL};
use tikv_jemallocator::Jemalloc;
use tokio::task::JoinHandle;

#[global_allocator]
static ALLOC: Jemalloc = Jemalloc;

async fn run_concurrent_parsing(scale: usize) -> deadpool::Status {
    let data = fs::read("../remote_write/tests/workloads/1709380533560664458.data")
        .expect("test data load failed");
    let data = Bytes::from(data);

    let handles: Vec<JoinHandle<()>> = (0..scale)
        .map(|_| {
            let data = data.clone();
            tokio::spawn(async move {
                let parser = PooledParser;
                let _ = parser
                    .decode_async(data.clone())
                    .await
                    .expect("parse failed");
            })
        })
        .collect();

    for handle in handles {
        handle.await.expect("task completion failed");
    }

    POOL.status()
}

#[tokio::main]
async fn main() {
    let scale_values = [1, 2, 5, 10, 20, 50, 100, 200, 500];

    println!(
        "{:<8} {:<10} {:<10} {:<10} {:<10}",
        "Scale", "MaxSize", "PoolSize", "Available", "Waiting"
    );
    println!("{}", "=".repeat(50));

    for &scale in &scale_values {
        let status = run_concurrent_parsing(scale).await;

        println!(
            "{:<8} {:<10} {:<10} {:<10} {:<10}",
            scale, status.max_size, status.size, status.available, status.waiting
        );

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    println!("=== Final Pool Status ===");
    let final_status = POOL.status();
    println!("Max Pool Size: {}", final_status.max_size);
    println!("Current Pool Size: {}", final_status.size);
    println!("Available Objects: {}", final_status.available);
    println!("Waiting Requests: {}", final_status.waiting);
}
