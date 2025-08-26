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

//! quick-protobuf parser memory bench.

use benchmarks::{
    quick_protobuf_remote_write::WriteRequest,
    util::{MemoryBenchConfig, MemoryStats},
};
use quick_protobuf::{BytesReader, MessageRead};
use tikv_jemallocator::Jemalloc;

#[global_allocator]
static ALLOC: Jemalloc = Jemalloc;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = MemoryBenchConfig::from_args();
    let start_stats = MemoryStats::collect()?;

    match config.mode.as_str() {
        "sequential" => {
            for _ in 0..config.scale {
                let mut reader = BytesReader::from_bytes(&config.test_data);
                WriteRequest::from_reader(&mut reader, &config.test_data)?;
            }
        }
        "concurrent" => {
            let mut handles = Vec::new();
            for _ in 0..config.scale {
                let data_clone = config.test_data.clone();
                let handle = tokio::spawn(async move {
                    let mut reader = BytesReader::from_bytes(&data_clone);
                    let _ = WriteRequest::from_reader(&mut reader, &data_clone);
                });
                handles.push(handle);
            }
            for handle in handles {
                handle.await?;
            }
        }
        _ => panic!("invalid mode"),
    }

    let end_stats = MemoryStats::collect()?;
    let memory_diff = start_stats.diff(&end_stats);
    config.output_json(&memory_diff);
    Ok(())
}
