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

use benchmarks::util::{MemoryBenchConfig, MemoryStats};
use pb_types::WriteRequest as ProstWriteRequest;
use prost::Message;
use protobuf::Message as ProtobufMessage;
use quick_protobuf::{BytesReader, MessageRead};
use remote_write::pooled_parser::PooledParser;
use tikv_jemallocator::Jemalloc;

#[global_allocator]
static ALLOC: Jemalloc = Jemalloc;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = MemoryBenchConfig::from_args();
    let args: Vec<String> = std::env::args().collect();
    let parser = args.get(3).map(|s| s.as_str()).unwrap_or("pooled");

    let start_stats = MemoryStats::collect()?;

    match config.mode.as_str() {
        "sequential" => match parser {
            "pooled" => {
                let parser = PooledParser;
                for _ in 0..config.scale {
                    let _ = parser.decode_async(config.test_data.clone()).await?;
                }
            }
            "prost" => {
                for _ in 0..config.scale {
                    ProstWriteRequest::decode(config.test_data.clone())?;
                }
            }
            "rust-protobuf" => {
                for _ in 0..config.scale {
                    let _ = benchmarks::rust_protobuf_remote_write::WriteRequest::parse_from_bytes(
                        &config.test_data,
                    )?;
                }
            }
            "quick-protobuf" => {
                for _ in 0..config.scale {
                    let mut reader = BytesReader::from_bytes(&config.test_data);
                    let _ = benchmarks::quick_protobuf_remote_write::WriteRequest::from_reader(
                        &mut reader,
                        &config.test_data,
                    )?;
                }
            }
            other => panic!("unknown parser: {}", other),
        },
        "concurrent" => match parser {
            "pooled" => {
                let mut handles = Vec::new();
                for _ in 0..config.scale {
                    let data_clone = config.test_data.clone();
                    let handle = tokio::spawn(async move {
                        let parser = PooledParser;
                        let _ = parser.decode_async(data_clone).await;
                    });
                    handles.push(handle);
                }
                for handle in handles {
                    handle.await?;
                }
            }
            "prost" => {
                let mut handles = Vec::new();
                for _ in 0..config.scale {
                    let data_clone = config.test_data.clone();
                    let handle = tokio::spawn(async move {
                        let _ = ProstWriteRequest::decode(data_clone);
                    });
                    handles.push(handle);
                }
                for handle in handles {
                    handle.await?;
                }
            }
            "rust-protobuf" => {
                let mut handles = Vec::new();
                for _ in 0..config.scale {
                    let data_clone = config.test_data.clone();
                    let handle = tokio::spawn(async move {
                        let _ =
                            benchmarks::rust_protobuf_remote_write::WriteRequest::parse_from_bytes(
                                &data_clone,
                            );
                    });
                    handles.push(handle);
                }
                for handle in handles {
                    handle.await?;
                }
            }
            "quick-protobuf" => {
                let mut handles = Vec::new();
                for _ in 0..config.scale {
                    let data_clone = config.test_data.clone();
                    let handle = tokio::spawn(async move {
                        let mut reader = BytesReader::from_bytes(&data_clone);
                        let _ = benchmarks::quick_protobuf_remote_write::WriteRequest::from_reader(
                            &mut reader,
                            &data_clone,
                        );
                    });
                    handles.push(handle);
                }
                for handle in handles {
                    handle.await?;
                }
            }
            other => panic!("unknown parser: {}", other),
        },
        _ => panic!("invalid mode"),
    }

    let end_stats = MemoryStats::collect()?;
    let memory_diff = start_stats.diff(&end_stats);
    config.output_json(&memory_diff);
    Ok(())
}
