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

//! remote write parser bench.

use std::{fs, path::PathBuf, sync::Arc};

use bytes::Bytes;
use pb_types::WriteRequest as ProstWriteRequest;
use prost::Message;
use protobuf::Message as ProtobufMessage;
use quick_protobuf::{BytesReader, MessageRead};
use remote_write::pooled_parser::PooledParser;

use crate::{
    config::RemoteWriteConfig,
    quick_protobuf_remote_write::WriteRequest as QuickProtobufWriteRequest,
    rust_protobuf_remote_write::WriteRequest as RustProtobufWriteRequest,
    util::run_concurrent_threads,
};

pub struct RemoteWriteBench {
    raw_data: Vec<u8>,
}

impl RemoteWriteBench {
    pub fn new(config: RemoteWriteConfig) -> Self {
        let mut workload_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        workload_path.push(&config.workload_file);

        let raw_data = fs::read(&workload_path)
            .unwrap_or_else(|_| panic!("failed to read workload file: {:?}", workload_path));

        Self { raw_data }
    }

    // prost parser sequential bench.
    pub fn prost_parser_sequential(&self, scale: usize) -> Result<(), String> {
        for _ in 0..scale {
            let data = Bytes::from(self.raw_data.clone());
            ProstWriteRequest::decode(data)
                .map_err(|e| format!("prost sequential parse failed: {}", e))?;
        }
        Ok(())
    }

    // Hand-written pooled parser sequential bench.
    pub fn pooled_parser_sequential(&self, scale: usize) -> Result<(), String> {
        let parser = PooledParser;
        for _ in 0..scale {
            let data = Bytes::from(self.raw_data.clone());
            let _ = parser
                .decode(data.clone())
                .map_err(|e| format!("pooled sequential parse failed: {e:?}"))?;
        }
        Ok(())
    }

    // quick-protobuf parser sequential bench.
    pub fn quick_protobuf_parser_sequential(&self, scale: usize) -> Result<(), String> {
        for _ in 0..scale {
            let mut reader = BytesReader::from_bytes(&self.raw_data);
            QuickProtobufWriteRequest::from_reader(&mut reader, &self.raw_data)
                .map_err(|e| format!("quick-protobuf sequential parse failed: {}", e))?;
        }
        Ok(())
    }

    // rust-protobuf parser sequential bench.
    pub fn rust_protobuf_parser_sequential(&self, scale: usize) -> Result<(), String> {
        for _ in 0..scale {
            RustProtobufWriteRequest::parse_from_bytes(&self.raw_data)
                .map_err(|e| format!("rust-protobuf sequential parse failed: {}", e))?;
        }
        Ok(())
    }

    // prost parser concurrent bench.
    pub fn prost_parser_concurrent(&self, scale: usize) -> Result<(), String> {
        let raw = Arc::new(self.raw_data.clone());
        run_concurrent_threads(scale, move |n| {
            for _ in 0..n {
                let data = Bytes::from((*raw).clone());
                ProstWriteRequest::decode(data)
                    .map_err(|e| format!("prost concurrent parse failed: {}", e))?;
            }
            Ok(())
        })
    }

    // Hand-written pooled parser concurrent bench.
    pub fn pooled_parser_concurrent(&self, scale: usize) -> Result<(), String> {
        let raw = Arc::new(self.raw_data.clone());
        run_concurrent_threads(scale, move |n| {
            let parser = PooledParser;
            for _ in 0..n {
                let data = Bytes::from((*raw).clone());
                let _ = parser
                    .decode(data.clone())
                    .map_err(|e| format!("pooled concurrent parse failed: {e:?}"))?;
            }
            Ok(())
        })
    }

    // quick-protobuf parser concurrent bench.
    pub fn quick_protobuf_parser_concurrent(&self, scale: usize) -> Result<(), String> {
        let raw = Arc::new(self.raw_data.clone());
        run_concurrent_threads(scale, move |n| {
            for _ in 0..n {
                let mut reader: BytesReader = BytesReader::from_bytes(&raw);
                QuickProtobufWriteRequest::from_reader(&mut reader, &raw)
                    .map_err(|e| format!("quick-protobuf concurrent parse failed: {}", e))?;
            }
            Ok(())
        })
    }

    // rust-protobuf parser concurrent bench.
    pub fn rust_protobuf_parser_concurrent(&self, scale: usize) -> Result<(), String> {
        let raw = Arc::new(self.raw_data.clone());
        run_concurrent_threads(scale, move |n| {
            for _ in 0..n {
                RustProtobufWriteRequest::parse_from_bytes(&raw)
                    .map_err(|e| format!("rust-protobuf concurrent parse failed: {}", e))?;
            }
            Ok(())
        })
    }
}
