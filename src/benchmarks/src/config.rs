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

//! Benchmark configs.

use std::{env, fs::File, io::Read};

use serde::Deserialize;

use crate::util::ReadableDuration;
const BENCH_CONFIG_PATH_KEY: &str = "BENCH_CONFIG_PATH";

#[derive(Deserialize)]
pub struct BenchConfig {
    pub encoding_bench: EncodingConfig,
}

pub fn bench_config_from_env() -> BenchConfig {
    let path = env::var(BENCH_CONFIG_PATH_KEY).unwrap_or_else(|e| {
        panic!("Env {BENCH_CONFIG_PATH_KEY} is required to run benches, err:{e}.")
    });

    let toml_buf = {
        let mut file = File::open(&path).unwrap_or_else(|_| panic!("open {path} failed"));
        let mut buf = String::new();
        file.read_to_string(&mut buf)
            .unwrap_or_else(|_| panic!("read {path} failed"));
        buf
    };

    toml::from_str(&toml_buf).unwrap_or_else(|e| panic!("parse {path} failed: {e}"))
}

#[derive(Deserialize)]
pub struct EncodingConfig {
    pub record_count: usize,
    pub append_count: usize,
    pub bench_measurement_time: ReadableDuration,
    pub bench_sample_size: usize,
}
