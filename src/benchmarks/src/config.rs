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

use std::{env, fs};

use serde::Deserialize;
use tracing::info;

use crate::util::ReadableDuration;
const BENCH_CONFIG_PATH_KEY: &str = "BENCH_CONFIG_PATH";

#[derive(Debug, Deserialize)]
pub struct BenchConfig {
    pub manifest: ManifestConfig,
}

pub fn config_from_env() -> BenchConfig {
    let path = env::var(BENCH_CONFIG_PATH_KEY)
        .expect("Env {BENCH_CONFIG_PATH_KEY} is required to run benches");

    info!(config_path = ?path.as_str(), "Load bench config");

    let toml_str = fs::read_to_string(&path).expect("read bench config file failed");
    let config = toml::from_str(&toml_str).expect("parse bench config file failed");
    info!(config = ?config, "Bench config");
    config
}

#[derive(Deserialize, Debug)]
pub struct ManifestConfig {
    pub record_count: usize,
    pub append_count: usize,
    pub bench_measurement_time: ReadableDuration,
    pub bench_sample_size: usize,
}
