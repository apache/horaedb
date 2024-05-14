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

//! The proxy module provides features such as forwarding and authentication,
//! adapts to different protocols.

use macros::define_result;
use serde::{Deserialize, Serialize};
use snafu::Snafu;

pub mod auth_with_file;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to open file, err:{}.", source))]
    OpenFile { source: std::io::Error },

    #[snafu(display("Failed to read line, err:{}.", source))]
    ReadLine { source: std::io::Error },

    #[snafu(display("File not existed, file path:{}", path))]
    FileNotExisted { path: String },
}

define_result!(Error);

/// Header of authorization
pub const AUTHORIZATION: &str = "authorization";

pub const DEFAULT_AUTH_TYPE: &str = "file";

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct Config {
    pub enable: bool,
    pub auth_type: String,
    pub source: String,
}
