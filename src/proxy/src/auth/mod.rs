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

use std::sync::{Arc, Mutex};

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

pub type AuthRef = Arc<Mutex<dyn Auth>>;

/// Header of tenant name
pub const TENANT_HEADER: &str = "x-horaedb-access-tenant";
/// Header of tenant name
pub const TENANT_TOKEN_HEADER: &str = "x-horaedb-access-token";

/// Admin tenant name
pub const ADMIN_TENANT: &str = "admin";

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct Config {
    pub enable: bool,
    pub auth_type: String,
    pub source: String,
}

pub trait Auth: Send + Sync {
    fn load_credential(&mut self) -> Result<()>;
    fn identify(&self, tenant: Option<String>, token: Option<String>) -> bool;
}

#[derive(Default)]
pub struct AuthBase;

impl Auth for AuthBase {
    fn load_credential(&mut self) -> Result<()> {
        Ok(())
    }

    fn identify(&self, _tenant: Option<String>, _token: Option<String>) -> bool {
        true
    }
}
