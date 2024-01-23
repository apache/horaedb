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

//! Catalog constants

use lazy_static::lazy_static;

/// Default schema name
pub const DEFAULT_SCHEMA: &str = "public";
/// Catalog name of the sys catalog
pub const SYSTEM_CATALOG: &str = "system";
/// Schema name of the sys catalog
pub const SYSTEM_CATALOG_SCHEMA: &str = "public";

lazy_static! {
    /// Default catalog name
    pub static ref DEFAULT_CATALOG: String =
        std::env::var("HORAEDB_DEFAULT_CATALOG").unwrap_or_else(|_| "horaedb".to_string());
}
