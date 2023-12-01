// Copyright 2023 The HoraeDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Catalog constants

// TODO: rename ceresdb to horaedb is a breaking change, we should config it in the config file.
/// Default catalog name
pub const DEFAULT_CATALOG: &str = "ceresdb";
/// Default schema name
pub const DEFAULT_SCHEMA: &str = "public";
/// Catalog name of the sys catalog
pub const SYSTEM_CATALOG: &str = "system";
/// Schema name of the sys catalog
pub const SYSTEM_CATALOG_SCHEMA: &str = "public";
