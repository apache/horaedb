// Copyright 2023 The CeresDB Authors
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

//! Schema configuration can be retrieved from the the [`SchemaConfigProvider`].

use std::sync::Arc;

use cluster::config::SchemaConfig;
use macros::define_result;
use snafu::Snafu;

pub mod cluster_based;
pub mod config_based;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {}

define_result!(Error);

pub type SchemaConfigProviderRef = Arc<dyn SchemaConfigProvider + Send + Sync>;

pub trait SchemaConfigProvider {
    fn schema_config(&self, schema_name: &str) -> Result<Option<&SchemaConfig>>;
}
