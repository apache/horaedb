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

// The schema config provider based on configs.

use std::collections::HashMap;

use cluster::config::SchemaConfig;

use crate::schema_config_provider::{Result, SchemaConfigProvider};

pub type SchemaConfigs = HashMap<String, SchemaConfig>;

/// Provide schema config according to the given config.
#[derive(Debug)]
pub struct ConfigBasedProvider {
    schema_configs: SchemaConfigs,
    default: SchemaConfig,
}

impl ConfigBasedProvider {
    pub fn new(schema_configs: SchemaConfigs, default: SchemaConfig) -> Self {
        Self {
            schema_configs,
            default,
        }
    }
}

impl SchemaConfigProvider for ConfigBasedProvider {
    fn schema_config(&self, schema_name: &str) -> Result<Option<&SchemaConfig>> {
        Ok(Some(
            self.schema_configs
                .get(schema_name)
                .unwrap_or(&self.default),
        ))
    }
}
