// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// The schema config provider based on configs.

use std::collections::HashMap;

use cluster::SchemaConfig;

use crate::schema_config_provider::{Result, SchemaConfigProvider};

pub type SchemaConfigs = HashMap<String, SchemaConfig>;

/// Provide schema config according to the given config.
#[derive(Debug)]
pub struct ConfigBasedProvider {
    schema_configs: SchemaConfigs,
}

impl ConfigBasedProvider {
    pub fn new(schema_configs: SchemaConfigs) -> Self {
        Self { schema_configs }
    }
}

impl SchemaConfigProvider for ConfigBasedProvider {
    fn schema_config(&self, schema_name: &str) -> Result<Option<&SchemaConfig>> {
        Ok(self.schema_configs.get(schema_name))
    }
}
