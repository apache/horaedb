// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Schema provider based on cluster.

use cluster::{config::SchemaConfig, ClusterRef};

use crate::schema_config_provider::{Result, SchemaConfigProvider};

pub struct ClusterBasedProvider {
    #[allow(dead_code)]
    cluster: ClusterRef,
    default_schema_config: SchemaConfig,
}

impl ClusterBasedProvider {
    pub fn new(cluster: ClusterRef) -> Self {
        Self {
            cluster,
            default_schema_config: Default::default(),
        }
    }
}

impl SchemaConfigProvider for ClusterBasedProvider {
    fn schema_config(&self, _schema_name: &str) -> Result<Option<&SchemaConfig>> {
        // FIXME: Fetch the schema config from the cluster rather than the hard-coded
        // default_schema_config.
        Ok(Some(&self.default_schema_config))
    }
}
