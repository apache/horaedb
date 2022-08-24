// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Schema provider based on cluster.

#![allow(dead_code)]

use cluster::ClusterRef;

use crate::schema_config_provider::{Result, SchemaConfigProvider};

pub struct ClusterBasedProvider {
    cluster: ClusterRef,
}

impl ClusterBasedProvider {
    pub fn new(cluster: ClusterRef) -> Self {
        Self { cluster }
    }
}

impl SchemaConfigProvider for ClusterBasedProvider {
    fn schema_config(&self, _schema_name: &str) -> Result<Option<&cluster::SchemaConfig>> {
        todo!()
    }
}
