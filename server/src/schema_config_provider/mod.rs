// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Schema configuration can be retrieved from the the [`SchemaConfigProvider`].

use std::sync::Arc;

use cluster::topology::SchemaConfig;
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
