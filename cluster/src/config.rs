// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use common_types::schema::TIMESTAMP_COLUMN;
use meta_client::meta_impl::MetaClientConfig;
use serde::Deserialize;
use table_engine::ANALYTIC_ENGINE_TYPE;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SchemaConfig {
    pub auto_create_tables: bool,
    pub default_engine_type: String,
    pub default_timestamp_column_name: String,
}

impl Default for SchemaConfig {
    fn default() -> Self {
        Self {
            auto_create_tables: false,
            default_engine_type: ANALYTIC_ENGINE_TYPE.to_string(),
            default_timestamp_column_name: TIMESTAMP_COLUMN.to_string(),
        }
    }
}

#[derive(Default, Clone, Deserialize, Debug)]
#[serde(default)]
pub struct ClusterConfig {
    pub cmd_channel_buffer_size: usize,
    pub meta_client: MetaClientConfig,
}
