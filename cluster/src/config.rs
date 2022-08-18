// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use meta_client::{meta_impl::MetaClientConfig, types::NodeMetaInfo};
use serde_derive::Deserialize;

#[derive(Default, Clone, Deserialize, Debug)]
#[serde(default)]
pub struct ClusterConfig {
    pub node: NodeMetaInfo,
    pub cmd_channel_buffer_size: usize,
    pub meta_client: MetaClientConfig,
}
