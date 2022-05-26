// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use meta_client_v2::MetaClientConfig;
use serde_derive::Deserialize;

#[derive(Default, Clone, Deserialize, Debug)]
pub struct ClusterConfig {
    /// Local ip address of this node, used as endpoint ip in meta.
    pub node: String,
    /// Grpc port of this node, also used as endpoint port in meta.
    pub port: u16,
    pub zone: String,
    pub idc: String,
    pub binary_version: String,
    pub cmd_channel_buffer_size: usize,

    pub meta_client_config: MetaClientConfig,
}
