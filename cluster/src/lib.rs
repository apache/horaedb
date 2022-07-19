use std::sync::Arc;

use async_trait::async_trait;
use common_util::define_result;
pub use meta_client_v2::{
    AllocSchemaIdRequest, AllocSchemaIdResponse, AllocTableIdRequest, AllocTableIdResponse,
    DropTableRequest, DropTableResponse, GetTablesRequest,
};
use meta_client_v2::{ShardId, TableId};
use snafu::{Backtrace, Snafu};

pub mod config;
pub mod meta_impl;
mod table_manager;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(display("Build meta client failed, err:{}.", source))]
    BuildMetaClient { source: meta_client_v2::Error },

    #[snafu(display("Meta client start failed, err:{}.", source))]
    StartMetaClient { source: meta_client_v2::Error },

    #[snafu(display("Meta client execute failed, err:{}.", source))]
    MetaClientFailure { source: meta_client_v2::Error },

    #[snafu(display(
        "Shard not found in current node, shard_id:{}.\nBacktrace:\n{}",
        shard_id,
        backtrace
    ))]
    ShardNotFound {
        shard_id: ShardId,
        backtrace: Backtrace,
    },
}

define_result!(Error);

pub type ClusterRef = Arc<dyn Cluster + Send + Sync>;

pub type TableManipulatorRef = Arc<dyn TableManipulator + Send + Sync>;

#[async_trait]
pub trait TableManipulator {
    async fn open_table(
        &self,
        schema_name: &str,
        table_name: &str,
        table_id: TableId,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn close_table(
        &self,
        schema_name: &str,
        table_name: &str,
        table_id: TableId,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

// TODO: add more methods
/// Cluster manages tables and shard infos in cluster mode.
#[async_trait]
pub trait Cluster {
    async fn start(&self) -> Result<()>;
}
