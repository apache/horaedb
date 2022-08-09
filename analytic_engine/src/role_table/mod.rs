use async_trait::async_trait;
use common_util::define_result;
use snafu::Snafu;
use table_engine::{
    stream::PartitionedStreams,
    table::{ReadRequest, WriteRequest},
};

use crate::instance::flush_compaction::TableFlushOptions;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to write table, err:{}", source))]
    WriteTable {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

define_result!(Error);

#[async_trait]
pub trait RoleTable {
    fn check_state(&self) -> bool;

    async fn close(&self) -> Result<()>;

    async fn write(&self, request: WriteRequest) -> Result<usize>;

    async fn read(&self, request: ReadRequest) -> Result<PartitionedStreams>;

    async fn flush(&self, flush_opts: TableFlushOptions) -> Result<()>;

    // async fn alter

    async fn change_role(&self) -> Result<()>;
}
