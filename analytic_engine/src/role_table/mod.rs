use std::sync::{atomic::AtomicU8, Arc};

use async_trait::async_trait;
use common_util::define_result;
use snafu::Snafu;
use table_engine::table::{AlterSchemaRequest, WriteRequest};

use crate::{
    instance::{
        alter::TableAlterSchemaPolicy,
        flush_compaction::{TableFlushOptions, TableFlushPolicy},
        write::TableWritePolicy,
        write_worker::WorkerLocal,
        Instance,
    },
    space::SpaceRef,
    table::data::TableDataRef,
};

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

    // async fn read(&self, request: ReadRequest) -> Result<PartitionedStreams>;

    async fn flush(&self, flush_opts: TableFlushOptions) -> Result<()>;

    async fn alter(&self) -> Result<()>;

    // async fn alter

    async fn change_role(&self) -> Result<()>;
}

#[repr(u8)]
pub enum TableRole {
    Invalid = 0,
    Leader = 1,
    InSync = 2,
    NoSync = 3,
}

pub struct LeaderTable {
    inner: Arc<LeaderTableInner>,
}

impl Drop for LeaderTable {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) == 1 {
            todo!("notify the state is completely changed")
        }
    }
}

struct LeaderTableInner {
    state: AtomicU8,
    table_data: TableDataRef,
}

// todo: handle `Result`
impl LeaderTableInner {
    const ROLE: u8 = TableRole::Leader as u8;

    /// This method is expected to be called by [Instance]
    async fn write(
        &self,
        request: WriteRequest,
        instance: &Arc<Instance>,
        space: &SpaceRef,
        worker_local: &mut WorkerLocal,
    ) -> Result<usize> {
        let res = instance
            .process_write_table_command(
                worker_local,
                space,
                &self.table_data,
                request,
                TableWritePolicy::Full,
            )
            .await
            .unwrap();

        Ok(res)
    }

    /// This method is expected to be called by [Instance]
    async fn flush(
        &self,
        mut flush_opts: TableFlushOptions,
        instance: &Arc<Instance>,
        worker_local: &mut WorkerLocal,
    ) -> Result<()> {
        // Leader Table will dump memtable to storage.
        flush_opts.policy = TableFlushPolicy::Dump;

        let res = instance
            .flush_table_in_worker(worker_local, &self.table_data, flush_opts)
            .await
            .unwrap();

        Ok(res)
    }

    /// This method is expected to be called by [Instance]
    async fn alter_schema(
        &self,
        instance: &Arc<Instance>,
        worker_local: &mut WorkerLocal,
        request: AlterSchemaRequest,
    ) -> Result<()> {
        let res = instance
            .process_alter_schema_command(
                worker_local,
                &self.table_data,
                request,
                TableAlterSchemaPolicy::Alter,
            )
            .await
            .unwrap();

        Ok(res)
    }
}
