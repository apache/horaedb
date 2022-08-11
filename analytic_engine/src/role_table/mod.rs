use std::sync::{atomic::AtomicU8, Arc};

use async_trait::async_trait;
use common_util::define_result;
use snafu::{ensure, Snafu};
use table_engine::{
    stream::PartitionedStreams,
    table::{ReadRequest, WriteRequest},
};

use crate::{
    instance::{
        flush_compaction::{TableFlushOptions, TableFlushPolicy, TableFlushRequest},
        write::EncodeContext,
        write_worker::{BackgroundStatus, WorkerLocal},
    },
    table::data::{TableData, TableDataRef},
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

    async fn read(&self, request: ReadRequest) -> Result<PartitionedStreams>;

    async fn flush(&self, flush_opts: TableFlushOptions) -> Result<()>;

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

impl LeaderTableInner {
    const ROLE: u8 = TableRole::Leader as u8;

    // this method is intend to be called by WriteWorker
    async fn write(&self, request: WriteRequest) -> Result<usize> {
        let mut encode_ctx = EncodeContext::new(request.row_group);

        todo!()
    }

    async fn preprocess_write(
        &self,
        worker_local: &mut WorkerLocal,
        encode_ctx: &mut EncodeContext,
    ) -> Result<()> {
        // Check schema compatibility
        self.table_data
            .schema()
            .compatible_for_write(
                encode_ctx.row_group.schema(),
                &mut encode_ctx.index_in_writer,
            )
            .unwrap();

        // TODO(yingwen): Allow write and retry flush.
        // Check background status, if background error occured, not allow to write
        // again.
        match &*worker_local.background_status() {
            // Compaction error is ignored now.
            BackgroundStatus::Ok | BackgroundStatus::CompactionFailed(_) => (),
            BackgroundStatus::FlushFailed(e) => {
                // return BackgroundFlushFailed { msg: e.to_string() }.fail();
                todo!()
            }
        }

        todo!()
    }

    // this method is intend to be called by WriteWorker
    async fn flush(&self, flush_opts: TableFlushOptions) -> Result<()> {
        let flush_req = TableFlushRequest {
            table_data: self.table_data.clone(),
            max_sequence: self.table_data.last_sequence(),
            policy: TableFlushPolicy::Dump,
        };

        todo!("call flush_memtables")
    }
}
