use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use snafu::ResultExt;
use table_engine::{
    stream::PartitionedStreams,
    table::{AlterSchemaRequest, ReadRequest, WriteRequest},
};

use crate::{
    instance::{
        alter::TableAlterPolicy,
        flush_compaction::{TableFlushOptions, TableFlushPolicy},
        write::TableWritePolicy,
        Instance, InstanceRef,
    },
    role_table::{AlterTable, Result, RoleTable, RoleTableRef, TableRole},
    table::data::TableDataRef,
};

pub struct LeaderTable {
    inner: Arc<LeaderTableInner>,
}

impl LeaderTable {
    pub fn open(table_data: TableDataRef) -> RoleTableRef {
        let inner = Arc::new(LeaderTableInner {
            state: AtomicU8::new(LeaderTableInner::ROLE),
            table_data,
        });
        Arc::new(Self { inner }) as _
    }
}

impl std::fmt::Debug for LeaderTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaderTable")
            .field("table_id", &self.inner.table_data.id)
            .finish()
    }
}

impl Drop for LeaderTable {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) == 1 {
            // TODO: notify the state is completely changed
        }
    }
}

struct LeaderTableInner {
    state: AtomicU8,
    table_data: TableDataRef,
}

// TODO: handle `Result`
impl LeaderTableInner {
    const ROLE: u8 = TableRole::Leader as u8;

    fn check_state(&self) -> bool {
        self.state.load(Ordering::Relaxed) == Self::ROLE
    }

    async fn change_role(&self) -> Result<()> {
        todo!()
    }

    async fn write(&self, instance: &InstanceRef, request: WriteRequest) -> Result<usize> {
        // Leader table should write to both WAL and memtable
        let policy = TableWritePolicy::Full;

        let res = instance
            .write_to_table(self.table_data.clone(), request, policy)
            .await
            .unwrap();

        Ok(res)
    }

    async fn read(
        &self,
        instance: &InstanceRef,
        request: ReadRequest,
    ) -> Result<PartitionedStreams> {
        let res = instance
            .partitioned_read_from_table(&self.table_data, request)
            .await
            .unwrap();

        Ok(res)
    }

    async fn flush(
        &self,
        instance: &Arc<Instance>,
        mut flush_opts: TableFlushOptions,
    ) -> Result<()> {
        // Leader Table will dump memtable to storage.
        flush_opts.policy = TableFlushPolicy::Dump;

        instance
            .flush_table(&self.table_data, flush_opts)
            .await
            .unwrap();

        Ok(())
    }

    async fn alter_schema(
        &self,
        instance: &Arc<Instance>,
        request: AlterSchemaRequest,
    ) -> Result<()> {
        // Leader table can alter schema.
        let policy = TableAlterPolicy::Alter;

        instance
            .alter_schema_of_table(&self.table_data, request, policy)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(AlterTable)?;

        Ok(())
    }

    async fn alter_options(
        &self,
        instance: &Arc<Instance>,
        options: HashMap<String, String>,
    ) -> Result<()> {
        // Leader table can alter option.
        let policy = TableAlterPolicy::Alter;

        instance
            .alter_options_of_table(&self.table_data, options, policy)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(AlterTable)?;

        Ok(())
    }
}

#[async_trait]
impl RoleTable for LeaderTable {
    fn check_state(&self) -> bool {
        self.inner.check_state()
    }

    async fn change_role(&self) -> Result<()> {
        self.inner.change_role().await
    }

    async fn write(&self, instance: &InstanceRef, request: WriteRequest) -> Result<usize> {
        self.inner.write(instance, request).await
    }

    async fn read(
        &self,
        instance: &InstanceRef,
        request: ReadRequest,
    ) -> Result<PartitionedStreams> {
        self.inner.read(instance, request).await
    }

    /// This method is expected to be called by [Instance]
    async fn flush(&self, instance: &Arc<Instance>, flush_opts: TableFlushOptions) -> Result<()> {
        self.inner.flush(instance, flush_opts).await
    }

    /// This method is expected to be called by [Instance]
    async fn alter_schema(
        &self,
        instance: &Arc<Instance>,
        request: AlterSchemaRequest,
    ) -> Result<()> {
        self.inner.alter_schema(instance, request).await
    }

    async fn alter_options(
        &self,
        instance: &Arc<Instance>,
        options: HashMap<String, String>,
    ) -> Result<()> {
        self.inner.alter_options(instance, options).await
    }

    fn table_data(&self) -> TableDataRef {
        self.inner.table_data.clone()
    }
}
