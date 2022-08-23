use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use common_util::define_result;
use snafu::Snafu;
use table_engine::table::{AlterSchemaRequest, WriteRequest};

pub use self::leader::LeaderTable;
use crate::{
    instance::{
        flush_compaction::TableFlushOptions, write_worker::WorkerLocal, Instance, InstanceRef,
    },
    table::data::TableDataRef,
};

mod leader;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to write table, err:{}", source))]
    WriteTable {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display("Failed to alter table, err:{}", source))]
    AlterTable {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

define_result!(Error);

pub type RoleTableRef = Arc<dyn RoleTable + Send + Sync + 'static>;

pub fn create_role_table(table_data: TableDataRef, role: TableRole) -> RoleTableRef {
    match role {
        TableRole::Invalid => todo!(),
        TableRole::Leader => LeaderTable::new(table_data),
        TableRole::InSync => todo!(),
        TableRole::NoSync => todo!(),
    }
}

/// Those methods are expected to be called by [Instance]
#[async_trait]
pub trait RoleTable: std::fmt::Debug + 'static {
    fn check_state(&self) -> bool;

    async fn change_role(&self) -> Result<()>;

    // async fn close(&self) -> Result<()>;

    async fn write(&self, instance: &InstanceRef, request: WriteRequest) -> Result<usize>;

    // async fn read(&self, request: ReadRequest) -> Result<PartitionedStreams>;

    async fn flush(
        &self,
        flush_opts: TableFlushOptions,
        instance: &Arc<Instance>,
        worker_local: &mut WorkerLocal,
    ) -> Result<()>;

    async fn alter_schema(
        &self,
        instance: &Arc<Instance>,
        request: AlterSchemaRequest,
    ) -> Result<()>;

    async fn alter_options(
        &self,
        instance: &Arc<Instance>,
        options: HashMap<String, String>,
    ) -> Result<()>;

    // TODO: do we need to expose this?
    fn table_data(&self) -> TableDataRef;
}

#[allow(dead_code)]
#[repr(u8)]
pub enum TableRole {
    Invalid = 0,
    Leader = 1,
    InSync = 2,
    NoSync = 3,
}
