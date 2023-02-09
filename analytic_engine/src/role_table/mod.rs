// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use common_util::{define_result, error::GenericError};
use snafu::Snafu;
use table_engine::table::{AlterSchemaRequest, WriteRequest};

use crate::{
    instance::{flush_compaction::TableFlushOptions, write_worker::WorkerLocal, Instance},
    space::SpaceRef,
};

mod leader;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to write table, err:{}", source))]
    WriteTable { source: GenericError },
}

define_result!(Error);

/// Those methods are expected to be called by [Instance]
#[async_trait]
pub trait RoleTable {
    fn check_state(&self) -> bool;

    async fn change_role(&self) -> Result<()>;

    // async fn close(&self) -> Result<()>;

    async fn write(
        &self,
        request: WriteRequest,
        instance: &Arc<Instance>,
        space: &SpaceRef,
        worker_local: &mut WorkerLocal,
    ) -> Result<usize>;

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
        worker_local: &mut WorkerLocal,
        request: AlterSchemaRequest,
    ) -> Result<()>;

    async fn alter_options(
        &self,
        instance: &Arc<Instance>,
        worker_local: &mut WorkerLocal,
        options: HashMap<String, String>,
    ) -> Result<()>;
}

#[allow(dead_code)]
#[repr(u8)]
pub enum TableRole {
    Invalid = 0,
    Leader = 1,
    InSync = 2,
    NoSync = 3,
}
