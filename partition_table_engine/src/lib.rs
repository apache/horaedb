// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Partition table engine implementations

mod error;
mod metrics;
mod partition;

use std::sync::Arc;

use async_trait::async_trait;
use common_util::error::BoxError;
use snafu::{OptionExt, ResultExt};
use table_engine::{
    engine::{
        CloseTableRequest, CreateTableRequest, DropTableRequest, OpenTableRequest, Result,
        TableEngine, Unexpected, UnexpectedNoCause,
    },
    remote::RemoteEngineRef,
    table::TableRef,
    PARTITION_TABLE_ENGINE_TYPE,
};

use crate::partition::PartitionTableImpl;

/// Partition table engine implementation.
pub struct PartitionTableEngine {
    remote_engine_ref: RemoteEngineRef,
}

impl PartitionTableEngine {
    pub fn new(remote_engine_ref: RemoteEngineRef) -> Self {
        Self { remote_engine_ref }
    }
}

#[async_trait]
impl TableEngine for PartitionTableEngine {
    fn engine_type(&self) -> &str {
        PARTITION_TABLE_ENGINE_TYPE
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }

    async fn create_table(&self, request: CreateTableRequest) -> Result<TableRef> {
        Ok(Arc::new(
            PartitionTableImpl::new(
                request.catalog_name,
                request.schema_name,
                request.table_name,
                request.table_id,
                request.table_schema,
                request.partition_info.context(UnexpectedNoCause {
                    msg: "partition info not found",
                })?,
                request.options,
                request.engine,
                self.remote_engine_ref.clone(),
            )
            .box_err()
            .context(Unexpected)?,
        ))
    }

    async fn drop_table(&self, _request: DropTableRequest) -> Result<bool> {
        Ok(true)
    }

    async fn open_table(&self, _request: OpenTableRequest) -> Result<Option<TableRef>> {
        Ok(None)
    }

    async fn close_table(&self, _request: CloseTableRequest) -> Result<()> {
        Ok(())
    }
}
