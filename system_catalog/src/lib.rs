// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! System catalog implementations

#![feature(const_option)]

use std::{
    collections::HashMap,
    fmt::Debug,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use async_trait::async_trait;
use common_types::{
    record_batch::RecordBatch,
    row::Row,
    schema::{RecordSchema, Schema},
};
use futures::Stream;
use table_engine::{
    stream,
    stream::{PartitionedStreams, RecordBatchStream, SendableRecordBatchStream},
    table::{
        AlterSchemaRequest, FlushRequest, GetRequest, ReadRequest, SchemaId, Table, TableId,
        TableSeq, TableStats, WriteRequest,
    },
};

pub mod sys_catalog_table;
pub mod tables;

/// Schema id of the sys catalog schema (`system/public`).
pub const SYSTEM_SCHEMA_ID: SchemaId = SchemaId::from_u32(1);

/// Table name of the `sys_catalog`.
pub const SYS_CATALOG_TABLE_NAME: &str = "sys_catalog";
/// Table sequence of the `sys_catalog` table, always set to 1
pub const SYS_CATALOG_TABLE_SEQ: TableSeq = TableSeq::from_u32(1);
/// Table id of the `sys_catalog` table.
pub const SYS_CATALOG_TABLE_ID: TableId =
    TableId::with_seq(SYSTEM_SCHEMA_ID, SYS_CATALOG_TABLE_SEQ).unwrap();

/// Table name of the `tables` table.
pub const TABLES_TABLE_NAME: &str = "tables";
/// Table sequence of the `tables` table.
pub const TABLES_TABLE_SEQ: TableSeq = TableSeq::from_u32(2);
/// Table id of the `tables` table.
pub const TABLES_TABLE_ID: TableId = TableId::with_seq(SYSTEM_SCHEMA_ID, TABLES_TABLE_SEQ).unwrap();

// NOTE: The MAX_SYSTEM_TABLE_ID should be updated if any new system table is
// added.

/// Max table id of all the system tables.
pub const MAX_SYSTEM_TABLE_SEQ: TableSeq = TABLES_TABLE_SEQ;

/// The minimal thing that a system table needs to implement
#[async_trait]
pub trait SystemTable: Send + Sync + Debug {
    /// System table name
    fn name(&self) -> &str;

    /// System table name
    fn id(&self) -> TableId;

    /// Produce the schema from this system table
    fn schema(&self) -> Schema;

    /// Get the contents of the system table as a single RecordBatch
    async fn read(
        &self,
        request: ReadRequest,
    ) -> table_engine::table::Result<SendableRecordBatchStream>;
}

#[derive(Debug)]
pub struct SystemTableAdapter {
    inner: Arc<dyn SystemTable>,
}

impl SystemTableAdapter {
    pub fn new(inner: impl SystemTable + 'static) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

#[async_trait]
impl Table for SystemTableAdapter {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn id(&self) -> TableId {
        self.inner.id()
    }

    fn schema(&self) -> Schema {
        self.inner.schema()
    }

    fn options(&self) -> HashMap<String, String> {
        HashMap::new()
    }

    fn engine_type(&self) -> &str {
        "system"
    }

    fn stats(&self) -> TableStats {
        TableStats::default()
    }

    async fn write(&self, _request: WriteRequest) -> table_engine::table::Result<usize> {
        Ok(0)
    }

    async fn read(
        &self,
        request: ReadRequest,
    ) -> table_engine::table::Result<SendableRecordBatchStream> {
        self.inner.read(request).await
    }

    async fn get(&self, _request: GetRequest) -> table_engine::table::Result<Option<Row>> {
        Ok(None)
    }

    async fn partitioned_read(
        &self,
        request: ReadRequest,
    ) -> table_engine::table::Result<PartitionedStreams> {
        let read_parallelism = request.opts.read_parallelism;
        let stream = self.inner.read(request).await?;
        let mut streams = Vec::with_capacity(read_parallelism);
        streams.push(stream);
        for _ in 0..read_parallelism - 1 {
            streams.push(Box::pin(OneRecordBatchStream {
                schema: self.schema().clone().to_record_schema(),
                record_batch: None,
            }));
        }
        Ok(PartitionedStreams { streams })
    }

    async fn alter_schema(
        &self,
        _request: AlterSchemaRequest,
    ) -> table_engine::table::Result<usize> {
        Ok(0)
    }

    async fn alter_options(
        &self,
        _options: HashMap<String, String>,
    ) -> table_engine::table::Result<usize> {
        Ok(0)
    }

    async fn flush(&self, _request: FlushRequest) -> table_engine::table::Result<()> {
        Ok(())
    }

    async fn compact(&self) -> table_engine::table::Result<()> {
        Ok(())
    }
}

pub struct OneRecordBatchStream {
    schema: RecordSchema,
    record_batch: Option<RecordBatch>,
}
impl Stream for OneRecordBatchStream {
    type Item = stream::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.record_batch.is_none() {
            Poll::Ready(None)
        } else {
            Poll::Ready(Some(Ok(self.record_batch.take().unwrap())))
        }
    }
}
impl RecordBatchStream for OneRecordBatchStream {
    fn schema(&self) -> &RecordSchema {
        &self.schema
    }
}
