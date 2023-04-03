// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! In-memory table engine implementations

use std::{
    collections::HashMap,
    fmt,
    pin::Pin,
    sync::{Arc, RwLock},
    task::{Context, Poll},
};

use async_trait::async_trait;
use common_types::{
    column::{ColumnBlock, ColumnBlockBuilder},
    datum::{Datum, DatumKind},
    record_batch::RecordBatch,
    row::{Row, RowGroup},
    schema::{RecordSchema, Schema},
};
use common_util::error::BoxError;
use futures::stream::Stream;
use snafu::{OptionExt, ResultExt};

use crate::{
    engine::{
        CloseTableRequest, CreateTableRequest, DropTableRequest, OpenTableRequest, TableEngine,
    },
    stream::{
        self, ErrNoSource, ErrWithSource, PartitionedStreams, RecordBatchStream,
        SendableRecordBatchStream,
    },
    table::{
        AlterSchemaRequest, FlushRequest, GetRequest, ReadRequest, Result, Table, TableId,
        TableRef, TableStats, UnsupportedMethod, WriteRequest,
    },
    MEMORY_ENGINE_TYPE,
};

type RowGroupVec = Vec<RowGroup>;

/// In-memory table
///
/// Mainly for test, DO NOT use it in production. All data inserted are buffered
/// in memory, does not support schema change.
pub struct MemoryTable {
    /// Table name
    name: String,
    /// Table id
    id: TableId,
    /// Table schema
    schema: Schema,
    /// Rows
    row_groups: Arc<RwLock<RowGroupVec>>,
    /// Engine type
    engine_type: String,
}

impl MemoryTable {
    pub fn new(name: String, id: TableId, schema: Schema, engine_type: String) -> Self {
        Self {
            name,
            id,
            schema,
            row_groups: Arc::new(RwLock::new(Vec::new())),
            engine_type,
        }
    }
}

impl fmt::Debug for MemoryTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryTable")
            .field("name", &self.name)
            .field("id", &self.id)
            .field("schema", &self.schema)
            // row_groups is ignored
            .finish()
    }
}

#[async_trait]
impl Table for MemoryTable {
    fn name(&self) -> &str {
        &self.name
    }

    fn id(&self) -> TableId {
        self.id
    }

    fn options(&self) -> HashMap<String, String> {
        HashMap::new()
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn engine_type(&self) -> &str {
        &self.engine_type
    }

    fn stats(&self) -> TableStats {
        TableStats::default()
    }

    async fn write(&self, request: WriteRequest) -> Result<usize> {
        // TODO(yingwen) Maybe check schema?
        let mut row_groups = self.row_groups.write().unwrap();
        let n = request.row_group.num_rows();
        row_groups.push(request.row_group);

        Ok(n)
    }

    // batch_size is ignored now
    async fn read(&self, request: ReadRequest) -> Result<SendableRecordBatchStream> {
        let scan = MemoryScan {
            schema: request.projected_schema.to_record_schema(),
            row_groups: self.row_groups.clone(),
            index: 0,
        };

        Ok(Box::pin(scan))
    }

    async fn get(&self, _request: GetRequest) -> Result<Option<Row>> {
        UnsupportedMethod {
            table: &self.name,
            method: "get",
        }
        .fail()
    }

    async fn partitioned_read(&self, request: ReadRequest) -> Result<PartitionedStreams> {
        let stream = self.read(request).await?;

        Ok(PartitionedStreams::one_stream(stream))
    }

    // TODO: Alter schema is not supported now
    async fn alter_schema(&self, _request: AlterSchemaRequest) -> Result<usize> {
        Ok(0)
    }

    // TODO: Alter modify setting is not supported now
    async fn alter_options(&self, _options: HashMap<String, String>) -> Result<usize> {
        Ok(0)
    }

    async fn flush(&self, _request: FlushRequest) -> Result<()> {
        // Flush is not supported now.
        UnsupportedMethod {
            table: self.name(),
            method: "flush",
        }
        .fail()
    }

    async fn compact(&self) -> Result<()> {
        // Compact is not supported now.
        UnsupportedMethod {
            table: self.name(),
            method: "compact",
        }
        .fail()
    }
}

#[derive(Debug)]
struct MemoryScan {
    // The schema of projected column indexed by ReadRequest::projection
    schema: RecordSchema,
    row_groups: Arc<RwLock<RowGroupVec>>,
    index: usize,
}

impl Stream for MemoryScan {
    type Item = stream::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // TODO(yingwen): Batch row groups
        let record_batch = {
            let row_groups = self.row_groups.read().unwrap();
            if self.index >= row_groups.len() {
                return Poll::Ready(None);
            }

            let rows = &row_groups[self.index];
            // Because the row group inserted may have different column order, so we cannot
            // reuse the projection index, and must find projection index for each row
            // group, which is inefficient
            row_group_to_record_batch(rows, &self.schema)
        };

        self.index += 1;
        Poll::Ready(Some(record_batch))
    }
}

impl RecordBatchStream for MemoryScan {
    fn schema(&self) -> &RecordSchema {
        &self.schema
    }
}

// REQUIRE: The schema is the projected schema
fn row_group_to_record_batch(
    rows: &RowGroup,
    record_schema: &RecordSchema,
) -> stream::Result<RecordBatch> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(record_schema.clone()));
    }

    let num_cols = record_schema.num_columns();
    let mut column_blocks = Vec::with_capacity(num_cols);
    // For each column, create an array for that column
    for column in record_schema.columns().iter() {
        let rows_schema = rows.schema();
        let col_index = rows_schema
            .index_of(&column.name)
            .with_context(|| ErrNoSource {
                msg: format!(
                    "failed to convert RowGroup to RecordBatch, column not found, column:{}",
                    &column.name
                ),
            })?;
        let cols = rows.iter_column(col_index);
        let column_block = build_column_block(&column.data_type, cols)?;
        column_blocks.push(column_block);
    }

    RecordBatch::new(record_schema.clone(), column_blocks)
        .box_err()
        .context(ErrWithSource {
            msg: "failed to create RecordBatch",
        })
}

fn build_column_block<'a, I: Iterator<Item = &'a Datum>>(
    data_type: &DatumKind,
    iter: I,
) -> stream::Result<ColumnBlock> {
    let mut builder = ColumnBlockBuilder::with_capacity(data_type, iter.size_hint().0);
    for datum in iter {
        builder
            .append(datum.clone())
            .box_err()
            .context(ErrWithSource {
                msg: "append datum",
            })?;
    }
    Ok(builder.build())
}

/// Memory table engine implementation
// Mainly for test purpose now
pub struct MemoryTableEngine;

#[async_trait]
impl TableEngine for MemoryTableEngine {
    fn engine_type(&self) -> &str {
        MEMORY_ENGINE_TYPE
    }

    async fn close(&self) -> crate::engine::Result<()> {
        Ok(())
    }

    async fn create_table(&self, request: CreateTableRequest) -> crate::engine::Result<TableRef> {
        Ok(Arc::new(MemoryTable::new(
            request.table_name,
            request.table_id,
            request.table_schema,
            MEMORY_ENGINE_TYPE.to_string(),
        )))
    }

    async fn drop_table(&self, _request: DropTableRequest) -> crate::engine::Result<bool> {
        Ok(true)
    }

    async fn open_table(
        &self,
        _request: OpenTableRequest,
    ) -> crate::engine::Result<Option<TableRef>> {
        Ok(None)
    }

    async fn close_table(&self, _request: CloseTableRequest) -> crate::engine::Result<()> {
        Ok(())
    }
}
