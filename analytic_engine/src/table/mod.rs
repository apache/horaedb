// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Table implementation

use std::{collections::HashMap, fmt};

use arrow_deps::datafusion::logical_plan::{Column, Expr};
use async_trait::async_trait;
use common_types::{row::Row, schema::Schema, time::TimeRange};
use futures::TryStreamExt;
use snafu::{ensure, OptionExt, ResultExt};
use table_engine::{
    predicate::PredicateBuilder,
    stream::{PartitionedStreams, SendableRecordBatchStream},
    table::{
        AlterOptions, AlterSchema, AlterSchemaRequest, Compact, Flush, FlushRequest, Get,
        GetInvalidPrimaryKey, GetNullPrimaryKey, GetRequest, ReadOptions, ReadOrder, ReadRequest,
        Result, Scan, Table, TableId, TableStats, Write, WriteRequest,
    },
};
use tokio::sync::oneshot;

use crate::{
    instance::{flush_compaction::TableFlushOptions, InstanceRef},
    space::SpaceAndTable,
};

pub mod data;
pub mod metrics;
pub mod sst_util;
pub mod version;
pub mod version_edit;

// TODO(yingwen): How to handle drop table?

/// Table trait implementation
pub struct TableImpl {
    /// Space and table info
    space_table: SpaceAndTable,
    /// Instance
    instance: InstanceRef,
    /// Engine type
    engine_type: String,
}

impl TableImpl {
    pub fn new(space_table: SpaceAndTable, instance: InstanceRef, engine_type: String) -> Self {
        Self {
            space_table,
            instance,
            engine_type,
        }
    }
}

impl fmt::Debug for TableImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableImpl")
            .field("space_table", &self.space_table)
            .finish()
    }
}

#[async_trait]
impl Table for TableImpl {
    fn name(&self) -> &str {
        &self.space_table.table_data().name
    }

    fn id(&self) -> TableId {
        self.space_table.table_data().id
    }

    fn schema(&self) -> Schema {
        self.space_table.table_data().schema()
    }

    fn options(&self) -> HashMap<String, String> {
        self.space_table.table_data().table_options().to_raw_map()
    }

    fn engine_type(&self) -> &str {
        &self.engine_type
    }

    fn stats(&self) -> TableStats {
        let metrics = &self.space_table.table_data().metrics;

        TableStats {
            num_write: metrics.write_request_counter.get(),
            num_read: metrics.read_request_counter.get(),
            num_flush: metrics.flush_duration_histogram.get_sample_count(),
        }
    }

    async fn write(&self, request: WriteRequest) -> Result<usize> {
        let num_rows = self
            .instance
            .write_to_table(&self.space_table, request)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(Write { table: self.name() })?;
        Ok(num_rows)
    }

    async fn read(&self, mut request: ReadRequest) -> Result<SendableRecordBatchStream> {
        request.opts.read_parallelism = 1;
        let mut streams = self
            .instance
            .partitioned_read_from_table(&self.space_table, request)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(Scan { table: self.name() })?;

        assert_eq!(streams.streams.len(), 1);
        let stream = streams.streams.pop().unwrap();

        Ok(stream)
    }

    async fn get(&self, request: GetRequest) -> Result<Option<Row>> {
        let schema = request.projected_schema.to_record_schema_with_key();
        let primary_key_columns = schema.key_columns();
        ensure!(
            primary_key_columns.len() == request.primary_key.len(),
            GetInvalidPrimaryKey {
                schema: schema.clone(),
                primary_key_columns,
            }
        );

        let mut primary_key_exprs: Vec<Expr> = Vec::with_capacity(request.primary_key.len());
        for (primary_key_value, column_schema) in
            request.primary_key.iter().zip(primary_key_columns.iter())
        {
            let v = primary_key_value
                .as_scalar_value()
                .with_context(|| GetNullPrimaryKey {
                    schema: schema.clone(),
                    primary_key_columns,
                })?;
            primary_key_exprs.push(
                Expr::Column(Column::from_qualified_name(&column_schema.name)).eq(Expr::Literal(v)),
            );
        }

        let predicate = PredicateBuilder::default()
            .set_time_range(TimeRange::min_to_max())
            .add_pushdown_exprs(&primary_key_exprs)
            .build();

        let read_request = ReadRequest {
            request_id: request.request_id,
            opts: ReadOptions::default(),
            projected_schema: request.projected_schema,
            predicate,
            order: ReadOrder::None,
        };
        let mut batch_stream = self
            .read(read_request)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(Scan { table: self.name() })?;

        let mut result_columns = Vec::with_capacity(schema.num_columns());

        while let Some(batch) = batch_stream
            .try_next()
            .await
            .map_err(|e| Box::new(e) as _)
            .context(Get { table: self.name() })?
        {
            let row_num = batch.num_rows();
            if row_num == 0 {
                return Ok(None);
            }
            for row_idx in 0..row_num {
                for col_idx in 0..batch.num_columns() {
                    let col = batch.column(col_idx);
                    result_columns.push(col.datum(row_idx));
                }

                if request.primary_key == result_columns[..schema.num_key_columns()] {
                    return Ok(Some(Row::from_datums(result_columns)));
                }
                result_columns.clear();
            }
        }

        Ok(None)
    }

    async fn partitioned_read(&self, request: ReadRequest) -> Result<PartitionedStreams> {
        let streams = self
            .instance
            .partitioned_read_from_table(&self.space_table, request)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(Scan { table: self.name() })?;

        Ok(streams)
    }

    async fn alter_schema(&self, request: AlterSchemaRequest) -> Result<usize> {
        self.instance
            .alter_schema_of_table(&self.space_table, request)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(AlterSchema { table: self.name() })?;
        Ok(0)
    }

    async fn alter_options(&self, options: HashMap<String, String>) -> Result<usize> {
        self.instance
            .alter_options_of_table(&self.space_table, options)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(AlterOptions { table: self.name() })?;
        Ok(0)
    }

    async fn flush(&self, request: FlushRequest) -> Result<()> {
        let mut rx_opt = None;
        let flush_opts = TableFlushOptions {
            compact_after_flush: request.compact_after_flush,
            // Never block write thread
            block_on_write_thread: false,
            res_sender: if request.sync {
                let (tx, rx) = oneshot::channel();
                rx_opt = Some(rx);
                Some(tx)
            } else {
                None
            },
        };

        self.instance
            .flush_table(&self.space_table, flush_opts)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(Flush { table: self.name() })?;
        if let Some(rx) = rx_opt {
            rx.await
                .map_err(|e| Box::new(e) as _)
                .context(Flush { table: self.name() })??;
        }
        Ok(())
    }

    async fn compact(&self) -> Result<()> {
        self.instance
            .manual_compact_table(&self.space_table)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(Compact { table: self.name() })?;
        Ok(())
    }
}
