// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Table implementation

use std::{collections::HashMap, fmt};

use async_trait::async_trait;
use common_types::{row::Row, schema::Schema, time::TimeRange};
use common_util::error::BoxError;
use datafusion::{common::Column, logical_expr::Expr};
use futures::TryStreamExt;
use snafu::{ensure, OptionExt, ResultExt};
use table_engine::{
    partition::PartitionInfo,
    predicate::PredicateBuilder,
    stream::{PartitionedStreams, SendableRecordBatchStream},
    table::{
        AlterOptions, AlterSchema, AlterSchemaRequest, Compact, Flush, FlushRequest, Get,
        GetInvalidPrimaryKey, GetNullPrimaryKey, GetRequest, ReadOptions, ReadOrder, ReadRequest,
        Result, Scan, Table, TableId, TableStats, Write, WriteRequest,
    },
};
use trace_metric::MetricsCollector;

use self::data::TableDataRef;
use crate::{
    instance::{alter::Alterer, write::Writer, InstanceRef},
    space::{SpaceAndTable, SpaceId},
};

pub mod data;
pub mod metrics;
pub mod sst_util;
pub mod version;
pub mod version_edit;

const GET_METRICS_COLLECTOR_NAME: &str = "get";

/// Table trait implementation
pub struct TableImpl {
    space_table: SpaceAndTable,
    /// Instance
    instance: InstanceRef,
    /// Engine type
    engine_type: String,

    space_id: SpaceId,
    table_id: TableId,

    /// Holds a strong reference to prevent the underlying table from being
    /// dropped when this handle exist.
    table_data: TableDataRef,
}

impl TableImpl {
    pub fn new(
        instance: InstanceRef,
        engine_type: String,
        space_id: SpaceId,
        table_id: TableId,
        table_data: TableDataRef,
        space_table: SpaceAndTable,
    ) -> Self {
        Self {
            space_table,
            instance,
            engine_type,
            space_id,
            table_id,
            table_data,
        }
    }
}

impl fmt::Debug for TableImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableImpl")
            .field("space_id", &self.space_id)
            .field("table_id", &self.table_id)
            .finish()
    }
}

#[async_trait]
impl Table for TableImpl {
    fn name(&self) -> &str {
        &self.table_data.name
    }

    fn id(&self) -> TableId {
        self.table_data.id
    }

    fn schema(&self) -> Schema {
        self.table_data.schema()
    }

    fn options(&self) -> HashMap<String, String> {
        self.table_data.table_options().to_raw_map()
    }

    fn partition_info(&self) -> Option<PartitionInfo> {
        None
    }

    fn engine_type(&self) -> &str {
        &self.engine_type
    }

    fn stats(&self) -> TableStats {
        self.table_data.metrics.table_stats()
    }

    async fn write(&self, request: WriteRequest) -> Result<usize> {
        let mut serializer = self.table_data.serializer.lock().await;
        let mut writer = Writer::new(
            self.instance.clone(),
            self.space_table.clone(),
            &mut serializer,
        );

        let num_rows = writer
            .write(request)
            .await
            .box_err()
            .context(Write { table: self.name() })?;
        Ok(num_rows)
    }

    async fn read(&self, mut request: ReadRequest) -> Result<SendableRecordBatchStream> {
        request.opts.read_parallelism = 1;
        let mut streams = self
            .instance
            .partitioned_read_from_table(&self.space_table, request)
            .await
            .box_err()
            .context(Scan { table: self.name() })?;

        assert_eq!(streams.streams.len(), 1);
        let stream = streams.streams.pop().unwrap();

        Ok(stream)
    }

    async fn get(&self, request: GetRequest) -> Result<Option<Row>> {
        let schema = request.projected_schema.to_record_schema_with_key();
        let primary_key_columns = &schema.key_columns()[..];
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
            metrics_collector: MetricsCollector::new(GET_METRICS_COLLECTOR_NAME.to_string()),
        };
        let mut batch_stream = self
            .read(read_request)
            .await
            .box_err()
            .context(Scan { table: self.name() })?;

        let mut result_columns = Vec::with_capacity(schema.num_columns());

        while let Some(batch) = batch_stream
            .try_next()
            .await
            .box_err()
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

                let mut result_columns_k = vec![];
                for col_idx in schema.primary_key_idx() {
                    result_columns_k.push(result_columns[*col_idx].clone());
                }
                if request.primary_key == result_columns_k {
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
            .box_err()
            .context(Scan { table: self.name() })?;

        Ok(streams)
    }

    async fn alter_schema(&self, request: AlterSchemaRequest) -> Result<usize> {
        let mut serializer = self.table_data.serializer.lock().await;
        let mut alterer = Alterer::new(
            self.table_data.clone(),
            &mut serializer,
            self.instance.clone(),
        )
        .await;

        alterer
            .alter_schema_of_table(request)
            .await
            .box_err()
            .context(AlterSchema { table: self.name() })?;
        Ok(0)
    }

    async fn alter_options(&self, options: HashMap<String, String>) -> Result<usize> {
        let mut serializer = self.table_data.serializer.lock().await;
        let alterer = Alterer::new(
            self.table_data.clone(),
            &mut serializer,
            self.instance.clone(),
        )
        .await;

        alterer
            .alter_options_of_table(options)
            .await
            .box_err()
            .context(AlterOptions { table: self.name() })?;
        Ok(0)
    }

    async fn flush(&self, request: FlushRequest) -> Result<()> {
        self.instance
            .manual_flush_table(&self.table_data, request)
            .await
            .box_err()
            .context(Flush { table: self.name() })
    }

    async fn compact(&self) -> Result<()> {
        self.instance
            .manual_compact_table(&self.table_data)
            .await
            .box_err()
            .context(Compact { table: self.name() })?;
        Ok(())
    }
}
