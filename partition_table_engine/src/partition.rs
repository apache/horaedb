// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Distributed Table implementation

use std::{collections::HashMap, fmt};

use analytic_engine::{table::support_pushdown, TableOptions};
use async_trait::async_trait;
use common_types::{
    row::{Row, RowGroup, RowGroupBuilder},
    schema::Schema,
};
use futures::{stream::FuturesUnordered, StreamExt};
use generic_error::BoxError;
use snafu::ResultExt;
use table_engine::{
    partition::{
        format_sub_partition_table_name,
        rule::{
            df_adapter::DfPartitionRuleAdapter, PartitionedRow, PartitionedRows,
            PartitionedRowsIter,
        },
        PartitionInfo,
    },
    remote::{
        model::{
            ReadRequest as RemoteReadRequest, TableIdentifier, WriteBatchResult,
            WriteRequest as RemoteWriteRequest,
        },
        RemoteEngineRef,
    },
    stream::{PartitionedStreams, SendableRecordBatchStream},
    table::{
        AlterSchemaRequest, CreatePartitionRule, FlushRequest, GetRequest, LocatePartitions,
        ReadRequest, Result, Scan, Table, TableId, TableStats, UnexpectedWithMsg,
        UnsupportedMethod, Write, WriteBatch, WriteRequest,
    },
};

use crate::metrics::{
    PARTITION_TABLE_PARTITIONED_READ_DURATION_HISTOGRAM, PARTITION_TABLE_WRITE_DURATION_HISTOGRAM,
};

#[derive(Debug)]
pub struct TableData {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub table_id: TableId,
    pub table_schema: Schema,
    pub partition_info: PartitionInfo,
    pub options: TableOptions,
    pub engine_type: String,
}

/// Table trait implementation
pub struct PartitionTableImpl {
    table_data: TableData,
    remote_engine: RemoteEngineRef,
    partition_rule: DfPartitionRuleAdapter,
}

impl PartitionTableImpl {
    pub fn new(table_data: TableData, remote_engine: RemoteEngineRef) -> Result<Self> {
        // Build partition rule.
        let partition_rule = DfPartitionRuleAdapter::new(
            table_data.partition_info.clone(),
            &table_data.table_schema,
        )
        .box_err()
        .context(CreatePartitionRule)?;

        Ok(Self {
            table_data,
            remote_engine,
            partition_rule,
        })
    }

    fn get_sub_table_ident(&self, id: usize) -> TableIdentifier {
        let partition_name = self.table_data.partition_info.get_definitions()[id]
            .name
            .clone();
        TableIdentifier {
            catalog: self.table_data.catalog_name.clone(),
            schema: self.table_data.schema_name.clone(),
            table: format_sub_partition_table_name(&self.table_data.table_name, &partition_name),
        }
    }

    async fn write_single_row_group(
        &self,
        partition_id: usize,
        row_group: RowGroup,
    ) -> Result<usize> {
        let sub_table_ident = self.get_sub_table_ident(partition_id);

        let request = RemoteWriteRequest {
            table: sub_table_ident,
            write_request: WriteRequest { row_group },
        };

        self.remote_engine
            .write(request)
            .await
            .box_err()
            .with_context(|| WriteBatch {
                tables: vec![self.table_data.table_name.clone()],
            })
    }

    async fn write_partitioned_row_groups(
        &self,
        schema: Schema,
        partitioned_rows: PartitionedRowsIter,
    ) -> Result<usize> {
        let mut split_rows = HashMap::new();
        for PartitionedRow { partition_id, row } in partitioned_rows {
            split_rows
                .entry(partition_id)
                .or_insert_with(Vec::new)
                .push(row);
        }

        // Insert split write request through remote engine.
        let mut request_batch = Vec::with_capacity(split_rows.len());
        for (partition, rows) in split_rows {
            let sub_table_ident = self.get_sub_table_ident(partition);
            let row_group = RowGroupBuilder::with_rows(schema.clone(), rows)
                .box_err()
                .with_context(|| Write {
                    table: sub_table_ident.table.clone(),
                })?
                .build();

            let request = RemoteWriteRequest {
                table: sub_table_ident,
                write_request: WriteRequest { row_group },
            };
            request_batch.push(request);
        }

        let batch_results = self
            .remote_engine
            .write_batch(request_batch)
            .await
            .box_err()
            .with_context(|| WriteBatch {
                tables: vec![self.table_data.table_name.clone()],
            })?;
        let mut total_rows = 0;
        for batch_result in batch_results {
            let WriteBatchResult {
                table_idents,
                result,
            } = batch_result;

            let written_rows = result.with_context(|| {
                let tables = table_idents
                    .into_iter()
                    .map(|ident| ident.table)
                    .collect::<Vec<_>>();
                WriteBatch { tables }
            })?;
            total_rows += written_rows;
        }

        Ok(total_rows as usize)
    }
}

impl fmt::Debug for PartitionTableImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartitionTableImpl")
            .field("table_data", &self.table_data)
            .finish()
    }
}

#[async_trait]
impl Table for PartitionTableImpl {
    fn name(&self) -> &str {
        &self.table_data.table_name
    }

    fn id(&self) -> TableId {
        self.table_data.table_id
    }

    fn schema(&self) -> Schema {
        self.table_data.table_schema.clone()
    }

    // TODO: get options from sub partition table with remote engine
    fn options(&self) -> HashMap<String, String> {
        self.table_data.options.to_raw_map()
    }

    fn partition_info(&self) -> Option<PartitionInfo> {
        Some(self.table_data.partition_info.clone())
    }

    fn engine_type(&self) -> &str {
        &self.table_data.engine_type
    }

    fn stats(&self) -> TableStats {
        TableStats::default()
    }

    // TODO: maybe we should ask remote sub table whether support pushdown
    fn support_pushdown(&self, read_schema: &Schema, col_names: &[String]) -> bool {
        let need_dedup = self.table_data.options.need_dedup();

        support_pushdown(read_schema, need_dedup, col_names)
    }

    async fn write(&self, request: WriteRequest) -> Result<usize> {
        let _timer = PARTITION_TABLE_WRITE_DURATION_HISTOGRAM
            .with_label_values(&["total"])
            .start_timer();

        // Split write request.
        let schema = request.row_group.schema().clone();
        let partition_rows = {
            let _locate_timer = PARTITION_TABLE_WRITE_DURATION_HISTOGRAM
                .with_label_values(&["locate"])
                .start_timer();
            self.partition_rule
                .locate_partitions_for_write(request.row_group)
                .box_err()
                .context(LocatePartitions)?
        };

        match partition_rows {
            PartitionedRows::One {
                partition_id,
                row_group,
            } => self.write_single_row_group(partition_id, row_group).await,
            PartitionedRows::Multiple(iter) => {
                self.write_partitioned_row_groups(schema, iter).await
            }
        }
    }

    async fn read(&self, _request: ReadRequest) -> Result<SendableRecordBatchStream> {
        UnsupportedMethod {
            table: self.name(),
            method: "read",
        }
        .fail()
    }

    async fn get(&self, _request: GetRequest) -> Result<Option<Row>> {
        UnsupportedMethod {
            table: self.name(),
            method: "get",
        }
        .fail()
    }

    async fn partitioned_read(&self, request: ReadRequest) -> Result<PartitionedStreams> {
        let _timer = PARTITION_TABLE_PARTITIONED_READ_DURATION_HISTOGRAM
            .with_label_values(&["total"])
            .start_timer();

        // Build partition rule.
        let df_partition_rule = match self.partition_info() {
            None => UnexpectedWithMsg {
                msg: "partition table partition info can't be empty",
            }
            .fail()?,
            Some(partition_info) => {
                DfPartitionRuleAdapter::new(partition_info, &self.table_data.table_schema)
                    .box_err()
                    .context(CreatePartitionRule)?
            }
        };

        // Evaluate expr and locate partition.
        let partitions = {
            let _locate_timer = PARTITION_TABLE_PARTITIONED_READ_DURATION_HISTOGRAM
                .with_label_values(&["locate"])
                .start_timer();
            df_partition_rule
                .locate_partitions_for_read(request.predicate.exprs())
                .box_err()
                .context(LocatePartitions)?
        };

        // Query streams through remote engine.
        let mut futures = FuturesUnordered::new();
        for partition in partitions {
            let read_partition = self.remote_engine.read(RemoteReadRequest {
                table: self.get_sub_table_ident(partition),
                read_request: request.clone(),
            });
            futures.push(read_partition);
        }

        let mut record_batch_streams = Vec::with_capacity(futures.len());
        while let Some(record_batch_stream) = futures.next().await {
            let record_batch_stream = record_batch_stream
                .box_err()
                .context(Scan { table: self.name() })?;
            record_batch_streams.push(record_batch_stream);
        }

        let streams = {
            let _remote_timer = PARTITION_TABLE_PARTITIONED_READ_DURATION_HISTOGRAM
                .with_label_values(&["remote_read"])
                .start_timer();
            record_batch_streams
        };

        Ok(PartitionedStreams { streams })
    }

    async fn alter_schema(&self, _request: AlterSchemaRequest) -> Result<usize> {
        UnsupportedMethod {
            table: self.name(),
            method: "alter_schema",
        }
        .fail()
    }

    async fn alter_options(&self, _options: HashMap<String, String>) -> Result<usize> {
        UnsupportedMethod {
            table: self.name(),
            method: "alter_options",
        }
        .fail()
    }

    // Partition table is a virtual table, so it don't need to flush.
    async fn flush(&self, _request: FlushRequest) -> Result<()> {
        Ok(())
    }

    // Partition table is a virtual table, so it don't need to compact.
    async fn compact(&self) -> Result<()> {
        Ok(())
    }
}
