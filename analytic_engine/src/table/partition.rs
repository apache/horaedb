// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Distributed Table implementation

use std::{collections::HashMap, fmt};

use async_trait::async_trait;
use catalog::consts::DEFAULT_CATALOG;
use common_types::{
    row::{Row, RowGroupBuilder},
    schema::Schema,
};
use futures::future::try_join_all;
use snafu::{ensure, ResultExt};
use table_engine::{
    partition::{
        format_sub_partition_table_name, rule::df_adapter::DfPartitionRuleAdapter, PartitionInfo,
    },
    remote::{
        model::{
            ReadRequest as RemoteReadRequest, TableIdentifier, WriteRequest as RemoteWriteRequest,
        },
        RemoteEngineRef,
    },
    stream::{PartitionedStreams, SendableRecordBatchStream},
    table::{
        AlterSchemaRequest, CreatePartitionRule, FlushRequest, GetRequest, LocatePartitions,
        ReadRequest, Result, Table, TableId, TableStats, UnexpectedWithMsg, UnsupportedMethod,
        Write, WriteRequest,
    },
};

use crate::space::SpaceAndTable;

/// Table trait implementation
pub struct PartitionTableImpl {
    space_table: SpaceAndTable,
    /// Instance
    remote_engine: RemoteEngineRef,
    /// Engine type
    engine_type: String,
}

impl PartitionTableImpl {
    pub fn new(
        remote_engine: RemoteEngineRef,
        engine_type: String,
        space_table: SpaceAndTable,
    ) -> Result<Self> {
        ensure!(
            space_table.table_data().partition_info.is_some(),
            UnexpectedWithMsg {
                msg: "partition table partition info can't be empty"
            }
        );
        Ok(Self {
            space_table,
            remote_engine,
            engine_type,
        })
    }

    fn generate_sub_table(&self, id: usize) -> TableIdentifier {
        let partition_name = self
            .space_table
            .table_data()
            .partition_info
            .as_ref()
            .map(|v| v.get_definitions()[id].name.clone())
            .unwrap();
        TableIdentifier {
            catalog: DEFAULT_CATALOG.to_string(),
            schema: self.space_table.space().schema_name.clone(),
            table: format_sub_partition_table_name(
                &self.space_table.table_data().name,
                &partition_name,
            ),
        }
    }
}

impl fmt::Debug for PartitionTableImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartitionTableImpl")
            .field("space_id", &self.space_table.space().id)
            .field("table_id", &self.space_table.table_data().id)
            .finish()
    }
}

#[async_trait]
impl Table for PartitionTableImpl {
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

    fn partition_info(&self) -> Option<PartitionInfo> {
        self.space_table.table_data().partition_info.clone()
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
        let partition_info = self.partition_info();
        ensure!(
            partition_info.is_some(),
            UnexpectedWithMsg {
                msg: "partition table partition info can't be empty"
            }
        );

        let partition_info = partition_info.unwrap();

        // build partition rule
        let df_partition_rule =
            DfPartitionRuleAdapter::new(partition_info, &self.space_table.table_data().schema())
                .map_err(|e| Box::new(e) as _)
                .context(CreatePartitionRule)?;
        // split write request
        let mut split_rows = HashMap::new();
        let partitions = df_partition_rule
            .locate_partitions_for_write(&request.row_group)
            .map_err(|e| Box::new(e) as _)
            .context(LocatePartitions)?;

        let schema = request.row_group.schema().clone();
        for (located_partition, row) in partitions.into_iter().zip(request.row_group.into_iter()) {
            split_rows
                .entry(located_partition)
                .or_insert_with(Vec::new)
                .push(row);
        }

        let mut futures = Vec::with_capacity(split_rows.len());
        for (id, rows) in split_rows {
            let row_group = RowGroupBuilder::with_rows(schema.clone(), rows)
                .map_err(|e| Box::new(e) as _)
                .context(Write {
                    table: self.generate_sub_table(id).table,
                })?
                .build();
            // client insert split write request
            futures.push(async move {
                self.remote_engine
                    .write(RemoteWriteRequest {
                        table: self.generate_sub_table(id),
                        write_request: WriteRequest { row_group },
                    })
                    .await
            });
        }
        let result = try_join_all(futures)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(Write {
                table: self.name().to_string(),
            })?;

        Ok(result.into_iter().sum())
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
        let partition_info = self.partition_info();
        ensure!(
            partition_info.is_some(),
            UnexpectedWithMsg {
                msg: "partition table partition info can't be empty"
            }
        );

        let partition_info = partition_info.unwrap();

        // build partition rule
        let df_partition_rule =
            DfPartitionRuleAdapter::new(partition_info, &self.space_table.table_data().schema())
                .map_err(|e| Box::new(e) as _)
                .context(CreatePartitionRule)?;

        // evaluate expr and locate partition
        let partitions = df_partition_rule
            .locate_partitions_for_read(request.predicate.exprs())
            .map_err(|e| Box::new(e) as _)
            .context(LocatePartitions)?;

        // client return async query streams
        let mut futures = Vec::with_capacity(partitions.len());
        for id in partitions {
            let remote_engine = self.remote_engine.clone();
            let request_clone = request.clone();
            futures.push(async move {
                remote_engine
                    .read(RemoteReadRequest {
                        table: self.generate_sub_table(id),
                        read_request: request_clone,
                    })
                    .await
            })
        }

        let streams = try_join_all(futures)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(Write {
                table: self.name().to_string(),
            })?;
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

    async fn flush(&self, _request: FlushRequest) -> Result<()> {
        Ok(())
    }

    async fn compact(&self) -> Result<()> {
        Ok(())
    }
}
