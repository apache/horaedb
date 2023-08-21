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

//! In-memory table engine implementations

use std::{collections::HashMap, fmt};

use async_trait::async_trait;
use common_types::{row::Row, schema::Schema};
use table_engine::{
    partition::PartitionInfo,
    stream::{PartitionedStreams, SendableRecordBatchStream},
    table::{
        AlterSchemaRequest, FlushRequest, GetRequest, ReadRequest, Table, TableId, TableStats,
        WriteRequest,
    },
};

/// In-memory table
///
/// Mainly for test, DO NOT use it in production. All data inserted are buffered
/// in memory, does not support schema change.
pub struct PartitionedMemoryTable {
    /// Table name
    name: String,
    /// Table id
    id: TableId,
    /// Table schema
    schema: Schema,
    /// Engine type
    engine_type: String,
    /// Partition info
    partition_info: PartitionInfo,
}

impl PartitionedMemoryTable {
    pub fn new(
        name: String,
        id: TableId,
        schema: Schema,
        engine_type: String,
        partition_info: PartitionInfo,
    ) -> Self {
        Self {
            name,
            id,
            schema,
            engine_type,
            partition_info,
        }
    }
}

impl fmt::Debug for PartitionedMemoryTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryTable")
            .field("name", &self.name)
            .field("id", &self.id)
            .field("schema", &self.schema)
            .finish()
    }
}

#[async_trait]
impl Table for PartitionedMemoryTable {
    fn name(&self) -> &str {
        &self.name
    }

    fn id(&self) -> TableId {
        self.id
    }

    fn options(&self) -> HashMap<String, String> {
        HashMap::new()
    }

    fn partition_info(&self) -> Option<PartitionInfo> {
        Some(self.partition_info.clone())
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

    fn support_pushdown(&self, _read_schema: &Schema, _col_names: &[String]) -> bool {
        false
    }

    async fn write(&self, _request: WriteRequest) -> table_engine::table::Result<usize> {
        unimplemented!()
    }

    // batch_size is ignored now
    async fn read(
        &self,
        _request: ReadRequest,
    ) -> table_engine::table::Result<SendableRecordBatchStream> {
        unimplemented!()
    }

    async fn get(&self, _request: GetRequest) -> table_engine::table::Result<Option<Row>> {
        unimplemented!()
    }

    async fn partitioned_read(
        &self,
        _request: ReadRequest,
    ) -> table_engine::table::Result<PartitionedStreams> {
        unimplemented!()
    }

    // TODO: Alter schema is not supported now
    async fn alter_schema(
        &self,
        _request: AlterSchemaRequest,
    ) -> table_engine::table::Result<usize> {
        unimplemented!()
    }

    // TODO: Alter modify setting is not supported now
    async fn alter_options(
        &self,
        _options: HashMap<String, String>,
    ) -> table_engine::table::Result<usize> {
        unimplemented!()
    }

    async fn flush(&self, _request: FlushRequest) -> table_engine::table::Result<()> {
        unimplemented!()
    }

    async fn compact(&self) -> table_engine::table::Result<()> {
        unimplemented!()
    }
}
