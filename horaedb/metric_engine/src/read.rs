// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use datafusion::{
    datasource::{
        physical_plan::{FileMeta, ParquetFileReaderFactory},
        schema_adapter::{SchemaAdapter, SchemaAdapterFactory, SchemaMapper},
    },
    error::Result as DfResult,
    parquet::arrow::async_reader::AsyncFileReader,
    physical_plan::metrics::ExecutionPlanMetricsSet,
};
use parquet::arrow::async_reader::ParquetObjectReader;

use crate::types::ObjectStoreRef;

#[derive(Debug, Clone)]
pub struct DefaultParquetFileReaderFactory {
    object_store: ObjectStoreRef,
}

/// Returns a AsyncFileReader factory
impl DefaultParquetFileReaderFactory {
    pub fn new(object_store: ObjectStoreRef) -> Self {
        Self { object_store }
    }
}

impl ParquetFileReaderFactory for DefaultParquetFileReaderFactory {
    fn create_reader(
        &self,
        _partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        _metrics: &ExecutionPlanMetricsSet,
    ) -> DfResult<Box<dyn AsyncFileReader + Send>> {
        let object_store = self.object_store.clone();
        let mut reader = ParquetObjectReader::new(object_store, file_meta.object_meta);
        if let Some(size) = metadata_size_hint {
            reader = reader.with_footer_size_hint(size);
        }
        Ok(Box::new(reader))
    }
}

pub struct HoraedbSchemaAdapterFactory {
    pub seq: u64,
}

impl SchemaAdapterFactory for HoraedbSchemaAdapterFactory {
    fn create(
        &self,
        projected_table_schema: SchemaRef,
        table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter> {
        Box::new(HoraedbSchemaAdapter {
            seq: self.seq,
            projected_table_schema,
            table_schema,
        })
    }
}

pub struct HoraedbSchemaAdapter {
    seq: u64,
    projected_table_schema: SchemaRef,
    table_schema: SchemaRef,
}

impl SchemaAdapter for HoraedbSchemaAdapter {
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
        let field = self.projected_table_schema.field(index);
        Some(file_schema.fields.find(field.name())?.0)
    }

    fn map_schema(&self, file_schema: &Schema) -> DfResult<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        Ok((Arc::new(HoraedbSchemaMapper { seq: self.seq }), vec![]))
    }
}

struct HoraedbSchemaMapper {
    seq: u64,
}

impl SchemaMapper for HoraedbSchemaMapper {
    fn map_batch(&self, batch: RecordBatch) -> DfResult<RecordBatch> {
        todo!()
    }

    fn map_partial_batch(&self, batch: RecordBatch) -> DfResult<RecordBatch> {
        todo!()
    }
}
