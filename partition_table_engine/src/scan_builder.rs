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

//! Partitioned table scan builder

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    error::{DataFusionError, Result},
    physical_plan::ExecutionPlan,
};
use df_engine_extensions::dist_sql_query::partitioned_table_scan::UnresolvedPartitionedScan;
use table_engine::{
    partition::{
        format_sub_partition_table_name, rule::df_adapter::DfPartitionRuleAdapter, PartitionInfo,
    },
    provider::TableScanBuilder,
    remote::model::TableIdentifier,
    table::ReadRequest,
};

#[derive(Debug)]
pub struct PartitionedTableScanBuilder {
    table_name: String,
    catalog_name: String,
    schema_name: String,
    partition_info: PartitionInfo,
}

impl PartitionedTableScanBuilder {
    pub fn new(
        table_name: String,
        catalog_name: String,
        schema_name: String,
        partition_info: PartitionInfo,
    ) -> Self {
        Self {
            table_name,
            catalog_name,
            schema_name,
            partition_info,
        }
    }

    fn get_sub_table_idents(
        &self,
        table_name: &str,
        partition_info: &PartitionInfo,
        partitions: Vec<usize>,
    ) -> Vec<TableIdentifier> {
        let definitions = partition_info.get_definitions();
        partitions
            .into_iter()
            .map(|p| {
                let partition_name = &definitions[p].name;
                TableIdentifier {
                    catalog: self.catalog_name.clone(),
                    schema: self.schema_name.clone(),
                    table: format_sub_partition_table_name(table_name, partition_name),
                }
            })
            .collect()
    }
}

#[async_trait]
impl TableScanBuilder for PartitionedTableScanBuilder {
    async fn build(&self, request: ReadRequest) -> Result<Arc<dyn ExecutionPlan>> {
        // Build partition rule.
        let table_schema_snapshot = request.projected_schema.original_schema();
        let df_partition_rule =
            DfPartitionRuleAdapter::new(self.partition_info.clone(), table_schema_snapshot)
                .map_err(|e| {
                    DataFusionError::Internal(format!("failed to build partition rule, err:{e}"))
                })?;

        // Evaluate expr and locate partition.
        let partitions = df_partition_rule
            .locate_partitions_for_read(request.predicate.exprs())
            .map_err(|e| {
                DataFusionError::Internal(format!("failed to locate partition for read, err:{e}"))
            })?;
        let sub_tables =
            self.get_sub_table_idents(&self.table_name, &self.partition_info, partitions);

        // Build plan.
        let plan = UnresolvedPartitionedScan {
            sub_tables,
            read_request: request,
        };

        Ok(Arc::new(plan))
    }
}
