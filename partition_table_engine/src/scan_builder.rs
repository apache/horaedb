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

use analytic_engine::sst::writer::BuildParquetFilter;
use async_trait::async_trait;
use common_types::schema::Schema;
use datafusion::{
    error::{DataFusionError, Result},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use df_engine_extensions::dist_sql_query::physical_plan::UnresolvedPartitionedScan;
use generic_error::BoxError;
use table_engine::{
    partition::{
        format_sub_partition_table_name, rule::df_adapter::DfPartitionRuleAdapter,
        BuildPartitionsMode, ExplicitMode, NormalMode, PartitionInfo, QueryPartitionsBuilder,
        SelectedPartitions,
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
        // Partitions builder.
        let partitions_builder = QueryPartitionsBuilder::new(
            &self.catalog_name,
            &self.schema_name,
            &self.table_name,
            &self.partition_info,
        );

        // Build sub table idents.
        let table_schema_snapshot = request.projected_schema.original_schema();
        let build_partitions_mode = match &request.selected_partitions {
            Some(selected) => {
                let explicit = ExplicitMode {
                    selected_partitions: selected,
                };
                BuildPartitionsMode::Explicit(explicit)
            }

            None => {
                let normal = NormalMode {
                    exprs: request.predicate.exprs(),
                    schema: table_schema_snapshot,
                };
                BuildPartitionsMode::Normal(normal)
            }
        };

        let sub_tables = partitions_builder
            .build(build_partitions_mode)
            .map_err(|e| {
                DataFusionError::Internal(format!("failed to build partitions, err:{e}"))
            })?;

        // Build plan.
        let plan = UnresolvedPartitionedScan {
            sub_tables,
            read_request: request,
        };

        Ok(Arc::new(plan))
    }
}
