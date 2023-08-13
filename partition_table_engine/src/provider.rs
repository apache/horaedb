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

// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Datafusion `TableProvider` adapter

use std::{
    any::Any,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use common_types::{projected_schema::ProjectedSchema, request_id::RequestId, schema::Schema};
use datafusion::{
    config::ExtensionOptions,
    datasource::TableProvider,
    error::{DataFusionError, Result},
    execution::context::SessionState,
    logical_expr::{Expr, TableProviderFilterPushDown, TableSource, TableType},
    physical_plan::ExecutionPlan,
};
use df_engine_extensions::dist_sql_query::partitioned_table_scan::UnresolvedPartitionedScan;
use log::debug;
use table_engine::{
    partition::{
        format_sub_partition_table_name, rule::df_adapter::DfPartitionRuleAdapter, PartitionInfo,
    },
    predicate::PredicateBuilder,
    provider::CeresdbOptions,
    remote::model::TableIdentifier,
    table::{ReadOptions, ReadRequest, TableRef},
};
use trace_metric::MetricsCollector;

const SCAN_TABLE_METRICS_COLLECTOR_NAME: &str = "scan_table";

/// `TableProviderAdapter` for `PartitionedTableImpl`.
// TODO: use single `TableProviderAdapter` for normal table and partitioned table
// (it will cause cyclic dependency now... I think it is due to our messy code organization).
#[derive(Debug)]
pub struct TableProviderAdapter {
    table: TableRef,
    /// The schema of the table when this adapter is created, used as schema
    /// snapshot for read to avoid the reader sees different schema during
    /// query
    read_schema: Schema,
    partition_info: PartitionInfo,
}

impl TableProviderAdapter {
    pub fn new(table: TableRef, partition_info: PartitionInfo) -> Self {
        // Take a snapshot of the schema
        let read_schema = table.schema();

        Self {
            table,
            read_schema,
            partition_info,
        }
    }

    pub fn as_table_ref(&self) -> &TableRef {
        &self.table
    }

    pub async fn scan_table(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let ceresdb_options = state.config_options().extensions.get::<CeresdbOptions>();
        assert!(ceresdb_options.is_some());
        let ceresdb_options = ceresdb_options.unwrap();
        let request_id = RequestId::from(ceresdb_options.request_id);
        let deadline = ceresdb_options
            .request_timeout
            .map(|n| Instant::now() + Duration::from_millis(n));
        let read_parallelism = state.config().target_partitions();
        debug!(
            "scan table, table:{}, request_id:{}, projection:{:?}, filters:{:?}, limit:{:?}, deadline:{:?}, parallelism:{}",
            self.table.name(),
            request_id,
            projection,
            filters,
            limit,
            deadline,
            read_parallelism,
        );

        let predicate = PredicateBuilder::default()
            .add_pushdown_exprs(filters)
            .extract_time_range(&self.read_schema, filters)
            .build();

        let projected_schema = ProjectedSchema::new(self.read_schema.clone(), projection.cloned())
            .map_err(|e| {
                DataFusionError::Internal(format!(
                    "Invalid projection, plan:{self:?}, projection:{projection:?}, err:{e:?}"
                ))
            })?;

        let opts = ReadOptions {
            deadline,
            read_parallelism,
            batch_size: state.config_options().execution.batch_size,
        };

        let request = ReadRequest {
            request_id,
            opts,
            projected_schema,
            predicate,
            metrics_collector: MetricsCollector::new(SCAN_TABLE_METRICS_COLLECTOR_NAME.to_string()),
        };

        self.build_plan(request, self.partition_info.clone()).await
    }

    async fn build_plan(
        &self,
        request: ReadRequest,
        partition_info: PartitionInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Build partition rule.
        let df_partition_rule =
            DfPartitionRuleAdapter::new(partition_info.clone(), &self.table.schema()).unwrap();

        // Evaluate expr and locate partition.
        let partitions = df_partition_rule
            .locate_partitions_for_read(request.predicate.exprs())
            .unwrap();
        let sub_tables = self.get_sub_table_idents(partition_info, partitions);

        // Build plan.
        let plan = UnresolvedPartitionedScan {
            sub_tables,
            read_request: request,
        };

        Ok(Arc::new(plan))
    }

    fn get_sub_table_idents(
        &self,
        partition_info: PartitionInfo,
        partitions: Vec<usize>,
    ) -> Vec<TableIdentifier> {
        let definitions = partition_info.get_definitions();
        partitions
            .into_iter()
            .map(|p| {
                let partition_name = &definitions[p].name;
                TableIdentifier {
                    catalog: self.table.catalog_name().to_string(),
                    schema: self.table.schema_name().to_string(),
                    table: format_sub_partition_table_name(self.table.name(), partition_name),
                }
            })
            .collect()
    }
}

#[async_trait]
impl TableProvider for TableProviderAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        // We use the `read_schema` as the schema of this `TableProvider`
        self.read_schema.clone().into_arrow_schema_ref()
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.scan_table(state, projection, filters, limit).await
    }

    fn supports_filter_pushdown(&self, _filter: &Expr) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }

    /// Get the type of this table for metadata/catalog purposes.
    fn table_type(&self) -> TableType {
        TableType::Base
    }
}

impl TableSource for TableProviderAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get a reference to the schema for this table
    fn schema(&self) -> SchemaRef {
        self.read_schema.clone().into_arrow_schema_ref()
    }

    /// Get the type of this table for metadata/catalog purposes.
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Tests whether the table provider can make use of a filter expression
    /// to optimize data retrieval.
    fn supports_filter_pushdown(&self, _filter: &Expr) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }
}
