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

//! Partition table engine implementations

mod error;
mod metrics;
mod partition;
pub mod scan_builder;
pub mod test_util;

use std::sync::Arc;

use analytic_engine::TableOptions;
use async_trait::async_trait;
use datafusion::logical_expr::expr::{Expr, InList};
use generic_error::BoxError;
use snafu::{ensure, OptionExt, ResultExt};
use table_engine::{
    engine::{
        CloseShardRequest, CloseTableRequest, CreateTableParams, CreateTableRequest,
        DropTableRequest, InvalidPartitionContext, OpenShardRequest, OpenShardResult,
        OpenTableRequest, Result, TableEngine, Unexpected, UnexpectedNoCause,
    },
    partition::rule::df_adapter::PartitionedFilterKeyIndex,
    predicate::Predicate,
    remote::RemoteEngineRef,
    table::TableRef,
    PARTITION_TABLE_ENGINE_TYPE,
};

use crate::partition::{PartitionTableImpl, TableData};

/// Partition table engine implementation.
pub struct PartitionTableEngine {
    remote_engine_ref: RemoteEngineRef,
}

impl PartitionTableEngine {
    pub fn new(remote_engine_ref: RemoteEngineRef) -> Self {
        Self { remote_engine_ref }
    }
}

#[async_trait]
impl TableEngine for PartitionTableEngine {
    fn engine_type(&self) -> &str {
        PARTITION_TABLE_ENGINE_TYPE
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }

    /// Validate the request of create table.
    async fn validate_create_table(&self, _params: &CreateTableParams) -> Result<()> {
        Ok(())
    }

    async fn create_table(&self, request: CreateTableRequest) -> Result<TableRef> {
        let table_data = TableData {
            catalog_name: request.params.catalog_name,
            schema_name: request.params.schema_name,
            table_name: request.params.table_name,
            table_id: request.table_id,
            table_schema: request.params.table_schema,
            partition_info: request.params.partition_info.context(UnexpectedNoCause {
                msg: "partition info not found",
            })?,
            options: TableOptions::from_map(&request.params.table_options, true)
                .box_err()
                .context(Unexpected)?,
            engine_type: request.params.engine,
        };
        Ok(Arc::new(
            PartitionTableImpl::new(table_data, self.remote_engine_ref.clone())
                .box_err()
                .context(Unexpected)?,
        ))
    }

    async fn drop_table(&self, _request: DropTableRequest) -> Result<bool> {
        Ok(true)
    }

    async fn open_table(&self, _request: OpenTableRequest) -> Result<Option<TableRef>> {
        Ok(None)
    }

    async fn close_table(&self, _request: CloseTableRequest) -> Result<()> {
        Ok(())
    }

    async fn open_shard(&self, _request: OpenShardRequest) -> Result<OpenShardResult> {
        Ok(OpenShardResult::default())
    }

    async fn close_shard(&self, _request: CloseShardRequest) -> Vec<Result<String>> {
        vec![Ok("".to_string())]
    }
}

pub fn partitioned_predicates(
    predicate: Arc<Predicate>,
    partitions: &[usize],
    partitioned_key_indices: &mut PartitionedFilterKeyIndex,
) -> Result<Vec<Predicate>> {
    ensure!(
        partitions.len() == partitioned_key_indices.keys().len(),
        InvalidPartitionContext {
            msg: format!(
                "partitions length:{}, partitioned_key_indices length: {}",
                partitions.len(),
                partitioned_key_indices.keys().len()
            )
        }
    );
    let mut predicates = vec![(*predicate).clone(); partitions.len()];
    for (idx, predicate) in predicates.iter_mut().enumerate() {
        let partition = partitions[idx];
        if let Some(filter_indices) = partitioned_key_indices.get(&partition) {
            let exprs = predicate.mut_exprs();
            for (filter_idx, key_indices) in filter_indices {
                if let Expr::InList(InList {
                    list,
                    negated: false,
                    ..
                }) = &mut exprs[*filter_idx]
                {
                    let mut idx = 0;
                    list.retain(|_| {
                        let should_kept = key_indices.contains(&idx);
                        idx += 1;
                        should_kept
                    });
                }
            }
        }
    }
    Ok(predicates)
}
