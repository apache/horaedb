// Copyright 2023 The HoraeDB Authors
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

use async_trait::async_trait;
use catalog::{
    schema,
    schema::{
        CreateOptions, CreateTableRequest, DropOptions, DropTableRequest, InvalidTableStatus,
        NameRef, Schema, SchemaRef,
    },
};
use cluster::{ClusterRef, TableStatus};
use table_engine::table::{SchemaId, TableRef};

/// A cluster-based implementation for [`schema`].

/// Schema with cluster.
/// It binds cluster and schema and will detect the health status of the cluster
/// when calling the schema interface.
pub(crate) struct SchemaWithCluster {
    internal: SchemaRef,

    cluster: ClusterRef,
}

impl SchemaWithCluster {
    pub(crate) fn new(internal: SchemaRef, cluster: ClusterRef) -> SchemaWithCluster {
        SchemaWithCluster { internal, cluster }
    }

    // Get table status, return None when table not found in shard.
    fn table_status(&self, table_name: NameRef) -> Option<TableStatus> {
        self.cluster.get_table_status(self.name(), table_name)
    }
}

#[async_trait]
impl Schema for SchemaWithCluster {
    fn name(&self) -> NameRef {
        self.internal.name()
    }

    fn id(&self) -> SchemaId {
        self.internal.id()
    }

    fn table_by_name(&self, name: NameRef) -> schema::Result<Option<TableRef>> {
        let find_table_result = self.internal.table_by_name(name)?;

        if find_table_result.is_none() {
            return match self.table_status(name) {
                // Table not found in schema and shard not contains this table.
                None => Ok(None),
                // Table not found in schema but shard contains this table.
                // Check the status of the shard.
                Some(table_status) => InvalidTableStatus {
                    status: format!("{:?}", table_status),
                }
                .fail()?,
            };
        }

        Ok(find_table_result)
    }

    async fn create_table(
        &self,
        request: CreateTableRequest,
        opts: CreateOptions,
    ) -> schema::Result<TableRef> {
        self.internal.create_table(request, opts).await
    }

    async fn drop_table(
        &self,
        request: DropTableRequest,
        opts: DropOptions,
    ) -> schema::Result<bool> {
        self.internal.drop_table(request, opts).await
    }

    fn all_tables(&self) -> schema::Result<Vec<TableRef>> {
        self.internal.all_tables()
    }

    fn register_table(&self, table: TableRef) {
        self.internal.register_table(table)
    }

    fn unregister_table(&self, table_name: &str) {
        self.internal.unregister_table(table_name)
    }
}
