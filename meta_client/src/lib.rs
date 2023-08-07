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

use std::sync::Arc;

use async_trait::async_trait;
use generic_error::GenericError;
use macros::define_result;
use snafu::{Backtrace, Snafu};
use types::{
    AllocSchemaIdRequest, AllocSchemaIdResponse, CreateTableRequest, CreateTableResponse,
    DropTableRequest, DropTableResponse, GetNodesRequest, GetNodesResponse,
    GetTablesOfShardsRequest, GetTablesOfShardsResponse, RouteTablesRequest, RouteTablesResponse,
    ShardInfo,
};

pub mod meta_impl;
pub mod types;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(display("{msg}, err:{source}"))]
    Convert { msg: String, source: GenericError },

    #[snafu(display("Missing shard info, msg:{}.\nBacktrace:\n{}", msg, backtrace))]
    MissingShardInfo { msg: String, backtrace: Backtrace },

    #[snafu(display("Missing table info, msg:{}.\nBacktrace:\n{}", msg, backtrace))]
    MissingTableInfo { msg: String, backtrace: Backtrace },

    #[snafu(display("Missing header in rpc response.\nBacktrace:\n{}", backtrace))]
    MissingHeader { backtrace: Backtrace },

    #[snafu(display(
        "Failed to connect the service endpoint:{}, err:{}\nBacktrace:\n{}",
        addr,
        source,
        backtrace
    ))]
    FailConnect {
        addr: String,
        source: GenericError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to send heartbeat, cluster:{}, err:{}", cluster, source))]
    FailSendHeartbeat {
        cluster: String,
        source: GenericError,
    },

    #[snafu(display("Failed to alloc schema id, err:{}", source))]
    FailAllocSchemaId { source: GenericError },

    #[snafu(display("Failed to alloc table id, err:{}", source))]
    FailCreateTable { source: GenericError },

    #[snafu(display("Failed to drop table, err:{}", source))]
    FailDropTable { source: GenericError },

    #[snafu(display("Failed to get tables, err:{}", source))]
    FailGetTables { source: GenericError },

    #[snafu(display("Failed to route tables, err:{}", source))]
    FailRouteTables { source: GenericError },

    #[snafu(display(
        "Bad response, resp code:{}, msg:{}.\nBacktrace:\n{}",
        code,
        msg,
        backtrace
    ))]
    BadResponse {
        code: u32,
        msg: String,
        backtrace: Backtrace,
    },
}

define_result!(Error);

/// MetaClient is the abstraction of client used to communicate with CeresMeta
/// cluster.
#[async_trait]
pub trait MetaClient: Send + Sync {
    async fn alloc_schema_id(&self, req: AllocSchemaIdRequest) -> Result<AllocSchemaIdResponse>;

    async fn create_table(&self, req: CreateTableRequest) -> Result<CreateTableResponse>;

    async fn drop_table(&self, req: DropTableRequest) -> Result<DropTableResponse>;

    async fn get_tables_of_shards(
        &self,
        req: GetTablesOfShardsRequest,
    ) -> Result<GetTablesOfShardsResponse>;

    async fn route_tables(&self, req: RouteTablesRequest) -> Result<RouteTablesResponse>;

    async fn get_nodes(&self, req: GetNodesRequest) -> Result<GetNodesResponse>;

    async fn send_heartbeat(&self, req: Vec<ShardInfo>) -> Result<()>;
}

pub type MetaClientRef = Arc<dyn MetaClient>;
