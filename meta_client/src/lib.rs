// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use async_trait::async_trait;
use common_util::{define_result, error::GenericError};
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
