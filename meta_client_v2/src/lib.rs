// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use async_trait::async_trait;
use common_util::define_result;
use snafu::{Backtrace, Snafu};
use types::{
    ActionCmd, AllocSchemaIdRequest, AllocSchemaIdResponse, AllocTableIdRequest,
    AllocTableIdResponse, DropTableRequest, DropTableResponse, GetTablesRequest, GetTablesResponse,
    ResponseHeader, ShardInfo,
};

pub mod meta_impl;
pub mod types;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(display(
        "Failed to fetch action cmd, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    FetchActionCmd {
        source: grpcio::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to init heatbeat stream, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    InitHeartBeatStream {
        source: grpcio::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to get grpc client, grpc client is not inited.\nBacktrace:\n{}",
        backtrace
    ))]
    FailGetGrpcClient { backtrace: Backtrace },

    #[snafu(display("Failed to send heartbeat, cluster:{}, err:{}", cluster, source))]
    FailSendHeartbeat {
        cluster: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to alloc schema id, err:{}", source))]
    FailAllocSchemaId {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to alloc table id, err:{}", source))]
    FailAllocTableId {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to drop table, err:{}", source))]
    FailDropTable {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to get tables, err:{}", source))]
    FailGetTables {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Meta rpc error, resp header:{:?}.\nBacktrace:\n{}", header, backtrace))]
    MetaRpc {
        header: ResponseHeader,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Handle event failed, handler:{}, event:{:?}, err:{}",
        name,
        event,
        source
    ))]
    FailHandleEvent {
        name: String,
        event: ActionCmd,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

define_result!(Error);

pub type EventHandlerRef = Arc<dyn EventHandler + Send + Sync>;

#[async_trait]
pub trait EventHandler {
    fn name(&self) -> &str;

    async fn handle(
        &self,
        event: &ActionCmd,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// MetaClient is the abstraction of client used to communicate with CeresMeta
/// cluster.
#[async_trait]
pub trait MetaClient: Send + Sync {
    /// Start the meta client and the events will occur afterwards.
    async fn start(&self) -> Result<()>;
    /// Stop the meta client and release all the resources.
    async fn stop(&self) -> Result<()>;

    /// Register handler for the event.
    ///
    /// It is better to register handlers before calling `start`.
    async fn register_event_handler(&self, handler: EventHandlerRef) -> Result<()>;

    async fn alloc_schema_id(&self, req: AllocSchemaIdRequest) -> Result<AllocSchemaIdResponse>;

    async fn alloc_table_id(&self, req: AllocTableIdRequest) -> Result<AllocTableIdResponse>;

    async fn drop_table(&self, req: DropTableRequest) -> Result<DropTableResponse>;

    async fn get_tables(&self, req: GetTablesRequest) -> Result<GetTablesResponse>;

    async fn send_heartbeat(&self, req: Vec<ShardInfo>) -> Result<()>;
}

pub type MetaClientRef = Arc<dyn MetaClient>;
