// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Remote table engine implementation

#![feature(let_chains)]

mod cached_router;
mod channel;
mod client;
pub mod config;
mod status_code;

use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use async_trait::async_trait;
use common_types::{record_batch::RecordBatch, schema::RecordSchema};
pub use config::Config;
use futures::{Stream, StreamExt};
use generic_error::BoxError;
use router::RouterRef;
use runtime::Runtime;
use snafu::ResultExt;
use table_engine::{
    remote::{
        self,
        model::{GetTableInfoRequest, ReadRequest, TableInfo, WriteBatchResult, WriteRequest},
        RemoteEngine,
    },
    stream::{self, ErrWithSource, RecordBatchStream, SendableRecordBatchStream},
};

use self::client::{Client, ClientReadRecordBatchStream};

pub mod error {
    use generic_error::GenericError;
    use macros::define_result;
    use snafu::{Backtrace, Snafu};
    use table_engine::remote::model::TableIdentifier;

    #[derive(Debug, Snafu)]
    #[snafu(visibility(pub))]
    pub enum Error {
        #[snafu(display("Failed to connect, addr:{}, msg:{}, err:{}", addr, msg, source))]
        BuildChannel {
            addr: String,
            msg: String,
            source: tonic::transport::Error,
        },

        #[snafu(display(
            "Invalid record batches number in the response, expect only one, given:{}.\nBacktrace:\n{}",
            batch_num,
            backtrace,
        ))]
        InvalidRecordBatchNumber {
            batch_num: usize,
            backtrace: Backtrace,
        },

        #[snafu(display("Failed to convert msg:{}, err:{}", msg, source))]
        Convert { msg: String, source: GenericError },

        #[snafu(display(
            "Failed to connect, table_idents:{:?}, msg:{}, err:{}",
            table_idents,
            msg,
            source
        ))]
        Rpc {
            table_idents: Vec<TableIdentifier>,
            msg: String,
            source: tonic::Status,
        },

        #[snafu(display(
            "Failed to query from table in server, table_idents:{:?}, code:{}, msg:{}",
            table_idents,
            code,
            msg
        ))]
        Server {
            table_idents: Vec<TableIdentifier>,
            code: u32,
            msg: String,
        },

        #[snafu(display("Failed to route table, table_ident:{:?}, err:{}", table_ident, source,))]
        RouteWithCause {
            table_ident: TableIdentifier,
            source: router::Error,
        },

        #[snafu(display("Failed to route table, table_ident:{:?}, msg:{}", table_ident, msg,))]
        RouteNoCause {
            table_ident: TableIdentifier,
            msg: String,
        },
    }

    define_result!(Error);
}

pub struct RemoteEngineImpl(Client);

impl RemoteEngineImpl {
    pub fn new(config: Config, router: RouterRef, worker_runtime: Arc<Runtime>) -> Self {
        let client = Client::new(config, router, worker_runtime);

        Self(client)
    }
}

#[async_trait]
impl RemoteEngine for RemoteEngineImpl {
    async fn read(&self, request: ReadRequest) -> remote::Result<SendableRecordBatchStream> {
        let client_read_stream = self.0.read(request).await.box_err().context(remote::Read)?;
        Ok(Box::pin(RemoteReadRecordBatchStream(client_read_stream)))
    }

    async fn write(&self, request: WriteRequest) -> remote::Result<usize> {
        self.0.write(request).await.box_err().context(remote::Write)
    }

    async fn write_batch(
        &self,
        requests: Vec<WriteRequest>,
    ) -> remote::Result<Vec<WriteBatchResult>> {
        self.0
            .write_batch(requests)
            .await
            .box_err()
            .context(remote::Write)
    }

    async fn get_table_info(&self, request: GetTableInfoRequest) -> remote::Result<TableInfo> {
        self.0
            .get_table_info(request)
            .await
            .box_err()
            .context(remote::GetTableInfo)
    }
}

struct RemoteReadRecordBatchStream(ClientReadRecordBatchStream);

impl Stream for RemoteReadRecordBatchStream {
    type Item = stream::Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.0.poll_next_unpin(cx) {
            Poll::Ready(Some(result)) => {
                let result = result.box_err().context(ErrWithSource {
                    msg: "poll read response failed",
                });

                Poll::Ready(Some(result))
            }

            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for RemoteReadRecordBatchStream {
    fn schema(&self) -> &RecordSchema {
        &self.0.projected_record_schema
    }
}
