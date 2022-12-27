// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Remote table engine implementation

mod channel;
mod client;
pub mod config;
mod statuts_code;

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use async_trait::async_trait;
use common_types::{record_batch::RecordBatch, schema::RecordSchema};
use config::Config;
use futures::{Stream, StreamExt};
use router::RouterRef;
use snafu::ResultExt;
use table_engine::{
    remote::{
        self,
        model::{ReadRequest, WriteRequest},
        RemoteEngine,
    },
    stream::{self, ErrWithSource, RecordBatchStream, SendableRecordBatchStream},
};

use self::client::{Client, ClientReadRecordBatchStream};

pub mod error {
    use common_util::define_result;
    use snafu::Snafu;
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
            "Failed to convert request or response, table, msg:{}, err:{}",
            msg,
            source
        ))]
        ConvertReadRequest {
            msg: String,
            source: Box<dyn std::error::Error + Send + Sync>,
        },

        #[snafu(display(
            "Failed to convert request or response, table, msg:{}, err:{}",
            msg,
            source
        ))]
        ConvertReadResponse {
            msg: String,
            source: Box<dyn std::error::Error + Send + Sync>,
        },

        #[snafu(display(
            "Failed to convert request or response, table, msg:{}, err:{}",
            msg,
            source
        ))]
        ConvertWriteRequest {
            msg: String,
            source: Box<dyn std::error::Error + Send + Sync>,
        },

        #[snafu(display("Failed to connect, table:{:?}, msg:{}, err:{}", table, msg, source))]
        Rpc {
            table: TableIdentifier,
            msg: String,
            source: tonic::Status,
        },

        #[snafu(display(
            "Failed query from table in server, table:{:?}, code:{}, msg:{}",
            table,
            code,
            msg
        ))]
        Server {
            table: TableIdentifier,
            code: u32,
            msg: String,
        },

        #[snafu(display("Failed to route table, table:{:?}, err:{}", table, source,))]
        RouteWithCause {
            table: TableIdentifier,
            source: router::Error,
        },

        #[snafu(display("Failed to route table, table:{:?}, msg:{}", table, msg,))]
        RouteNoCause { table: TableIdentifier, msg: String },
    }

    define_result!(Error);
}

pub struct RemoteEngineImpl(Client);

impl RemoteEngineImpl {
    pub fn new(config: Config, router: RouterRef) -> Self {
        let client = Client::new(config, router);

        Self(client)
    }
}

#[async_trait]
impl RemoteEngine for RemoteEngineImpl {
    /// Read from the remote engine
    async fn read(&self, request: ReadRequest) -> remote::Result<SendableRecordBatchStream> {
        let client_read_stream = self
            .0
            .read(request)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(remote::Read)?;
        Ok(Box::pin(RemoteReadRecordBatchStream(client_read_stream)))
    }

    /// Write to the remote engine
    async fn write(&self, request: WriteRequest) -> remote::Result<usize> {
        self.0
            .write(request)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(remote::Write)
    }
}

struct RemoteReadRecordBatchStream(ClientReadRecordBatchStream);

impl Stream for RemoteReadRecordBatchStream {
    type Item = stream::Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.0.poll_next_unpin(cx) {
            Poll::Ready(Some(result)) => {
                let result = result.map_err(|e| Box::new(e) as _).context(ErrWithSource {
                    msg: "polling remote read response failed",
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
