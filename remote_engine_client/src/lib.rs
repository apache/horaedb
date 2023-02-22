// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Remote table engine implementation

#![feature(let_chains)]

mod cached_router;
mod channel;
mod client;
pub mod config;
mod status_code;

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use async_trait::async_trait;
use common_types::{record_batch::RecordBatch, schema::RecordSchema};
use common_util::error::BoxError;
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
    use common_util::{define_result, error::GenericError};
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
            "Failed to convert request or response, table, msg:{}, err:{}",
            msg,
            source
        ))]
        ConvertReadRequest { msg: String, source: GenericError },

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
            "Failed to connect, table_ident:{:?}, msg:{}, err:{}",
            table_ident,
            msg,
            source
        ))]
        Rpc {
            table_ident: TableIdentifier,
            msg: String,
            source: tonic::Status,
        },

        #[snafu(display(
            "Failed to query from table in server, table_ident:{:?}, code:{}, msg:{}",
            table_ident,
            code,
            msg
        ))]
        Server {
            table_ident: TableIdentifier,
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
    pub fn new(config: Config, router: RouterRef) -> Self {
        let client = Client::new(config, router);

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
