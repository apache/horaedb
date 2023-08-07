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
use proxy::hotspot::{HotspotRecorder, Message};
use router::RouterRef;
use runtime::Runtime;
use snafu::ResultExt;
use table_engine::{
    remote::{
        self,
        model::{
            GetTableInfoRequest, ReadRequest, TableIdentifier, TableInfo, WriteBatchResult,
            WriteRequest,
        },
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

pub struct RemoteEngineImpl {
    client: Client,
    hotspot_recorder: Arc<HotspotRecorder>,
}

impl RemoteEngineImpl {
    pub fn new(
        config: Config,
        router: RouterRef,
        worker_runtime: Arc<Runtime>,
        hotspot_recorder: Arc<HotspotRecorder>,
    ) -> Self {
        let client = Client::new(config, router, worker_runtime);

        Self {
            client,
            hotspot_recorder,
        }
    }

    fn format_hot_key(table: &TableIdentifier) -> String {
        format!("{}/{}", table.schema, table.table)
    }

    async fn record_write(&self, request: &WriteRequest) {
        let hot_key = Self::format_hot_key(&request.table);
        let row_count = request.write_request.row_group.num_rows();
        let field_count = row_count * request.write_request.row_group.schema().num_columns();
        self.hotspot_recorder
            .send_msg_or_log(
                "inc_write_reqs",
                Message::Write {
                    key: hot_key,
                    row_count,
                    field_count,
                },
            )
            .await;
    }
}

#[async_trait]
impl RemoteEngine for RemoteEngineImpl {
    async fn read(&self, request: ReadRequest) -> remote::Result<SendableRecordBatchStream> {
        self.hotspot_recorder
            .send_msg_or_log(
                "inc_query_reqs",
                Message::Query(Self::format_hot_key(&request.table)),
            )
            .await;

        let client_read_stream = self
            .client
            .read(request)
            .await
            .box_err()
            .context(remote::Read)?;
        Ok(Box::pin(RemoteReadRecordBatchStream(client_read_stream)))
    }

    async fn write(&self, request: WriteRequest) -> remote::Result<usize> {
        self.record_write(&request).await;

        self.client
            .write(request)
            .await
            .box_err()
            .context(remote::Write)
    }

    async fn write_batch(
        &self,
        requests: Vec<WriteRequest>,
    ) -> remote::Result<Vec<WriteBatchResult>> {
        for req in &requests {
            self.record_write(req).await;
        }

        self.client
            .write_batch(requests)
            .await
            .box_err()
            .context(remote::Write)
    }

    async fn get_table_info(&self, request: GetTableInfoRequest) -> remote::Result<TableInfo> {
        self.client
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
