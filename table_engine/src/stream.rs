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

//! Table record stream

use std::{
    convert::TryFrom,
    pin::Pin,
    task::{Context, Poll},
};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch as ArrowRecordBatch};
use common_types::{record_batch::RecordBatch, schema::RecordSchema};
use datafusion::{
    error::{DataFusionError, Result as DataFusionResult, Result as DfResult},
    physical_plan::{
        RecordBatchStream as DfRecordBatchStream,
        SendableRecordBatchStream as DfSendableRecordBatchStream,
    },
};
use futures::stream::Stream;
use generic_error::{BoxError, GenericError};
use macros::define_result;
use snafu::{Backtrace, ResultExt, Snafu};

use crate::table;

// TODO(yingwen): Classify the error.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Stream error, msg:{}, err:{}", msg, source))]
    ErrWithSource { msg: String, source: GenericError },

    #[snafu(display("Stream error, msg:{}.\nBacktrace:\n{}", msg, backtrace))]
    ErrNoSource { msg: String, backtrace: Backtrace },
}

define_result!(Error);

pub trait RecordBatchStream: Stream<Item = Result<RecordBatch>> {
    fn schema(&self) -> &RecordSchema;
}

pub type SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream + Send>>;

/// Record batch streams divided by time range.
pub struct PartitionedStreams {
    pub streams: Vec<SendableRecordBatchStream>,
}

impl PartitionedStreams {
    pub fn one_stream(stream: SendableRecordBatchStream) -> Self {
        Self {
            streams: vec![stream],
        }
    }
}

pub struct ToDfStream(pub SendableRecordBatchStream);

impl Stream for ToDfStream {
    type Item = DataFusionResult<ArrowRecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.0.as_mut().poll_next(ctx) {
            Poll::Ready(Some(Ok(record_batch))) => {
                Poll::Ready(Some(Ok(record_batch.into_arrow_record_batch())))
            }
            Poll::Ready(Some(Err(e))) => {
                Poll::Ready(Some(Err(DataFusionError::External(Box::new(e)))))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl DfRecordBatchStream for ToDfStream {
    fn schema(&self) -> SchemaRef {
        self.0.schema().to_arrow_schema_ref()
    }
}

pub struct FromDfStream {
    schema: RecordSchema,
    df_stream: DfSendableRecordBatchStream,
}

impl FromDfStream {
    pub fn new(df_stream: DfSendableRecordBatchStream) -> Result<Self> {
        let df_schema = df_stream.schema();
        let schema = RecordSchema::try_from(df_schema)
            .box_err()
            .context(ErrWithSource {
                msg: "convert record schema",
            })?;

        Ok(Self { schema, df_stream })
    }
}

impl Stream for FromDfStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.df_stream.as_mut().poll_next(ctx) {
            Poll::Ready(Some(record_batch_res)) => Poll::Ready(Some(
                record_batch_res
                    .box_err()
                    .and_then(|batch| RecordBatch::try_from(batch).box_err())
                    .context(ErrWithSource {
                        msg: "convert from arrow record batch",
                    }),
            )),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for FromDfStream {
    fn schema(&self) -> &RecordSchema {
        &self.schema
    }
}

#[derive(Default)]
pub struct ScanStreamState {
    inited: bool,
    err: Option<table::Error>,
    pub streams: Vec<Option<SendableRecordBatchStream>>,
}

impl ScanStreamState {
    pub fn init(&mut self, streams_result: table::Result<PartitionedStreams>) {
        self.inited = true;
        match streams_result {
            Ok(partitioned_streams) => {
                self.streams = partitioned_streams.streams.into_iter().map(Some).collect()
            }
            Err(e) => self.err = Some(e),
        }
    }

    pub fn is_inited(&self) -> bool {
        self.inited
    }

    pub fn take_stream(&mut self, index: usize) -> DfResult<SendableRecordBatchStream> {
        if let Some(e) = &self.err {
            return Err(DataFusionError::Execution(format!(
                "Failed to read table, partition:{index}, err:{e}"
            )));
        }

        // TODO(yingwen): Return an empty stream if index is out of bound.
        self.streams[index].take().ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Read partition multiple times is not supported, partition:{index}"
            ))
        })
    }
}
