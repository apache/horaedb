// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Table record stream

use std::{
    convert::TryFrom,
    pin::Pin,
    task::{Context, Poll},
};

use arrow::{
    datatypes::SchemaRef,
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch as ArrowRecordBatch,
};
use common_types::{record_batch::RecordBatch, schema::RecordSchema};
use common_util::define_result;
use datafusion::physical_plan::{
    RecordBatchStream as DfRecordBatchStream,
    SendableRecordBatchStream as DfSendableRecordBatchStream,
};
use futures::stream::Stream;
use snafu::{Backtrace, ResultExt, Snafu};

// TODO(yingwen): Classify the error.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Stream error, msg:{}, err:{}", msg, source))]
    ErrWithSource {
        msg: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

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
    type Item = ArrowResult<ArrowRecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.0.as_mut().poll_next(ctx) {
            Poll::Ready(Some(Ok(record_batch))) => {
                Poll::Ready(Some(Ok(record_batch.into_arrow_record_batch())))
            }
            Poll::Ready(Some(Err(e))) => {
                Poll::Ready(Some(Err(ArrowError::ExternalError(Box::new(e)))))
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
            .map_err(|e| Box::new(e) as _)
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
                    .map_err(|e| Box::new(e) as _)
                    .and_then(|batch| RecordBatch::try_from(batch).map_err(|e| Box::new(e) as _))
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
