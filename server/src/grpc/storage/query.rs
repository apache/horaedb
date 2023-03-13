// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use arrow_ext::ipc::{CompressOptions, CompressionMethod, RecordBatchesEncoder};
use ceresdbproto::{
    common::ResponseHeader,
    storage::{arrow_payload, sql_query_response, ArrowPayload, SqlQueryResponse},
};
use common_types::record_batch::RecordBatch;
use common_util::error::BoxError;
use http::StatusCode;
use interpreters::interpreter::Output;
use snafu::ResultExt;

use crate::grpc::storage::error::{ErrWithCause, Result};

// TODO(chenxiang): Output can have both `rows` and `affected_rows`
pub fn convert_output(
    output: &Output,
    resp_compress_min_length: usize,
) -> Result<SqlQueryResponse> {
    match output {
        Output::Records(batches) => {
            let mut writer = QueryResponseWriter::new(resp_compress_min_length);
            writer.write_batches(batches)?;
            writer.finish()
        }
        Output::AffectedRows(rows) => {
            Ok(QueryResponseBuilder::with_ok_header().build_with_affected_rows(*rows))
        }
    }
}

/// Builder for building [`SqlQueryResponse`].
#[derive(Debug, Default)]
pub struct QueryResponseBuilder {
    header: ResponseHeader,
}

impl QueryResponseBuilder {
    pub fn with_ok_header() -> Self {
        let header = ResponseHeader {
            code: StatusCode::OK.as_u16() as u32,
            ..Default::default()
        };
        Self { header }
    }

    pub fn build_with_affected_rows(self, affected_rows: usize) -> SqlQueryResponse {
        let output = Some(sql_query_response::Output::AffectedRows(
            affected_rows as u32,
        ));
        SqlQueryResponse {
            header: Some(self.header),
            output,
        }
    }

    pub fn build_with_empty_arrow_payload(self) -> SqlQueryResponse {
        let payload = ArrowPayload {
            record_batches: Vec::new(),
            compression: arrow_payload::Compression::None as i32,
        };
        self.build_with_arrow_payload(payload)
    }

    pub fn build_with_arrow_payload(self, payload: ArrowPayload) -> SqlQueryResponse {
        let output = Some(sql_query_response::Output::Arrow(payload));
        SqlQueryResponse {
            header: Some(self.header),
            output,
        }
    }
}

/// Writer for encoding multiple [`RecordBatch`]es to the [`SqlQueryResponse`].
///
/// Whether to do compression depends on the size of the encoded bytes.
pub struct QueryResponseWriter {
    encoder: RecordBatchesEncoder,
}

impl QueryResponseWriter {
    pub fn new(compress_min_length: usize) -> Self {
        let compress_opts = CompressOptions {
            compress_min_length,
            method: CompressionMethod::Zstd,
        };
        Self {
            encoder: RecordBatchesEncoder::new(compress_opts),
        }
    }

    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.encoder
            .write(batch.as_arrow_record_batch())
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "failed to encode record batch".to_string(),
            })
    }

    pub fn write_batches(&mut self, record_batch: &[RecordBatch]) -> Result<()> {
        for batch in record_batch {
            self.write(batch)?;
        }

        Ok(())
    }

    pub fn finish(self) -> Result<SqlQueryResponse> {
        let compress_output = self
            .encoder
            .finish()
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "failed to encode record batch".to_string(),
            })?;

        if compress_output.payload.is_empty() {
            return Ok(QueryResponseBuilder::with_ok_header().build_with_empty_arrow_payload());
        }

        let compression = match compress_output.method {
            CompressionMethod::None => arrow_payload::Compression::None,
            CompressionMethod::Zstd => arrow_payload::Compression::Zstd,
        };
        let resp = QueryResponseBuilder::with_ok_header().build_with_arrow_payload(ArrowPayload {
            record_batches: vec![compress_output.payload],
            compression: compression as i32,
        });

        Ok(resp)
    }
}
