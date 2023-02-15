// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Payloads to write to wal

use ceresdbproto::{manifest as manifest_pb, table_requests};
use common_types::{
    bytes::{Buf, BufMut, SafeBuf, SafeBufMut},
    row::{RowGroup, RowGroupBuilder},
    schema::Schema,
};
use common_util::{
    codec::{row::WalRowDecoder, Decoder},
    define_result,
};
use prost::Message;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use wal::log_batch::{Payload, PayloadDecoder};

use crate::{table_options, TableOptions};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to encode header, err:{}", source))]
    EncodeHeader { source: common_types::bytes::Error },

    #[snafu(display("Failed to encode body, err:{}.\nBacktrace:\n{}", source, backtrace))]
    EncodeBody {
        source: prost::EncodeError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to decode header, err:{}", source))]
    DecodeHeader { source: common_types::bytes::Error },

    #[snafu(display(
        "Invalid wal entry header, value:{}.\nBacktrace:\n{}",
        value,
        backtrace
    ))]
    InvalidHeader { value: u8, backtrace: Backtrace },

    #[snafu(display("Failed to decode body, err:{}.\nBacktrace:\n{}", source, backtrace))]
    DecodeBody {
        source: prost::DecodeError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to decode schema, err:{}", source))]
    DecodeSchema { source: common_types::schema::Error },

    #[snafu(display("Failed to decode row, err:{}", source))]
    DecodeRow {
        source: common_util::codec::row::Error,
    },

    #[snafu(display(
        "Table schema is not found in the write request.\nBacktrace:\n{}",
        backtrace
    ))]
    TableSchemaNotFound { backtrace: Backtrace },

    #[snafu(display(
        "Table options is not found in the write request.\nBacktrace:\n{}",
        backtrace
    ))]
    TableOptionsNotFound { backtrace: Backtrace },

    #[snafu(display("Invalid table options, err:{}", source))]
    InvalidTableOptions { source: table_options::Error },
}

define_result!(Error);

/// Wal entry header
#[derive(Clone, Copy)]
enum Header {
    Write = 1,
    AlterSchema = 2,
    AlterOption = 3,
}

impl Header {
    pub fn to_u8(self) -> u8 {
        self as u8
    }

    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            value if value == Self::Write as u8 => Some(Self::Write),
            value if value == Self::AlterSchema as u8 => Some(Self::AlterSchema),
            value if value == Self::AlterOption as u8 => Some(Self::AlterOption),
            _ => None,
        }
    }
}

fn write_header<B: BufMut>(header: Header, buf: &mut B) -> Result<()> {
    buf.try_put_u8(header.to_u8()).context(EncodeHeader)
}

/// Header size in bytes
const HEADER_SIZE: usize = 1;

/// Write request to persist in wal
#[derive(Debug)]
pub enum WritePayload<'a> {
    Write(&'a table_requests::WriteRequest),
    AlterSchema(&'a manifest_pb::AlterSchemaMeta),
    AlterOption(&'a manifest_pb::AlterOptionsMeta),
}

impl<'a> Payload for WritePayload<'a> {
    type Error = Error;

    fn encode_size(&self) -> usize {
        let body_size = match self {
            WritePayload::Write(req) => req.encoded_len(),
            WritePayload::AlterSchema(req) => req.encoded_len(),
            WritePayload::AlterOption(req) => req.encoded_len(),
        };

        HEADER_SIZE + body_size
    }

    fn encode_to<B: BufMut>(&self, buf: &mut B) -> Result<()> {
        match self {
            WritePayload::Write(req) => {
                write_header(Header::Write, buf)?;
                req.encode(buf).context(EncodeBody)
            }
            WritePayload::AlterSchema(req) => {
                write_header(Header::AlterSchema, buf)?;
                req.encode(buf).context(EncodeBody)
            }
            WritePayload::AlterOption(req) => {
                write_header(Header::AlterOption, buf)?;
                req.encode(buf).context(EncodeBody)
            }
        }
    }
}

impl<'a> From<&'a table_requests::WriteRequest> for WritePayload<'a> {
    fn from(write_request: &'a table_requests::WriteRequest) -> Self {
        Self::Write(write_request)
    }
}

/// Payload decoded from wal
#[derive(Debug)]
pub enum ReadPayload {
    Write { row_group: RowGroup },
    AlterSchema { schema: Schema },
    AlterOptions { options: TableOptions },
}

impl ReadPayload {
    fn decode_write_from_pb(buf: &[u8]) -> Result<Self> {
        let write_req_pb: table_requests::WriteRequest =
            Message::decode(buf).context(DecodeBody)?;

        // Consume and convert schema in pb
        let schema: Schema = write_req_pb
            .schema
            .context(TableSchemaNotFound)?
            .try_into()
            .context(DecodeSchema)?;

        // Consume and convert rows in pb
        let encoded_rows = write_req_pb.rows;
        let mut builder = RowGroupBuilder::with_capacity(schema.clone(), encoded_rows.len());
        let row_decoder = WalRowDecoder::new(&schema);
        for row_bytes in &encoded_rows {
            let row = row_decoder
                .decode(&mut row_bytes.as_slice())
                .context(DecodeRow)?;
            // We skip schema check here
            builder.push_checked_row(row);
        }

        let row_group = builder.build();

        Ok(Self::Write { row_group })
    }

    fn decode_alter_schema_from_pb(buf: &[u8]) -> Result<Self> {
        let alter_schema_meta_pb: manifest_pb::AlterSchemaMeta =
            Message::decode(buf).context(DecodeBody)?;

        // Consume and convert schema in pb
        let schema: Schema = alter_schema_meta_pb
            .schema
            .context(TableSchemaNotFound)?
            .try_into()
            .context(DecodeSchema)?;

        Ok(Self::AlterSchema { schema })
    }

    fn decode_alter_option_from_pb(buf: &[u8]) -> Result<Self> {
        let alter_option_meta_pb: manifest_pb::AlterOptionsMeta =
            Message::decode(buf).context(DecodeBody)?;

        // Consume and convert options in pb
        let options: TableOptions = alter_option_meta_pb
            .options
            .context(TableOptionsNotFound)?
            .try_into()
            .context(InvalidTableOptions)?;

        Ok(Self::AlterOptions { options })
    }
}

/// Wal payload decoder
#[derive(Default)]
pub struct WalDecoder;

impl PayloadDecoder for WalDecoder {
    type Error = Error;
    type Target = ReadPayload;

    fn decode<B: Buf>(&self, buf: &mut B) -> Result<Self::Target> {
        let header_value = buf.try_get_u8().context(DecodeHeader)?;
        let header = match Header::from_u8(header_value) {
            Some(header) => header,
            None => {
                return InvalidHeader {
                    value: header_value,
                }
                .fail()
            }
        };

        let chunk = buf.chunk();
        let payload = match header {
            Header::Write => ReadPayload::decode_write_from_pb(chunk)?,
            Header::AlterSchema => ReadPayload::decode_alter_schema_from_pb(chunk)?,
            Header::AlterOption => ReadPayload::decode_alter_option_from_pb(chunk)?,
        };

        Ok(payload)
    }
}
