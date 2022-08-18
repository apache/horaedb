// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Payloads to write to wal

use std::convert::TryInto;

use common_types::{
    bytes::{MemBuf, MemBufMut, Writer},
    row::{RowGroup, RowGroupBuilder},
    schema::Schema,
};
use common_util::{
    codec::{row::WalRowDecoder, Decoder},
    define_result,
};
use proto::{meta_update, table_requests};
use protobuf::Message;
use snafu::{Backtrace, ResultExt, Snafu};
use wal::log_batch::{Payload, PayloadDecoder};

use crate::TableOptions;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to encode header, err:{}", source))]
    EncodeHeader { source: common_types::bytes::Error },

    #[snafu(display("Failed to encode body, err:{}.\nBacktrace:\n{}", source, backtrace))]
    EncodeBody {
        source: protobuf::ProtobufError,
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
        source: protobuf::ProtobufError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to decode schema, err:{}", source))]
    DecodeSchema { source: common_types::schema::Error },

    #[snafu(display("Failed to decode row, err:{}", source))]
    DecodeRow {
        source: common_util::codec::row::Error,
    },
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

fn write_header(header: Header, buf: &mut dyn MemBufMut) -> Result<()> {
    buf.write_u8(header.to_u8()).context(EncodeHeader)?;
    Ok(())
}

/// Header size in bytes
const HEADER_SIZE: usize = 1;

/// Write request to persist in wal
#[derive(Debug)]
pub enum WritePayload<'a> {
    Write(&'a table_requests::WriteRequest),
    AlterSchema(&'a meta_update::AlterSchemaMeta),
    AlterOption(&'a meta_update::AlterOptionsMeta),
}

impl<'a> Payload for WritePayload<'a> {
    type Error = Error;

    fn encode_size(&self) -> usize {
        let body_size = match self {
            WritePayload::Write(req) => req.compute_size(),
            WritePayload::AlterSchema(req) => req.compute_size(),
            WritePayload::AlterOption(req) => req.compute_size(),
        };

        HEADER_SIZE + body_size as usize
    }

    fn encode_to<B: MemBufMut>(&self, buf: &mut B) -> Result<()> {
        match self {
            WritePayload::Write(req) => {
                write_header(Header::Write, buf)?;
                let mut writer = Writer::new(buf);
                req.write_to_writer(&mut writer).context(EncodeBody)
            }
            WritePayload::AlterSchema(req) => {
                write_header(Header::AlterSchema, buf)?;
                let mut writer = Writer::new(buf);
                req.write_to_writer(&mut writer).context(EncodeBody)
            }
            WritePayload::AlterOption(req) => {
                write_header(Header::AlterOption, buf)?;
                let mut writer = Writer::new(buf);
                req.write_to_writer(&mut writer).context(EncodeBody)
            }
        }
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
        let mut write_req_pb: table_requests::WriteRequest =
            Message::parse_from_bytes(buf).context(DecodeBody)?;

        // Consume and convert schema in pb
        let schema: Schema = write_req_pb
            .take_schema()
            .try_into()
            .context(DecodeSchema)?;

        // Consume and convert rows in pb
        let encoded_rows = write_req_pb.take_rows().into_vec();
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
        let mut alter_schema_meta_pb: meta_update::AlterSchemaMeta =
            Message::parse_from_bytes(buf).context(DecodeBody)?;

        // Consume and convert schema in pb
        let schema: Schema = alter_schema_meta_pb
            .take_schema()
            .try_into()
            .context(DecodeSchema)?;

        Ok(Self::AlterSchema { schema })
    }

    fn decode_alter_option_from_pb(buf: &[u8]) -> Result<Self> {
        let mut alter_option_meta_pb: meta_update::AlterOptionsMeta =
            Message::parse_from_bytes(buf).context(DecodeBody)?;

        // Consume and convert options in pb
        let options: TableOptions = alter_option_meta_pb.take_options().into();

        Ok(Self::AlterOptions { options })
    }
}

/// Wal payload decoder
#[derive(Default)]
pub struct WalDecoder;

impl PayloadDecoder for WalDecoder {
    type Error = Error;
    type Target = ReadPayload;

    fn decode<B: MemBuf>(&self, buf: &mut B) -> Result<Self::Target> {
        let header_value = buf.read_u8().context(DecodeHeader)?;
        let header = match Header::from_u8(header_value) {
            Some(header) => header,
            None => {
                return InvalidHeader {
                    value: header_value,
                }
                .fail()
            }
        };

        let payload = match header {
            Header::Write => ReadPayload::decode_write_from_pb(buf.remaining_slice())?,
            Header::AlterSchema => ReadPayload::decode_alter_schema_from_pb(buf.remaining_slice())?,
            Header::AlterOption => ReadPayload::decode_alter_option_from_pb(buf.remaining_slice())?,
        };

        Ok(payload)
    }
}
