// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Model for remote table engine

use common_types::schema::Schema;
use common_util::avro;
use snafu::{OptionExt, ResultExt};

use crate::{
    remote::{
        ConvertRowGroup, ConvertTableReadRequest, ConvertTableSchema, EmptyRowGroup,
        EmptyTableIdentifier, EmptyTableReadRequest, EmptyTableSchema, Error,
    },
    table::{ReadRequest as TableReadRequest, WriteRequest as TableWriteRequest},
};

#[allow(dead_code)]
#[derive(Debug)]
pub struct TableIdentifier {
    pub catalog: String,
    pub schema: String,
    pub table: String,
}

impl From<proto::remote_engine::TableIdentifier> for TableIdentifier {
    fn from(pb: proto::remote_engine::TableIdentifier) -> Self {
        Self {
            catalog: pb.catalog,
            schema: pb.schema,
            table: pb.table,
        }
    }
}

#[allow(dead_code)]
pub struct ReadRequest {
    pub table: TableIdentifier,
    pub read_request: TableReadRequest,
}

impl TryFrom<proto::remote_engine::ReadRequest> for ReadRequest {
    type Error = Error;

    fn try_from(pb: proto::remote_engine::ReadRequest) -> Result<Self, Self::Error> {
        let table_identifier = pb.table.context(EmptyTableIdentifier)?;
        let table_read_request = pb.read_request.context(EmptyTableReadRequest)?;
        Ok(Self {
            table: table_identifier.into(),
            read_request: table_read_request
                .try_into()
                .map_err(|e| Box::new(e) as _)
                .context(ConvertTableReadRequest)?,
        })
    }
}

#[allow(dead_code)]
pub struct WriteRequest {
    pub table: TableIdentifier,
    pub write_request: TableWriteRequest,
}

impl TryFrom<proto::remote_engine::WriteRequest> for WriteRequest {
    type Error = Error;

    fn try_from(pb: proto::remote_engine::WriteRequest) -> Result<Self, Self::Error> {
        let table_identifier = pb.table.context(EmptyTableIdentifier)?;
        let row_group = pb.row_group.context(EmptyRowGroup)?;
        let table_schema: Schema = row_group
            .table_schema
            .context(EmptyTableSchema)?
            .try_into()
            .map_err(|e| Box::new(e) as _)
            .context(ConvertTableSchema)?;
        Ok(Self {
            table: table_identifier.into(),
            write_request: TableWriteRequest {
                row_group: avro::convert_avro_rows_to_row_group(table_schema, &row_group.rows)
                    .map_err(|e| Box::new(e) as _)
                    .context(ConvertRowGroup)?,
            },
        })
    }
}
