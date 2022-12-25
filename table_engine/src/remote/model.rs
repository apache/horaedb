// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Model for remote table engine

use snafu::{OptionExt, ResultExt};

use crate::{
    remote::{ConvertTableReadRequest, EmptyTableIdentifier, EmptyTableReadRequest, Error},
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
