// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Model for remote table engine

use crate::table::{ReadRequest as TableReadRequest, WriteRequest as TableWriteRequest};

#[allow(dead_code)]
pub struct TableIdentifier {
    pub catalog: String,
    pub schema: String,
    pub table: String,
}

#[allow(dead_code)]
pub struct ReadRequest {
    pub table: TableIdentifier,
    pub table_request: TableReadRequest,
}

#[allow(dead_code)]
pub struct WriteRequest {
    pub table: TableIdentifier,
    pub table_request: TableWriteRequest,
}
