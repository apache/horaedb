// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Error definitions for storage service.

use ceresdbproto::common::ResponseHeader;

pub fn build_err_header(code: u32, msg: String) -> ResponseHeader {
    ResponseHeader { code, error: msg }
}
