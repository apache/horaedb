// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Error definitions for meta event service.

use common_util::{define_result, error::GenericError};
use proto::remote_engine::ResponseHeader;
use snafu::Snafu;

use crate::error_util;

define_result!(Error);

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Server error, code:{:?}, message:{}", code, msg))]
    ErrNoCause { code: StatusCode, msg: String },

    #[snafu(display("Server error, code:{:?}, message:{}, cause:{}", code, msg, source))]
    ErrWithCause {
        code: StatusCode,
        msg: String,
        source: GenericError,
    },
}

impl Error {
    pub fn code(&self) -> StatusCode {
        match *self {
            Error::ErrNoCause { code, .. } => code,
            Error::ErrWithCause { code, .. } => code,
        }
    }

    /// Get the error message returned to the user.
    pub fn error_message(&self) -> String {
        match self {
            Error::ErrNoCause { msg, .. } => msg.clone(),

            Error::ErrWithCause { msg, source, .. } => {
                let err_string = source.to_string();
                let first_line = error_util::remove_backtrace_from_err(&err_string);
                format!("{}. Caused by: {}", msg, first_line)
            }
        }
    }
}

/// A set of codes for meta event service.
///
/// Note that such a set of codes is different with the codes (alias to http
/// status code) used by storage service.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum StatusCode {
    #[default]
    Ok = 0,
    BadRequest = 401,
    NotFound = 404,
    Internal = 500,
}

impl StatusCode {
    #[inline]
    pub fn as_u32(self) -> u32 {
        self as u32
    }
}

pub fn build_err_header(err: Error) -> ResponseHeader {
    ResponseHeader {
        code: err.code().as_u32(),
        error: err.error_message(),
    }
}

pub fn build_ok_header() -> ResponseHeader {
    ResponseHeader {
        code: StatusCode::Ok.as_u32(),
        ..Default::default()
    }
}
