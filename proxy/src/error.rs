// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use ceresdbproto::common::ResponseHeader;
use generic_error::GenericError;
use http::StatusCode;
use macros::define_result;
use snafu::{Backtrace, Snafu};

use crate::error_util;

define_result!(Error);

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Internal error, msg:{}, err:{}", msg, source))]
    Internal { msg: String, source: GenericError },

    #[snafu(display("Internal error, msg:{}.\nBacktrace:\n{}", msg, backtrace))]
    InternalNoCause { msg: String, backtrace: Backtrace },

    #[snafu(display("Rpc error, code:{:?}, err:{}", code, msg))]
    ErrNoCause { code: StatusCode, msg: String },

    #[snafu(display("Rpc error, code:{:?}, message:{}, err:{}", code, msg, source))]
    ErrWithCause {
        code: StatusCode,
        msg: String,
        source: GenericError,
    },

    #[snafu(display("Query warning, msg:{msg}"))]
    QueryMaybeExceedTTL { msg: String },
}

impl Error {
    pub fn code(&self) -> StatusCode {
        match *self {
            Error::ErrNoCause { code, .. } => code,
            Error::ErrWithCause { code, .. } => code,
            Error::QueryMaybeExceedTTL { .. } => StatusCode::OK,
            Error::Internal { .. } | Error::InternalNoCause { .. } => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }

    /// Get the error message returned to the user.
    pub fn error_message(&self) -> String {
        match self {
            Error::ErrNoCause { msg, .. } | Error::InternalNoCause { msg, .. } => msg.clone(),

            Error::ErrWithCause { msg, source, .. } | Error::Internal { msg, source, .. } => {
                let err_string = source.to_string();
                let first_line = error_util::remove_backtrace_from_err(&err_string);
                format!("{msg}. Caused by: {first_line}")
            }
            Error::QueryMaybeExceedTTL { msg } => msg.clone(),
        }
    }
}

pub fn build_err_header(err: Error) -> ResponseHeader {
    ResponseHeader {
        code: err.code().as_u16() as u32,
        error: err.error_message(),
    }
}

pub fn build_ok_header() -> ResponseHeader {
    ResponseHeader {
        code: StatusCode::OK.as_u16() as u32,
        ..Default::default()
    }
}

impl From<router::Error> for Error {
    fn from(route_err: router::Error) -> Self {
        match &route_err {
            router::Error::RouteNotFound { .. } | router::Error::ShardNotFound { .. } => {
                Error::ErrNoCause {
                    code: StatusCode::NOT_FOUND,
                    msg: route_err.to_string(),
                }
            }
            router::Error::ParseEndpoint { .. }
            | router::Error::OtherWithCause { .. }
            | router::Error::OtherNoCause { .. } => Error::ErrNoCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: route_err.to_string(),
            },
        }
    }
}
