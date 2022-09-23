// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Server error

use common_util::define_result;
use snafu::Snafu;
pub use tonic::Code;

define_result!(ServerError);

#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum ServerError {
    #[snafu(display("Rpc error, code:{:?}, message:{}", code, msg))]
    ErrNoCause { code: Code, msg: String },

    #[snafu(display("Rpc error, code:{:?}, message:{}, cause:{}", code, msg, source))]
    ErrWithCause {
        code: Code,
        msg: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl ServerError {
    pub fn code(&self) -> Code {
        match *self {
            ServerError::ErrNoCause { code, .. } => code,
            ServerError::ErrWithCause { code, .. } => code,
        }
    }

    /// Get the error message returned to the user.
    pub fn error_message(&self) -> String {
        match self {
            ServerError::ErrNoCause { msg, .. } => msg.clone(),

            ServerError::ErrWithCause { msg, source, .. } => {
                let err_string = source.to_string();
                let first_line = first_line_in_error(&err_string);
                format!("{}. Caused by: {}", msg, first_line)
            }
        }
    }
}

impl From<ServerError> for tonic::Status {
    fn from(srv_err: ServerError) -> Self {
        tonic::Status::new(srv_err.code(), srv_err.error_message())
    }
}

/// Returns first line in error message, now we use this hack to exclude
/// backtrace from error message that returned to user.
// TODO(yingwen): Consider a better way to get the error message.
pub(crate) fn first_line_in_error(err_string: &str) -> &str {
    err_string.split('\n').next().unwrap_or(err_string)
}
