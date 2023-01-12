// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::fmt;

use snafu::{Backtrace, GenerateBacktrace};

pub type GenericError = Box<dyn std::error::Error + Send + Sync>;
pub type GenericResult<T> = std::result::Result<T, GenericError>;

/// An error with msg and backtrace.
#[derive(Debug)]
pub struct MsgErrWithBacktrace {
    msg: String,
    backtrace: Backtrace,
}

impl MsgErrWithBacktrace {
    pub fn new(msg: String) -> Self {
        Self {
            msg,
            backtrace: Backtrace::generate(),
        }
    }

    #[inline]
    pub fn boxed(self) -> GenericError {
        Box::new(self)
    }
}

impl fmt::Display for MsgErrWithBacktrace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}\nBacktrace:\n{}", self.msg, self.backtrace)
    }
}

impl std::error::Error for MsgErrWithBacktrace {
    fn source(&self) -> Option<&(dyn snafu::Error + 'static)> {
        None
    }
}
