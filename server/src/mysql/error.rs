// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use snafu::{Backtrace, Snafu};

#[derive(Debug, Snafu)]
#[allow(clippy::large_enum_variant)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Missing runtimes to build service.\nBacktrace:\n{}", backtrace))]
    MissingRuntimes { backtrace: Backtrace },

    #[snafu(display("Missing instance to build service.\nBacktrace:\n{}", backtrace))]
    MissingInstance { backtrace: Backtrace },

    #[snafu(display(
        "Failed to parse ip addr, ip:{}, err:{}.\nBacktrace:\n{}",
        ip,
        source,
        backtrace
    ))]
    ParseIpAddr {
        ip: String,
        source: std::net::AddrParseError,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Mysql Server not running, err: {}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    ServerNotRunning {
        backtrace: Backtrace,
        source: std::io::Error,
    },

    #[snafu(display("Failed to create request context, err:{}", source))]
    CreateContext { source: crate::context::Error },

    #[snafu(display("Failed to handle sql:{}, err:{}", sql, source))]
    HandleSql {
        sql: String,
        source: crate::handlers::error::Error,
    },

    #[snafu(display("Unexpected error, err:{}", source))]
    Unexpected { source: std::io::Error },
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Unexpected { source: e }
    }
}

define_result!(Error);
