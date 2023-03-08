// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Influxql processing

pub mod planner;
pub(crate) mod select;
pub mod test_util;
pub(crate) mod util;
pub mod error {
    use common_util::error::GenericError;
    use snafu::{Backtrace, Snafu};

    #[derive(Debug, Snafu)]
    #[snafu(visibility = "pub")]
    pub enum Error {
        #[snafu(display(
            "Unimplemented influxql statement, msg: {}.\nBacktrace:{}",
            msg,
            backtrace
        ))]
        Unimplemented { msg: String, backtrace: Backtrace },

        #[snafu(display(
            "Failed to rewrite influxql from statement with cause, msg:{}, source:{}",
            msg,
            source
        ))]
        RewriteWithCause { msg: String, source: GenericError },

        #[snafu(display(
            "Failed to rewrite influxql from statement no cause, msg:{}.\nBacktrace:{}",
            msg,
            backtrace
        ))]
        RewriteNoCause { msg: String, backtrace: Backtrace },

        #[snafu(display(
            "Failed to convert to sql statement, msg:{}..\nBacktrace:{}",
            msg,
            backtrace
        ))]
        Convert { msg: String, backtrace: Backtrace },
    }
    define_result!(Error);
}
