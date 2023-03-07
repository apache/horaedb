// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

pub mod planner;
mod stmt_rewriter;
pub(crate) mod util;
pub mod error {
    use common_util::error::GenericError;
    use snafu::Snafu;

    #[derive(Debug, Snafu)]
    #[snafu(visibility = "pub")]
    pub enum Error {
        #[snafu(display("Unimplemented influxql statement, msg: {}", msg))]
        Unimplemented { msg: String },

        #[snafu(display(
            "Failed to rewrite influxql from statement with cause, msg:{}, source:{}",
            msg,
            source
        ))]
        RewriteFromWithCause { msg: String, source: GenericError },

        #[snafu(display("Failed to rewrite influxql from statement no cause, msg:{}", msg))]
        RewriteFromNoCause { msg: String },

        #[snafu(display(
            "Failed to rewrite influxql projection statement with cause, msg:{}, source: {}",
            msg,
            source
        ))]
        RewriteFieldsWithCause { msg: String, source: GenericError },

        #[snafu(display(
            "Failed to rewrite influxql projection statement no cause, msg: {}",
            msg
        ))]
        RewriteFieldsNoCause { msg: String },

        #[snafu(display("Failed to find table with case, source:{}", source))]
        FindTableWithCause { source: GenericError },

        #[snafu(display("Failed to find table no case, msg: {}", msg))]
        FindTableNoCause { msg: String },
    }

    define_result!(Error);
}
