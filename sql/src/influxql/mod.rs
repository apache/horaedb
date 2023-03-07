// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Influxql processing

pub mod planner;
pub(crate) mod stmt_converter;
pub(crate) mod stmt_rewriter;
#[cfg(any(test, feature = "test"))]
pub mod test_util;
pub(crate) mod util;

use std::collections::HashSet;

use lazy_static::lazy_static;
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

        #[snafu(display("Failed to convert to sql statement, msg: {}", msg))]
        Convert { msg: String },
    }

    define_result!(Error);
}

// Copy from influxql_iox.
lazy_static! {
    pub(crate) static ref SCALAR_MATH_FUNCTIONS: HashSet<&'static str> = HashSet::from([
        "abs", "sin", "cos", "tan", "asin", "acos", "atan", "atan2", "exp", "log", "ln", "log2",
        "log10", "sqrt", "pow", "floor", "ceil", "round",
    ]);
}

/// Returns `true` if `name` is a mathematical scalar function
/// supported by InfluxQL.
pub(crate) fn is_scalar_math_function(name: &str) -> bool {
    SCALAR_MATH_FUNCTIONS.contains(name)
}
