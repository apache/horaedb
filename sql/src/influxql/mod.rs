// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Influxql processing

pub mod planner;

pub mod error {
    use common_util::error::GenericError;
    use snafu::{Backtrace, Snafu};

    #[derive(Debug, Snafu)]
    #[snafu(visibility = "pub")]
    pub enum Error {
        #[snafu(display(
            "Failed to build influxdb schema, msg:{}.\nBacktrace:{}",
            msg,
            backtrace
        ))]
        BuildSchema { msg: String, backtrace: Backtrace },

        #[snafu(display(
            "Failed to build influxql plan with cause, msg:{}, err:{}",
            msg,
            source
        ))]
        BuildPlanWithCause { msg: String, source: GenericError },

        #[snafu(display(
            "Failed to build influxql plan with no cause, msg:{}.\nBacktrace:{}",
            msg,
            backtrace
        ))]
        BuildPlanNoCause { msg: String, backtrace: Backtrace },

        #[snafu(display("Unimplemented influxql statement, msg:{}", msg))]
        Unimplemented { msg: String },
    }
    define_result!(Error);
}
