// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Influxql processing

pub mod planner;
pub(crate) mod provider;

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

        #[snafu(display("Failed to build influxql plan, msg:{}, err:{}", msg, source))]
        BuildPlan { msg: String, source: GenericError },
    }
    define_result!(Error);
}
