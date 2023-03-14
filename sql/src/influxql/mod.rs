// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Influxql processing

pub mod planner;
pub(crate) mod provider;

pub mod error {
    use snafu::{Backtrace, Snafu};

    #[derive(Debug, Snafu)]
    #[snafu(visibility = "pub")]
    pub enum Error {
        #[snafu(display(
            "Failed to build influxdb schema, msg: {}.\nBacktrace:{}",
            msg,
            backtrace
        ))]
        BuildInfluxSchema { msg: String, backtrace: Backtrace },
    }
    define_result!(Error);
}
