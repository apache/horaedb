// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Influxql processing

pub mod planner;

pub mod error {
    use generic_error::GenericError;
    use macros::define_result;
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
