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

use generic_error::GenericError;
use macros::define_result;
use snafu::{Backtrace, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to init query engine, msg:{msg}.\nBacktrace:\n{backtrace}"))]
    InitNoCause { msg: String, backtrace: Backtrace },

    #[snafu(display("Failed to init query engine, msg:{msg}, err:{source}"))]
    InitWithCause { msg: String, source: GenericError },

    #[snafu(display("Physical plan err with cause,, msg:{msg:?}, err:{source}"))]
    PhysicalPlanWithCause {
        msg: Option<String>,
        source: GenericError,
    },

    #[snafu(display("Physical plan err with no cause, msg:{msg:?}.\nBacktrace:\n{backtrace}"))]
    PhysicalPlanNoCause {
        msg: Option<String>,
        backtrace: Backtrace,
    },

    #[snafu(display("Physical planner err with cause,, msg:{msg:?}, err:{source}",))]
    PhysicalPlannerWithCause {
        msg: Option<String>,
        source: GenericError,
    },

    #[snafu(display("Physical planner err with no cause, msg:{msg:?}.\nBacktrace:\n{backtrace}",))]
    PhysicalPlannerNoCause {
        msg: Option<String>,
        backtrace: Backtrace,
    },

    #[snafu(display("Logical optimizer err with cause,, msg:{msg:?}, err:{source}",))]
    LogicalOptimizerWithCause {
        msg: Option<String>,
        source: GenericError,
    },

    #[snafu(display(
        "Logical optimizer err with no cause, msg:{msg:?}.\nBacktrace:\n{backtrace}",
    ))]
    LogicalOptimizerNoCause {
        msg: Option<String>,
        backtrace: Backtrace,
    },

    #[snafu(display("Physical optimizer err with cause, msg:{msg:?}, err:{source}",))]
    PhysicalOptimizerWithCause {
        msg: Option<String>,
        source: GenericError,
    },

    #[snafu(display(
        "Physical optimizer err with no cause, msg:{msg:?}.\nBacktrace:\n{backtrace}",
    ))]
    PhysicalOptimizerNoCause {
        msg: Option<String>,
        backtrace: Backtrace,
    },

    #[snafu(display("Executor err with cause, msg:{msg:?}, err:{source}",))]
    ExecutorWithCause {
        msg: Option<String>,
        source: GenericError,
    },

    #[snafu(display("Executor err with no cause, msg:{msg:?}.\nBacktrace:\n{backtrace}",))]
    ExecutorNoCause {
        msg: Option<String>,
        backtrace: Backtrace,
    },

    #[snafu(display("PhysicalPlanEncoder err with cause, msg:{msg}, err:{source}",))]
    PhysicalPlanEncoderWithCause { msg: String, source: GenericError },

    #[snafu(display(
        "PhysicalPlanEncoder err with no cause, msg:{msg}.\nBacktrace:\n{backtrace}",
    ))]
    PhysicalPlanEncoderNoCause { msg: String, backtrace: Backtrace },
}

define_result!(Error);
