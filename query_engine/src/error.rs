// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use generic_error::GenericError;
use macros::define_result;
use snafu::{Backtrace, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Physical plan err with cause,, msg:{msg:?}, err:{source}",))]
    PhysicalPlanWithCause {
        msg: Option<String>,
        source: GenericError,
    },

    #[snafu(display("Physical plan err with no cause, msg:{msg:?}.\nBacktrace:\n{backtrace}",))]
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
}

define_result!(Error);
