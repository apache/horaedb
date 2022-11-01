// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Error of handlers

use snafu::{Backtrace, Snafu};
// TODO(yingwen): Avoid printing huge sql string
// TODO(yingwen): Maybe add an error type to sql sub mod

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Failed to parse sql, err:{}", source))]
    ParseSql { source: sql::frontend::Error },

    #[snafu(display("Failed to create plan, query:{}, err:{}", query, source))]
    CreatePlan {
        query: String,
        source: sql::frontend::Error,
    },

    #[snafu(display(
        "Only support execute one statement now, current num:{}, query:{}.\nBacktrace:\n{}",
        len,
        query,
        backtrace,
    ))]
    TooMuchStmt {
        len: usize,
        query: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to execute interpreter, query:{}, err:{}", query, source))]
    InterpreterExec {
        query: String,
        source: interpreters::interpreter::Error,
    },

    #[snafu(display(
        "Failed to convert arrow to string, query:{}, err:{}.\nBacktrace:\n{}",
        query,
        source,
        backtrace
    ))]
    ArrowToString {
        query: String,
        source: arrow::error::ArrowError,
        backtrace: Backtrace,
    },

    #[snafu(display(
    "Query limited by block list, query:{}",
    query
    ))]
    QueryBlock {
        query: String,
    },
}

define_result!(Error);
