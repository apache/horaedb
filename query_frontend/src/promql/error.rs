// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use ceresdbproto::prometheus::sub_expr::OperatorType;
use datafusion::error::DataFusionError;
use snafu::{Backtrace, Snafu};

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(display("Invalid expr, expected: {}, actual:{:?}", expected, actual))]
    UnexpectedExpr { expected: String, actual: String },

    #[snafu(display("Expr pushdown not implemented, expr_type:{:?}", expr_type))]
    NotImplemented { expr_type: OperatorType },

    #[snafu(display("MetaProvider {}, err:{}", msg, source))]
    MetaProviderError {
        msg: String,
        source: crate::provider::Error,
    },

    #[snafu(display("Table not found, table:{}", name))]
    TableNotFound { name: String },

    #[snafu(display("Table provider not found, table:{}, err:{}", name, source))]
    TableProviderNotFound {
        name: String,
        source: DataFusionError,
    },

    #[snafu(display("Failed to build schema, err:{}", source))]
    BuildTableSchema { source: common_types::schema::Error },

    #[snafu(display("Failed to build plan, source:{}", source,))]
    BuildPlanError { source: DataFusionError },

    #[snafu(display("Invalid expr, msg:{}\nBacktrace:\n{}", msg, backtrace))]
    InvalidExpr { msg: String, backtrace: Backtrace },

    #[snafu(display("Failed to pushdown, source:{}", source))]
    PushdownError {
        source: crate::promql::pushdown::Error,
    },
}

define_result!(Error);

impl From<DataFusionError> for Error {
    fn from(df_err: DataFusionError) -> Self {
        Error::BuildPlanError { source: df_err }
    }
}
