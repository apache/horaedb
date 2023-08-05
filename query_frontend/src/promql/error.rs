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

use ceresdbproto::prometheus::sub_expr::OperatorType;
use datafusion::error::DataFusionError;
use macros::define_result;
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
