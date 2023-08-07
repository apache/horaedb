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

define_result!(Error);

#[derive(Debug, Snafu)]
#[allow(clippy::large_enum_variant)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Missing runtimes to build service.\nBacktrace:\n{}", backtrace))]
    MissingRuntimes { backtrace: Backtrace },

    #[snafu(display("Missing instance to build service.\nBacktrace:\n{}", backtrace))]
    MissingInstance { backtrace: Backtrace },

    #[snafu(display(
        "Failed to parse ip addr, ip:{}, err:{}.\nBacktrace:\n{}",
        ip,
        source,
        backtrace
    ))]
    ParseIpAddr {
        ip: String,
        source: std::net::AddrParseError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to create request context, err:{}", source))]
    CreateContext { source: proxy::context::Error },

    #[snafu(display("Failed to handle sql:{}, err:{}", sql, source))]
    HandleSql { sql: String, source: GenericError },

    #[snafu(display("Unexpected error, err:{}", source))]
    Unexpected { source: std::io::Error },
}
