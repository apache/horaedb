// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use async_trait::async_trait;
use generic_error::GenericError;
use macros::define_result;
use snafu::{Backtrace, Snafu};

pub mod compaction_impl;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(display(
        "Failed to connect the service endpoint:{}, err:{}\nBacktrace:\n{}",
        addr,
        source,
        backtrace
    ))]
    FailConnect {
        addr: String,
        source: GenericError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to execute compaction task, err:{}", source))]
    FailExecuteCompactionTask { source: GenericError },

    #[snafu(display("Missing header in rpc response.\nBacktrace:\n{}", backtrace))]
    MissingHeader { backtrace: Backtrace },

    #[snafu(display(
        "Bad response, resp code:{}, msg:{}.\nBacktrace:\n{}",
        code,
        msg,
        backtrace
    ))]
    BadResponse {
        code: u32,
        msg: String,
        backtrace: Backtrace,
    },
}

define_result!(Error);

/// CompactionClient is the abstraction of client used for HoraeDB to
/// communicate with CompactionServer cluster.
#[async_trait]
pub trait CompactionClient: Send + Sync {
    async fn execute_compaction_task(
        &self,
        req: horaedbproto::compaction_service::ExecuteCompactionTaskRequest,
    ) -> Result<horaedbproto::compaction_service::ExecuteCompactionTaskResponse>;
}

pub type CompactionClientRef = Arc<dyn CompactionClient>;
