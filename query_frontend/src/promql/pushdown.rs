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

use std::convert::TryFrom;

use common_types::time::{TimeRange, Timestamp};
use macros::define_result;
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Func {} is not supported yet", func))]
    NotSupportedFunc { func: String },
}

define_result!(Error);

#[derive(Debug, Clone, Copy, Hash, PartialEq)]
pub enum Func {
    Instant, // used to simulate instant query
    Rate,
    Irate,
    Delta,
    Idelta,
    Increase,
}

impl TryFrom<&str> for Func {
    type Error = Error;

    fn try_from(op: &str) -> Result<Self> {
        let t = match op {
            "rate" => Func::Rate,
            "delta" => Func::Delta,
            "irate" => Func::Irate,
            "idelta" => Func::Idelta,
            "increase" => Func::Increase,
            func => return NotSupportedFunc { func }.fail(),
        };

        Ok(t)
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq)]
pub struct AlignParameter {
    pub align_range: TimeRange,
    pub step: Timestamp,
    pub offset: Timestamp,
    /// 0 for no look back
    pub lookback_delta: Timestamp,
}
