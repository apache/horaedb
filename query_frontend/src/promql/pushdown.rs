// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

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
