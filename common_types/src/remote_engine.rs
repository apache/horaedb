// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Common type for remote engine rpc service

use snafu::{Backtrace, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unknown version, value:{}.\nBacktrace:\n{}", version, backtrace))]
    UnknownVersion { version: u32, backtrace: Backtrace },
}

type Result<T> = std::result::Result<T, Error>;

pub enum Version {
    ArrowIPCWithZstd,
}

impl TryFrom<u32> for Version {
    type Error = Error;

    fn try_from(version: u32) -> Result<Self> {
        match version {
            0 => Ok(Self::ArrowIPCWithZstd),
            _ => UnknownVersion { version }.fail(),
        }
    }
}

impl Version {
    pub fn as_u32(&self) -> u32 {
        match self {
            Self::ArrowIPCWithZstd => 0,
        }
    }
}
