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

use std::{error, fmt};

/// Bit
/// An u8 struct used to represent a single bit,
/// when it is equal to zero, it represents bit zero, otherwise it represents
/// bit one.
#[derive(Debug, PartialEq)]
pub struct Bit(pub u8);

/// Error
/// Enum used to represent potential errors when interacting with a stream.
#[derive(Debug, PartialEq)]
pub enum Error {
    Eof,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Eof => write!(f, "Encountered the end of the stream"),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Eof => "Encountered the end of the stream",
        }
    }
}

pub mod buffered_write;

pub use self::buffered_write::BufferedWriter;

pub mod buffered_read;

pub use self::buffered_read::BufferedReader;
