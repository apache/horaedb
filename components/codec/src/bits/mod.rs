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

const BIT_MASKS: [u8; 8] = [128, 64, 32, 16, 8, 4, 2, 1];

/// Bit
/// An u8 struct used to represent a single bit,
/// when it is equal to zero, it represents bit zero, otherwise it represents
/// bit one.
#[derive(Debug, PartialEq)]
pub struct Bit(pub u8);

pub mod buffered_write;

pub use self::buffered_write::BufferedWriter;

pub mod buffered_read;

pub use self::buffered_read::BufferedReader;
