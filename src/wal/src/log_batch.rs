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

//! Log entries definition.

use std::fmt::Debug;

use bytes_ext::{Buf, BufMut};
use common_types::{table::TableId, SequenceNumber};

use crate::manager::WalLocation;

pub trait Payload: Send + Sync + Debug {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Compute size of the encoded payload.
    fn encode_size(&self) -> usize;
    /// Append the encoded payload to the `buf`.
    fn encode_to<B: BufMut>(&self, buf: &mut B) -> Result<(), Self::Error>;
}

#[derive(Debug)]
pub struct LogEntry<P> {
    pub table_id: TableId,
    pub sequence: SequenceNumber,
    pub payload: P,
}

/// An encoded entry to be written into the Wal.
#[derive(Debug)]
pub struct LogWriteEntry {
    pub payload: Vec<u8>,
}

/// A batch of `LogWriteEntry`s.
#[derive(Debug)]
pub struct LogWriteBatch {
    pub location: WalLocation,
    pub entries: Vec<LogWriteEntry>,
}

impl LogWriteBatch {
    pub fn new(location: WalLocation) -> Self {
        Self::with_capacity(location, 0)
    }

    pub fn with_capacity(location: WalLocation, cap: usize) -> Self {
        Self {
            location,
            entries: Vec::with_capacity(cap),
        }
    }

    #[inline]
    pub fn push(&mut self, entry: LogWriteEntry) {
        self.entries.push(entry)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[inline]
    pub fn clear(&mut self) {
        self.entries.clear()
    }
}

pub trait PayloadDecoder: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;
    type Target: Send + Sync;
    /// Decode `Target` from the `bytes`.
    fn decode<B: Buf>(&self, buf: &mut B) -> Result<Self::Target, Self::Error>;
}
