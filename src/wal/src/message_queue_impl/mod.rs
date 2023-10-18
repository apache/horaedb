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

//! Wal manager based on message queue

use std::collections::BTreeMap;

use chrono::Utc;
use message_queue::Message;

pub mod config;
mod encoding;
mod log_cleaner;
mod namespace;
mod region;
mod region_context;
mod snapshot_synchronizer;
pub mod wal;

#[cfg(test)]
mod test_util;

#[inline]
fn to_message(log_key: Vec<u8>, log_value: Vec<u8>) -> Message {
    Message {
        key: Some(log_key),
        value: Some(log_value),
        headers: BTreeMap::default(),
        timestamp: Utc::now(),
    }
}
