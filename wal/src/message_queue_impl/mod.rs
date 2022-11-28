// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

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
