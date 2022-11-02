// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Wal manager based on message queue

use std::collections::BTreeMap;

use chrono::Utc;
use message_queue::Message;

pub(crate) mod encoding;
pub(crate) mod helpers;
pub(crate) mod region;
pub(crate) mod region_meta;

#[cfg(test)]
pub(crate) mod test_util;

#[inline]
fn to_message(log_key: Vec<u8>, log_value: Vec<u8>) -> Message {
    Message {
        key: Some(log_key),
        value: Some(log_value),
        headers: BTreeMap::default(),
        timestamp: Utc::now(),
    }
}
