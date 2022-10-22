// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Test util for message queue

use std::collections::BTreeMap;

use chrono::{DateTime, Duration, TimeZone, Utc};

use crate::Message;

pub fn generate_test_data(cnt: usize) -> Vec<Message> {
    let mut messages = Vec::with_capacity(cnt);
    let base_ts = Utc.timestamp_millis(1337);
    for i in 0..cnt {
        let key = format!("test_key_{}", i);
        let val = format!("test_val_{}", i);
        let timestamp = base_ts + Duration::milliseconds(i as i64);

        messages.push(message(key.as_bytes(), val.as_bytes(), timestamp));
    }

    messages
}

fn message(key: &[u8], value: &[u8], timestamp: DateTime<Utc>) -> Message {
    Message {
        key: Some(key.to_vec()),
        value: Some(value.to_vec()),
        headers: BTreeMap::new(),
        timestamp,
    }
}

pub fn random_topic_name() -> String {
    format!("test_topic_{}", uuid::Uuid::new_v4())
}
