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

//! Test util for message queue

use std::collections::BTreeMap;

use chrono::{DateTime, Duration, TimeZone, Utc};

use crate::Message;

pub fn generate_test_data(cnt: usize) -> Vec<Message> {
    let mut messages = Vec::with_capacity(cnt);
    let base_ts = Utc.timestamp_millis_opt(1337).unwrap();
    for i in 0..cnt {
        let key = format!("test_key_{i}");
        let val = format!("test_val_{i}");
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
