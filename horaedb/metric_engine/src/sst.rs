// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        LazyLock,
    },
    time::SystemTime,
};

use crate::types::TimeRange;

pub const PREFIX_PATH: &str = "data";

pub type FileId = u64;

pub struct SSTable {
    pub id: FileId,
}

pub struct FileMeta {
    pub num_row: u32,
    pub range: TimeRange,
}

// Used as base for id allocation
// This number mustn't go backwards on restarts, otherwise file id
// collisions are possible. So don't change time on the server
// between server restarts.
static NEXT_ID: LazyLock<AtomicU64> = LazyLock::new(|| {
    AtomicU64::new(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64,
    )
});

pub fn allocate_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::SeqCst)
}
