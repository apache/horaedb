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

use macros::ensure;

use crate::{types::TimeRange, Error};

pub const PREFIX_PATH: &str = "data";

pub type FileId = u64;

#[derive(Clone, Debug)]
pub struct SstFile {
    pub id: FileId,
    pub meta: FileMeta,
}

impl TryFrom<pb_types::SstFile> for SstFile {
    type Error = Error;

    fn try_from(value: pb_types::SstFile) -> Result<Self, Self::Error> {
        ensure!(value.meta.is_some(), "file meta is missing");
        let meta = value.meta.unwrap();
        let meta = meta.try_into()?;

        Ok(Self { id: value.id, meta })
    }
}

impl From<SstFile> for pb_types::SstFile {
    fn from(value: SstFile) -> Self {
        pb_types::SstFile {
            id: value.id,
            meta: Some(value.meta.into()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct FileMeta {
    pub max_sequence: u64,
    pub num_rows: u32,
    pub time_range: TimeRange,
}

impl TryFrom<pb_types::SstMeta> for FileMeta {
    type Error = Error;

    fn try_from(value: pb_types::SstMeta) -> Result<Self, Self::Error> {
        ensure!(value.time_range.is_some(), "time range is missing");
        let time_range = value.time_range.unwrap();

        Ok(Self {
            max_sequence: value.max_sequence,
            num_rows: value.num_rows,
            time_range: TimeRange {
                start: time_range.start,
                end: time_range.end,
            },
        })
    }
}

impl From<FileMeta> for pb_types::SstMeta {
    fn from(value: FileMeta) -> Self {
        pb_types::SstMeta {
            max_sequence: value.max_sequence,
            num_rows: value.num_rows,
            time_range: Some(pb_types::TimeRange {
                start: value.time_range.start,
                end: value.time_range.end,
            }),
        }
    }
}

// Used for sst file id allocation.
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
