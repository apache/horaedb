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
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, LazyLock,
    },
    time::SystemTime,
};

use crate::{
    ensure,
    types::{TimeRange, Timestamp},
    Error,
};

const PREFIX_PATH: &str = "data";

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

pub type FileId = u64;

#[derive(Clone, Debug)]
pub struct SstFile {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    id: FileId,
    meta: FileMeta,

    in_compaction: AtomicBool,
}

impl Inner {
    pub fn new(id: FileId, meta: FileMeta) -> Self {
        Self {
            id,
            meta,
            in_compaction: AtomicBool::new(false),
        }
    }
}

impl SstFile {
    pub fn new(id: FileId, meta: FileMeta) -> Self {
        let inner = Arc::new(Inner::new(id, meta));
        Self { inner }
    }

    pub fn id(&self) -> FileId {
        self.inner.id
    }

    pub fn meta(&self) -> &FileMeta {
        &self.inner.meta
    }

    pub fn mark_compaction(&self) {
        self.inner.in_compaction.store(true, Ordering::Relaxed);
    }

    pub fn unmark_compaction(&self) {
        self.inner.in_compaction.store(false, Ordering::Relaxed);
    }

    pub fn is_compaction(&self) -> bool {
        self.inner.in_compaction.load(Ordering::Relaxed)
    }

    pub fn is_expired(&self, expire_time: Option<Timestamp>) -> bool {
        match expire_time {
            Some(expire_time) => self.meta().time_range.end < expire_time,
            None => false,
        }
    }

    pub fn size(&self) -> u32 {
        self.meta().size
    }

    pub fn allocate_id() -> FileId {
        NEXT_ID.fetch_add(1, Ordering::SeqCst)
    }
}

impl TryFrom<pb_types::SstFile> for SstFile {
    type Error = Error;

    fn try_from(value: pb_types::SstFile) -> Result<Self, Self::Error> {
        ensure!(value.meta.is_some(), "file meta is missing");
        let meta = value.meta.unwrap();
        let meta = meta.try_into()?;

        Ok(Self::new(value.id, meta))
    }
}

impl From<SstFile> for pb_types::SstFile {
    fn from(value: SstFile) -> Self {
        pb_types::SstFile {
            id: value.id(),
            meta: Some(value.meta().clone().into()),
        }
    }
}

impl PartialEq for SstFile {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id() && self.meta() == self.meta()
    }
}

impl Eq for SstFile {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileMeta {
    pub max_sequence: u64,
    pub num_rows: u32,
    pub size: u32,
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
            size: value.size,
            time_range: TimeRange::new(time_range.start.into(), time_range.end.into()),
        })
    }
}

impl From<FileMeta> for pb_types::SstMeta {
    fn from(value: FileMeta) -> Self {
        pb_types::SstMeta {
            max_sequence: value.max_sequence,
            num_rows: value.num_rows,
            size: value.size,
            time_range: Some(pb_types::TimeRange {
                start: *value.time_range.start,
                end: *value.time_range.end,
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SstPathGenerator {
    prefix: String,
}

impl SstPathGenerator {
    pub fn new(prefix: String) -> Self {
        Self { prefix }
    }

    pub fn generate(&self, id: FileId) -> String {
        format!("{}/{}/{}.sst", self.prefix, PREFIX_PATH, id)
    }
}
