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
    collections::{BTreeSet, HashMap},
    time::Duration,
};

use tracing::debug;

use crate::{
    compaction::Task,
    sst::{FileId, SstFile},
    types::Timestamp,
};

pub struct TimeWindowCompactionStrategy {
    segment_duration: Duration,
}

impl TimeWindowCompactionStrategy {
    pub fn new(segment_duration: Duration) -> Self {
        Self { segment_duration }
    }

    pub fn pick_candidate(
        &self,
        ssts: Vec<SstFile>,
        expire_time: Option<Timestamp>,
    ) -> Option<Task> {
        let (uncompacted_files, expired_files) =
            Self::find_uncompacted_and_expired_files(ssts, expire_time);
        debug!(
            "uncompacted_files: {:?}, expired_files: {:?}",
            uncompacted_files, expired_files
        );

        let buckets = self.get_buckets(&uncompacted_files);
        let compact_files = self.get_compact_files(buckets);

        let expireds = expired_files
            .into_iter()
            .map(|f| f.id())
            .collect::<Vec<_>>();

        if compact_files.is_empty() && expireds.is_empty() {
            return None;
        }

        let task = Task {
            inputs: compact_files,
            expireds,
        };
        debug!("Pick compaction task: {:?}", task);

        Some(task)
    }

    ///  Group files of similar timestamp into buckets.
    fn get_buckets(&self, files: &[SstFile]) -> HashMap<i64, Vec<FileId>> {
        let mut buckets: HashMap<i64, Vec<FileId>> = HashMap::new();
        for f in files {
            let (left, _) = self.get_window_bounds(f.meta().time_range.end);

            let bucket_files = buckets.entry(left).or_default();

            bucket_files.push(f.id());
        }

        debug!(
            "Group files of similar timestamp into buckets: {:?}",
            buckets
        );
        buckets
    }

    fn get_window_bounds(&self, ts: Timestamp) -> (i64, i64) {
        let ts_secs = ts.0 / 1000;

        let size = self.segment_duration.as_secs() as i64;

        let lower = ts_secs - (ts_secs % size);
        let upper = lower + size - 1;

        (lower * 1000, upper * 1000)
    }

    fn get_compact_files(&self, buckets: HashMap<i64, Vec<FileId>>) -> Vec<FileId> {
        let all_keys: BTreeSet<_> = buckets.keys().collect();
        let mut compact_files = Vec::new();

        for key in all_keys.into_iter().rev() {
            if let Some(bucket) = buckets.get(key) {
                if bucket.len() >= 2 {
                    compact_files = bucket.clone();
                    break;
                } else {
                    debug!(
                        "No compaction necessary for bucket size {} , key {}",
                        bucket.len(),
                        key,
                    );
                }
            }
        }
        compact_files
    }

    fn find_uncompacted_and_expired_files(
        files: Vec<SstFile>,
        expire_time: Option<Timestamp>,
    ) -> (Vec<SstFile>, Vec<SstFile>) {
        let mut uncompacted_files = vec![];
        let mut expired_files = vec![];

        for f in files {
            if f.is_uncompacted() {
                if f.is_expired(expire_time) {
                    expired_files.push(f);
                } else {
                    uncompacted_files.push(f);
                }
            }
        }
        (uncompacted_files, expired_files)
    }
}
