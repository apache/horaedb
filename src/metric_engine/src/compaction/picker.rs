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

use std::{collections::BTreeMap, time::Duration};

use common::now;
use tracing::trace;

use crate::{compaction::Task, manifest::ManifestRef, sst::SstFile, types::Timestamp};

pub struct Picker {
    manifest: ManifestRef,
    ttl: Option<Duration>,
    strategy: TimeWindowCompactionStrategy,
}

impl Picker {
    pub fn new(
        manifest: ManifestRef,
        ttl: Option<Duration>,
        segment_duration: Duration,
        new_sst_max_size: u64,
        input_sst_max_num: usize,
    ) -> Self {
        Self {
            manifest,
            ttl,
            strategy: TimeWindowCompactionStrategy::new(
                segment_duration,
                new_sst_max_size,
                input_sst_max_num,
            ),
        }
    }

    pub async fn pick_candidate(&self) -> Option<Task> {
        let ssts = self.manifest.all_ssts().await;
        let expire_time = self.ttl.map(|ttl| (now() - ttl.as_micros() as i64).into());
        self.strategy.pick_candidate(ssts, expire_time)
    }
}

pub struct TimeWindowCompactionStrategy {
    segment_duration: Duration,
    new_sst_max_size: u64,
    input_sst_max_num: usize,
}

impl TimeWindowCompactionStrategy {
    pub fn new(
        segment_duration: Duration,
        new_sst_max_size: u64,
        input_sst_max_num: usize,
    ) -> Self {
        Self {
            segment_duration,
            new_sst_max_size,
            input_sst_max_num,
        }
    }

    pub fn pick_candidate(
        &self,
        ssts: Vec<SstFile>,
        expire_time: Option<Timestamp>,
    ) -> Option<Task> {
        let (uncompacted_files, expired_files) =
            Self::find_uncompacted_and_expired_files(ssts, expire_time);
        trace!(uncompacted_files = ?uncompacted_files, expired_files = ?expired_files, "Begin pick candidate");

        let files_by_segment = self.files_by_segment(uncompacted_files);
        let compaction_files = self.pick_compaction_files(files_by_segment)?;

        if compaction_files.is_empty() && expired_files.is_empty() {
            return None;
        }

        for f in &compaction_files {
            f.mark_compaction();
        }
        for f in &expired_files {
            f.mark_compaction();
        }

        let task = Task {
            inputs: compaction_files,
            expireds: expired_files,
        };

        trace!(task = ?task, "End pick candidate");

        Some(task)
    }

    fn find_uncompacted_and_expired_files(
        files: Vec<SstFile>,
        expire_time: Option<Timestamp>,
    ) -> (Vec<SstFile>, Vec<SstFile>) {
        let mut uncompacted_files = vec![];
        let mut expired_files = vec![];

        for f in files {
            if !f.is_compaction() {
                if f.is_expired(expire_time) {
                    expired_files.push(f);
                } else {
                    uncompacted_files.push(f);
                }
            }
        }
        (uncompacted_files, expired_files)
    }

    fn files_by_segment(&self, files: Vec<SstFile>) -> BTreeMap<Timestamp, Vec<SstFile>> {
        let mut files_by_segment = BTreeMap::new();
        let segment_duration = self.segment_duration;
        for file in files {
            let segment = file.meta().time_range.start.truncate_by(segment_duration);
            trace!(segment = ?segment, file = ?file);
            files_by_segment
                .entry(segment)
                .or_insert_with(Vec::new)
                .push(file);
        }

        trace!(
            files = ?files_by_segment,
            "Group files of similar timestamp into segment"
        );
        files_by_segment
    }

    fn pick_compaction_files(
        &self,
        files_by_segment: BTreeMap<Timestamp, Vec<SstFile>>,
    ) -> Option<Vec<SstFile>> {
        for (segment, mut files) in files_by_segment.into_iter().rev() {
            trace!(segment = ?segment, files = ?files, "Loop segment for pick files");
            if files.len() < 2 {
                continue;
            }

            // Prefer to compact smaller files first.
            files.sort_unstable_by_key(SstFile::size);

            let mut input_size = 0;
            // Suppose the comaction will reduce the size of files by 10%.
            let memory_limit = (self.new_sst_max_size as f64 * 1.1) as u64;

            let compaction_files = files
                .into_iter()
                .take(self.input_sst_max_num)
                .take_while(|f| {
                    input_size += f.size() as u64;
                    input_size <= memory_limit
                })
                .collect::<Vec<_>>();

            if compaction_files.len() >= 2 {
                return Some(compaction_files);
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use itertools::Itertools;
    use test_log::test;

    use super::*;
    use crate::sst::FileMeta;

    #[test]
    fn test_pick_candidate() {
        let segment_duration = Duration::from_millis(20);
        let strategy = TimeWindowCompactionStrategy::new(segment_duration, 9999, 10);

        let ssts = (0_i64..5_i64)
            .map(|i| {
                SstFile::new(
                    i as u64,
                    FileMeta {
                        max_sequence: i as u64,
                        num_rows: i as u32,
                        size: (100 - i) as u32, // size desc
                        time_range: (i * 10..(i * 10 + 10)).into(),
                    },
                )
            })
            .collect_vec();
        let task = strategy
            .pick_candidate(ssts.clone(), Some(15.into()))
            .unwrap();

        // ssts should be grouped into three segments:
        // | 0 1 | 2 3 | 4 |
        let excepted_task = Task {
            inputs: vec![ssts[3].clone(), ssts[2].clone()],
            expireds: vec![ssts[0].clone()],
        };

        assert_eq!(task, excepted_task);

        // sst1, sst3, ss4 are in compaction, so it should not be picked again.
        // sst2, sst5 are in different segment, so it also should not be picked.
        let task = strategy.pick_candidate(ssts, None);
        assert!(task.is_none());
    }
}
