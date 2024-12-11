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
    collections::{BTreeMap, BTreeSet, HashMap},
    time::Duration,
};

use tracing::debug;

use super::SchedulerConfig;
use crate::{compaction::Task, sst::SstFile, types::Timestamp};

pub struct TimeWindowCompactionStrategy {
    segment_duration: Duration,
    config: SchedulerConfig,
}

impl TimeWindowCompactionStrategy {
    pub fn new(segment_duration: Duration, config: SchedulerConfig) -> Self {
        Self {
            segment_duration,
            config,
        }
    }

    pub fn pick_candidate(
        &self,
        ssts: Vec<SstFile>,
        expire_time: Option<Timestamp>,
    ) -> Option<Task> {
        let (uncompacted_files, expired_files) =
            Self::find_uncompacted_and_expired_files(ssts, expire_time);
        debug!(uncompacted_files = ?uncompacted_files, expired_files = ?expired_files, "Begin pick candidate");

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

        debug!(task = ?task, "End pick candidate");

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
            debug!(segment = ?segment, file = ?file);
            files_by_segment
                .entry(segment)
                .or_insert_with(Vec::new)
                .push(file);
        }

        debug!(
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
            debug!(segment = ?segment, files = ?files, "Loop segment for pick files");
            if files.len() < 2 {
                continue;
            }

            // Prefer to compact smaller files first.
            files.sort_unstable_by_key(SstFile::size);

            let mut input_size = 0;
            let memory_limit = self.config.memory_limit;
            let compaction_files_limit = self.config.compaction_files_limit;

            let compaction_files = files
                .into_iter()
                .take(compaction_files_limit)
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
        let config = SchedulerConfig::default();
        let strategy = TimeWindowCompactionStrategy::new(segment_duration, config);

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

        // ssts should be grouped by tree segments:
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
