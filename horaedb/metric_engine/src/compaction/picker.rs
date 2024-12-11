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
        debug!(
            "uncompacted_files: {:?}, expired_files: {:?}",
            uncompacted_files, expired_files
        );

        let files_by_segment = self.files_by_segment(uncompacted_files);
        let compaction_files = self.pick_compaction_files(files_by_segment);

        if compaction_files.is_empty() && expired_files.is_empty() {
            return None;
        }

        for f in &compaction_files {
            f.mark_compaction();
        }

        let task = Task {
            inputs: compaction_files,
            expireds: expired_files,
        };
        debug!("Pick compaction task: {:?}", task);

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

    fn files_by_segment(&self, files: Vec<SstFile>) -> HashMap<Timestamp, Vec<SstFile>> {
        let mut files_by_segment = HashMap::new();
        let segment_duration = self.segment_duration;
        for file in files {
            let segment = file.meta().time_range.end.truncate_by(segment_duration);
            files_by_segment
                .entry(segment)
                .or_insert_with(Vec::new)
                .push(file);
        }

        debug!(
            "Group files of similar timestamp into same segment: {:?}",
            files_by_segment
        );
        files_by_segment
    }

    fn pick_compaction_files(
        &self,
        mut files_by_segment: HashMap<Timestamp, Vec<SstFile>>,
    ) -> Vec<SstFile> {
        let all_segments: BTreeSet<_> = files_by_segment.keys().copied().collect();

        for segment in all_segments {
            if let Some(mut files) = files_by_segment.remove(&segment) {
                if files.len() < 2 {
                    debug!(
                        "No compaction necessary for files size {} , segment {:?}",
                        files.len(),
                        segment,
                    );
                    continue;
                }

                // Pick files for compaction
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
                    return compaction_files;
                }
            }
        }
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::sst::FileMeta;

    #[test]
    fn test_pick_candidate() {
        let segment_duration = Duration::from_millis(20);
        let config = SchedulerConfig::default();
        let strategy = TimeWindowCompactionStrategy::new(segment_duration, config);

        let sst1 = SstFile::new(
            1,
            FileMeta {
                time_range: (0..10).into(),
                size: 1,
                max_sequence: 1,
                num_rows: 1,
            },
        );
        let sst2 = SstFile::new(
            2,
            FileMeta {
                time_range: (10..20).into(),
                size: 2,
                max_sequence: 2,
                num_rows: 2,
            },
        );
        let sst3 = SstFile::new(
            3,
            FileMeta {
                time_range: (20..30).into(),
                size: 3,
                max_sequence: 3,
                num_rows: 3,
            },
        );
        let sst4 = SstFile::new(
            4,
            FileMeta {
                time_range: (30..40).into(),
                size: 4,
                max_sequence: 4,
                num_rows: 4,
            },
        );
        let sst5 = SstFile::new(
            5,
            FileMeta {
                time_range: (40..50).into(),
                size: 5,
                max_sequence: 5,
                num_rows: 5,
            },
        );

        let ssts = vec![
            sst1.clone(),
            sst2.clone(),
            sst3.clone(),
            sst4.clone(),
            sst5.clone(),
        ];

        let task = strategy
            .pick_candidate(ssts.clone(), Some(15.into()))
            .unwrap();

        let excepted_task = Task {
            inputs: vec![sst2, sst3],
            expireds: vec![sst1],
        };

        assert_eq!(task.inputs.len(), 2);
        assert_eq!(task.expireds.len(), 1);
        assert_eq!(task, excepted_task);

        let task = strategy.pick_candidate(ssts, None).unwrap();
        let excepted_task = Task {
            inputs: vec![sst4, sst5],
            expireds: vec![],
        };
        assert_eq!(task.inputs.len(), 2);
        assert_eq!(task.expireds.len(), 0);
        assert_eq!(task, excepted_task);
    }
}
