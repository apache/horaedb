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

//! RocksDB Config

use serde::{Deserialize, Serialize};
use size_ext::ReadableSize;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub max_subcompactions: u32,
    pub max_background_jobs: i32,
    pub enable_statistics: bool,
    pub write_buffer_size: ReadableSize,
    pub max_write_buffer_number: i32,
    // Number of files to trigger level-0 compaction. A value <0 means that level-0 compaction will
    // not be triggered by number of files at all.
    pub level_zero_file_num_compaction_trigger: i32,
    // Soft limit on number of level-0 files. We start slowing down writes at this point. A value
    // <0 means that no writing slow down will be triggered by number of files in level-0.
    pub level_zero_slowdown_writes_trigger: i32,
    // Maximum number of level-0 files.  We stop writes at this point.
    pub level_zero_stop_writes_trigger: i32,
    pub fifo_compaction_max_table_files_size: ReadableSize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            // Same with rocksdb
            // https://github.com/facebook/rocksdb/blob/v6.4.6/include/rocksdb/options.h#L537
            max_subcompactions: 1,
            max_background_jobs: 2,
            enable_statistics: false,
            write_buffer_size: ReadableSize::mb(64),
            max_write_buffer_number: 2,
            level_zero_file_num_compaction_trigger: 4,
            level_zero_slowdown_writes_trigger: 20,
            level_zero_stop_writes_trigger: 36,
            // default is 1G, use 0 to disable fifo
            fifo_compaction_max_table_files_size: ReadableSize::gb(0),
        }
    }
}
