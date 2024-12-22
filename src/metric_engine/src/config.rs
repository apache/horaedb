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

use std::{collections::HashMap, time::Duration};

use parquet::{
    basic::{Compression, Encoding, ZstdLevel},
    file::properties::WriterProperties,
};

use crate::types::UpdateMode;

#[derive(Clone)]
pub struct SchedulerConfig {
    pub schedule_interval: Duration,
    pub max_pending_compaction_tasks: usize,
    // Runner config
    pub memory_limit: u64,
    pub write_props: WriterProperties,
    // Picker config
    pub ttl: Option<Duration>,
    pub new_sst_max_size: u64,
    pub input_sst_max_num: usize,
    pub input_sst_min_num: usize,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            schedule_interval: Duration::from_secs(10),
            max_pending_compaction_tasks: 10,
            memory_limit: bytesize::gb(3_u64),
            write_props: WriterProperties::default(),
            ttl: None,
            new_sst_max_size: bytesize::gb(1_u64),
            input_sst_max_num: 30,
            input_sst_min_num: 5,
        }
    }
}

#[derive(Debug)]
pub struct ColumnOptions {
    pub enable_dict: Option<bool>,
    pub enable_bloom_filter: Option<bool>,
    pub encoding: Option<Encoding>,
    pub compression: Option<Compression>,
}

#[derive(Debug)]
pub struct WriteOptions {
    pub max_row_group_size: usize,
    pub write_bacth_size: usize,
    pub enable_sorting_columns: bool,
    // use to set column props with default value
    pub enable_dict: bool,
    pub enable_bloom_filter: bool,
    pub encoding: Encoding,
    pub compression: Compression,
    // use to set column props with column name
    pub column_options: Option<HashMap<String, ColumnOptions>>,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            max_row_group_size: 8192,
            write_bacth_size: 1024,
            enable_sorting_columns: true,
            enable_dict: false,
            enable_bloom_filter: false,
            encoding: Encoding::PLAIN,
            compression: Compression::ZSTD(ZstdLevel::default()),
            column_options: None,
        }
    }
}

#[derive(Debug)]
pub struct ManifestMergeOptions {
    pub channel_size: usize,
    pub merge_interval_seconds: usize,
    pub min_merge_threshold: usize,
    pub hard_merge_threshold: usize,
    pub soft_merge_threshold: usize,
}

impl Default for ManifestMergeOptions {
    fn default() -> Self {
        Self {
            channel_size: 3,
            merge_interval_seconds: 5,
            min_merge_threshold: 10,
            soft_merge_threshold: 50,
            hard_merge_threshold: 90,
        }
    }
}

#[derive(Debug, Default)]
pub struct StorageOptions {
    pub write_opts: WriteOptions,
    pub manifest_merge_opts: ManifestMergeOptions,
    pub update_mode: UpdateMode,
}
