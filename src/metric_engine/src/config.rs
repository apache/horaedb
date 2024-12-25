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

use std::collections::HashMap;

use common::{ReadableDuration, ReadableSize};
use parquet::basic::{Compression, Encoding, ZstdLevel};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct SchedulerConfig {
    pub schedule_interval: ReadableDuration,
    pub max_pending_compaction_tasks: usize,
    // Runner config
    pub memory_limit: ReadableSize,
    // Picker config
    pub ttl: Option<ReadableDuration>,
    pub new_sst_max_size: ReadableSize,
    pub input_sst_max_num: usize,
    pub input_sst_min_num: usize,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            schedule_interval: ReadableDuration::secs(10),
            max_pending_compaction_tasks: 10,
            memory_limit: ReadableSize::gb(20_u64),
            ttl: None,
            new_sst_max_size: ReadableSize::gb(1_u64),
            input_sst_max_num: 30,
            input_sst_min_num: 5,
        }
    }
}

#[derive(Debug, Default, Deserialize, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub enum ParquetEncoding {
    #[default]
    Plain,
    Rle,
    DeltaBinaryPacked,
    DeltaLengthByteArray,
    DeltaByteArray,
    RleDictionary,
}

impl From<ParquetEncoding> for Encoding {
    fn from(value: ParquetEncoding) -> Self {
        match value {
            ParquetEncoding::Plain => Encoding::PLAIN,
            ParquetEncoding::Rle => Encoding::RLE,
            ParquetEncoding::DeltaBinaryPacked => Encoding::DELTA_BINARY_PACKED,
            ParquetEncoding::DeltaLengthByteArray => Encoding::DELTA_LENGTH_BYTE_ARRAY,
            ParquetEncoding::DeltaByteArray => Encoding::DELTA_BYTE_ARRAY,
            ParquetEncoding::RleDictionary => Encoding::RLE_DICTIONARY,
        }
    }
}

#[derive(Debug, Default, Deserialize, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub enum ParquetCompression {
    #[default]
    Uncompressed,
    Snappy,
    Zstd,
}

impl From<ParquetCompression> for Compression {
    fn from(value: ParquetCompression) -> Self {
        match value {
            ParquetCompression::Uncompressed => Compression::UNCOMPRESSED,
            ParquetCompression::Snappy => Compression::SNAPPY,
            ParquetCompression::Zstd => Compression::ZSTD(ZstdLevel::default()),
        }
    }
}

#[derive(Debug, Default, Deserialize, Serialize, Clone, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct ColumnOptions {
    pub enable_dict: Option<bool>,
    pub enable_bloom_filter: Option<bool>,
    pub encoding: Option<ParquetEncoding>,
    pub compression: Option<ParquetCompression>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct WriteConfig {
    pub max_row_group_size: usize,
    pub write_bacth_size: usize,
    pub enable_sorting_columns: bool,
    // use to set column props with default value
    pub enable_dict: bool,
    pub enable_bloom_filter: bool,
    pub encoding: ParquetEncoding,
    pub compression: ParquetCompression,
    // use to set column props with column name
    pub column_options: Option<HashMap<String, ColumnOptions>>,
}

impl Default for WriteConfig {
    fn default() -> Self {
        Self {
            max_row_group_size: 8192,
            write_bacth_size: 1024,
            enable_sorting_columns: true,
            enable_dict: false,
            enable_bloom_filter: false,
            encoding: ParquetEncoding::Plain,
            compression: ParquetCompression::Snappy,
            column_options: None,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct ManifestConfig {
    pub channel_size: usize,
    pub merge_interval_seconds: usize,
    pub min_merge_threshold: usize,
    pub hard_merge_threshold: usize,
    pub soft_merge_threshold: usize,
}

impl Default for ManifestConfig {
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

#[derive(Debug, Default, Deserialize, Serialize, Clone, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    pub write: WriteConfig,
    pub manifest: ManifestConfig,
    pub scheduler: SchedulerConfig,
    pub update_mode: UpdateMode,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub enum UpdateMode {
    #[default]
    Overwrite,
    Append,
}
