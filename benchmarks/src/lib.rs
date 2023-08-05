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

//! Utilities for benchmarks.

use common_types::SequenceNumber;

pub mod config;
pub mod merge_memtable_bench;
pub mod merge_sst_bench;
pub mod parquet_bench;
pub mod scan_memtable_bench;
pub mod sst_bench;
pub mod sst_tools;
pub mod util;
pub mod wal_write_bench;

pub(crate) const INIT_SEQUENCE: SequenceNumber = 1;
