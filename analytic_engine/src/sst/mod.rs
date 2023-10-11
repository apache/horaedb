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

//! SST (Sorted String Table) file

use std::sync::atomic::AtomicBool;

pub mod factory;
pub mod file;
pub mod header;
pub mod manager;
pub mod meta_data;
pub mod metrics;
pub mod parquet;
pub mod reader;
pub mod writer;

// TODO: make it a enum rather than plaining all file format options.
#[derive(Debug, Default)]
pub struct DynamicConfig {
    pub parquet_enable_page_filter: AtomicBool,
    pub parquet_enable_lazy_row_filter: AtomicBool,
}
