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

//! Table engine facade, provides read/write interfaces of table

pub mod engine;
pub mod memory;
pub mod metrics;
pub mod partition;
pub mod predicate;
pub mod provider;
pub mod proxy;
pub mod remote;
pub mod stream;
pub mod table;

pub const MEMORY_ENGINE_TYPE: &str = "Memory";
pub const ANALYTIC_ENGINE_TYPE: &str = "Analytic";
pub const PARTITION_TABLE_ENGINE_TYPE: &str = "PartitionTable";
