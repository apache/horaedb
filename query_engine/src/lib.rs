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

//! Query engine
//!
//! Optimizes and executes logical plan

// TODO(yingwen): Maybe renamed to query_executor or query_backend?
// TODO(yingwen): Use datafusion or fuse-query as query backend

pub mod config;
pub mod context;
pub mod df_execution_extension;
pub mod df_planner_extension;
pub mod executor;
pub mod logical_optimizer;
pub mod physical_optimizer;
pub mod physical_plan;

pub use config::Config;
