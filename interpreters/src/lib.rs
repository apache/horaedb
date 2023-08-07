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

//! Interpreters of query/insert/update/delete commands
//!
//! Inspired by fuse-query: <https://github.com/datafuselabs/fuse-query> and ClickHouse

#![feature(string_remove_matches)]

pub mod alter_table;
pub mod context;
pub mod create;
pub mod describe;
pub mod drop;
pub mod exists;
pub mod factory;
pub mod insert;
pub mod interpreter;
pub mod select;
pub mod show;
pub mod table_manipulator;
pub mod validator;

mod show_create;

#[cfg(test)]
mod tests;
