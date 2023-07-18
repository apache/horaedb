// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

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
