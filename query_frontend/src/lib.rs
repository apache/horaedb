// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.
#![feature(once_cell)]

//! SQL frontend
//!
//! Parse sql into logical plan that can be handled by interpreters

#[macro_use]
extern crate common_util;

pub mod ast;
pub mod container;
pub mod frontend;
pub mod influxql;
pub mod parser;
mod partition;
pub mod plan;
pub mod planner;
pub mod promql;
pub mod provider;
#[cfg(any(test, feature = "test"))]
pub mod tests;
