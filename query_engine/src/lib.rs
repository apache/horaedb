// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Query engine
//!
//! Optimizes and executes logical plan

// TODO(yingwen): Maybe renamed to query_executor or query_backend?
// TODO(yingwen): Use datafusion or fuse-query as query backend

#[macro_use]
extern crate common_util;

pub mod config;
pub mod context;
pub mod df_execution_extension;
pub mod df_planner_extension;
pub mod executor;
pub mod logical_optimizer;
pub mod physical_optimizer;
pub mod physical_plan;
