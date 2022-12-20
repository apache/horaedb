// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Partition rules

use common_util::define_result;
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {}

define_result!(Error);

/// Partition rule locate partition
pub trait PartitionRule {}
