// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Error of read buffer

use common_types::table::{ShardId, TableId};
use common_util::define_result;
use snafu::{Backtrace, Snafu};

use super::RegistrationSheet;
use crate::manager::Error as WalManagerError;

// Now most error from manage implementation don't have backtrace, so we add
// backtrace here.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display(
        "Register shard to read buffer failed, shard has existed, registration sheet:{:?}, backtrace:{}",
        sheet,
        backtrace
    ))]
    RegisterShard {
        sheet: RegistrationSheet,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Unregister shard from read buffer failed, shard not found, shard:{}, backtrace:{}",
        shard_id,
        backtrace
    ))]
    UnregisterShard {
        shard_id: ShardId,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Split failed to read logs from wal manager on shard:{}, err:{}",
        shard_id,
        source
    ))]
    SplitRead {
        shard_id: ShardId,
        source: WalManagerError,
    },

    #[snafu(display(
        "Split failed to send logs to table:{} on shard:{}, err:{}",
        table_id,
        shard_id,
        source
    ))]
    SplitSend {
        shard_id: ShardId,
        table_id: TableId,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unknown error occurred, msg:{}, backtrace:{}", msg, backtrace))]
    Unknown { msg: String, backtrace: Backtrace },
}

define_result!(Error);
