// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use common_util::{define_result, error::GenericError};
use snafu::{Snafu, Backtrace};
use wal::manager::WalLocation;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display(
        "Failed to encode payloads, wal_location:{:?}, err:{}",
        wal_location,
        source
    ))]
    EncodePayloads {
        wal_location: WalLocation,
        source: wal::manager::Error,
    },

    #[snafu(display("Failed to write update to wal, err:{}", source))]
    WriteWal { source: wal::manager::Error },

    #[snafu(display("Failed to read wal, err:{}", source))]
    ReadWal { source: wal::manager::Error },

    #[snafu(display("Failed to read log entry, err:{}", source))]
    ReadEntry { source: wal::manager::Error },

    #[snafu(display("Failed to apply table meta update, err:{}", source))]
    ApplyUpdate {
        source: crate::manifest::meta_snapshot::Error,
    },

    #[snafu(display("Failed to clean wal, err:{}", source))]
    CleanWal { source: wal::manager::Error },

    #[snafu(display(
        "Failed to store snapshot, err:{}.\nBacktrace:\n{:?}",
        source,
        backtrace
    ))]
    StoreSnapshot {
        source: object_store::ObjectStoreError,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to fetch snapshot, err:{}.\nBacktrace:\n{:?}",
        source,
        backtrace
    ))]
    FetchSnapshot {
        source: object_store::ObjectStoreError,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to decode snapshot, err:{}.\nBacktrace:\n{:?}",
        source,
        backtrace
    ))]
    DecodeSnapshot {
        source: prost::DecodeError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to build snapshot, msg:{}.\nBacktrace:\n{:?}", msg, backtrace))]
    BuildSnapshotNoCause { msg: String, backtrace: Backtrace },

    #[snafu(display("Failed to build snapshot, msg:{}, err:{}", msg, source))]
    BuildSnapshotWithCause { msg: String, source: GenericError },

    #[snafu(display(
        "Failed to apply edit to table, msg:{}.\nBacktrace:\n{:?}",
        msg,
        backtrace
    ))]
    ApplyUpdateToTableNoCause { msg: String, backtrace: Backtrace },

    #[snafu(display("Failed to apply edit to table, msg:{}, err:{}", msg, source))]
    ApplyUpdateToTableWithCause { msg: String, source: GenericError },

    #[snafu(display(
        "Failed to apply snapshot to table, msg:{}.\nBacktrace:\n{:?}",
        msg,
        backtrace
    ))]
    ApplySnapshotToTableNoCause { msg: String, backtrace: Backtrace },

    #[snafu(display("Failed to apply snapshot to table, msg:{}, err:{}", msg, source))]
    ApplySnapshotToTableWithCause { msg: String, source: GenericError },

    #[snafu(display("Failed to load snapshot, err:{}", source))]
    LoadSnapshot { source: GenericError },
}

define_result!(Error);
