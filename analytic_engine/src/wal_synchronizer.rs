// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! WAL Synchronizer implementation.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use common_types::SequenceNumber;
use common_util::{
    define_result,
    runtime::{JoinHandle, Runtime},
};
use log::{debug, error, info};
use snafu::{ResultExt, Snafu};
use table_engine::table::WriteRequest;
use tokio::{
    sync::{
        mpsc::{self, Receiver, Sender},
        Mutex, RwLock,
    },
    time,
};
use wal::{
    log_batch::LogEntry,
    manager::{
        BatchLogIterator, BatchLogIteratorAdapter, ReadBoundary, ReadContext, ReadRequest,
        WalLocation, WalManagerRef,
    },
};

use self::role_table::ReaderTable;
use crate::payload::{ReadPayload, WalDecoder};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to write to wal, err:{}", source))]
    WriteLogBatch {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display("Failed to read wal, err:{}", source))]
    ReadWal { source: wal::manager::Error },

    #[snafu(display("Encounter invalid table state, err:{}", source))]
    InvalidTableState {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display("Failed to stop synchronizer, err:{}", source))]
    StopSynchronizer {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

define_result!(Error);

pub struct WalSynchronizerConfig {
    /// Interval between two syncs
    interval: Duration,
    /// Used as WAL's read batch size
    batch_size: usize,
}

impl Default for WalSynchronizerConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
            batch_size: 128,
        }
    }
}

/// A background synchronizer that keep polling WAL update.
///
/// This [WalSynchronizer] has a queue of [RegionId]s that need synchronization.
/// Others can register new region with [register_table] method. And invalid
/// table will be removed automatically. The workflow looks like:
///
/// ```plaintext
///            register IDs
///           need synchronization
///      ┌─────────────────────┐
///      │                     │
///      │              ┌──────▼─────────┐
/// ┌────┴─────┐        │  background    │
/// │Role Table│        │WAL Synchronizer│
/// └────▲─────┘        └──────┬─────────┘
///      │                     │
///      └─────────────────────┘
///           synchronize log
///             to table
/// ```
pub struct WalSynchronizer {
    inner: Arc<Inner>,
    stop_sender: Sender<()>,
    join_handle: Mutex<Option<JoinHandle<()>>>,
    stop_receiver: Option<Receiver<()>>,
}

impl WalSynchronizer {
    pub fn new(config: WalSynchronizerConfig, wal: WalManagerRef) -> Self {
        let (tx, rx) = mpsc::channel(1);
        let inner = Inner {
            wal,
            config,
            tables: RwLock::default(),
        };
        Self {
            inner: Arc::new(inner),
            stop_sender: tx,
            stop_receiver: Some(rx),
            join_handle: Mutex::new(None),
        }
    }

    pub async fn stop(&self) -> Result<()> {
        let _ = self.stop_sender.send(()).await;
        if let Some(handle) = self.join_handle.lock().await.take() {
            handle
                .await
                .map_err(|e| Box::new(e) as _)
                .context(StopSynchronizer)?;
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub async fn register_table(&self, wal_location: WalLocation, table: ReaderTable) {
        self.inner.register_table(wal_location, table).await;
    }

    pub async fn start(&mut self, runtime: &Runtime) {
        let join_handle = runtime.spawn(
            self.inner
                .clone()
                .start_synchronize(self.stop_receiver.take().unwrap()),
        );
        *self.join_handle.lock().await = Some(join_handle);
    }
}

pub struct Inner {
    wal: WalManagerRef,
    config: WalSynchronizerConfig,
    tables: RwLock<BTreeMap<WalLocation, SynchronizeState>>,
}

impl Inner {
    #[allow(dead_code)]
    pub async fn register_table(&self, wal_location: WalLocation, table: ReaderTable) {
        let state = SynchronizeState {
            wal_location,
            table,
            last_synced_seq: AtomicU64::new(SequenceNumber::MIN),
        };
        self.tables.write().await.insert(wal_location, state);
    }

    pub async fn start_synchronize(self: Arc<Self>, mut stop_listener: Receiver<()>) {
        info!("Wal Synchronizer Started");

        // constants
        let read_context = ReadContext {
            batch_size: self.config.batch_size,
            ..Default::default()
        };

        loop {
            let mut invalid_tables = vec![];
            let tables = self.tables.read().await;
            // todo: consider clone [SynchronizeState] out to release the read lock.
            let states = tables.values().collect::<Vec<_>>();

            // Poll WAL region by region.
            for state in states {
                // check state before polling WAL
                if !state.check_state() {
                    invalid_tables.push(state.wal_location);
                    continue;
                }

                // build wal iterator
                let req = state.read_req();
                let mut iter = match self.wal.read_batch(&read_context, &req).await {
                    Err(e) => {
                        error!(
                            "Failed to read from WAL, read request: {:?}, error: {:?}",
                            req, e
                        );
                        // Failed to read from wal cannot indicate the region is invalid. Just
                        // ignore it.
                        continue;
                    }
                    Ok(iter) => iter,
                };

                // double check state before writing to table. Error due to
                // state changed after this check will be treat as normal error.
                if !state.check_state() {
                    invalid_tables.push(state.wal_location);
                    continue;
                }

                // read logs from iterator
                if let Err(e) = self.consume_logs(&mut iter, state).await {
                    error!("Failed to consume WAL, error: {:?}", e);
                }
            }

            drop(tables);
            self.purge_invalid_tables(&mut invalid_tables).await;

            if time::timeout(self.config.interval, stop_listener.recv())
                .await
                .is_ok()
            {
                info!("WAL Synchronizer stopped");
                break;
            }
        }
    }

    async fn consume_logs(
        &self,
        iter: &mut BatchLogIteratorAdapter,
        synchronize_state: &SynchronizeState,
    ) -> Result<()> {
        let mut buf = VecDeque::with_capacity(self.config.batch_size);
        let mut should_continue = true;
        let mut max_seq = SequenceNumber::MIN;

        while should_continue {
            // fetch entries
            buf = iter
                .next_log_entries(WalDecoder::default(), buf)
                .await
                .context(ReadWal)?;
            if buf.len() <= self.config.batch_size {
                should_continue = false;
            }

            // record max sequence number
            max_seq = max_seq.max(
                buf.back()
                    .map(|entry| entry.sequence)
                    .unwrap_or(SequenceNumber::MIN),
            );

            self.replay_logs(&mut buf, synchronize_state).await?;
        }

        // update sequence number in state
        synchronize_state
            .last_synced_seq
            .fetch_max(max_seq, Ordering::Relaxed);

        Ok(())
    }

    async fn replay_logs(
        &self,
        logs: &mut VecDeque<LogEntry<ReadPayload>>,
        synchronize_state: &SynchronizeState,
    ) -> Result<()> {
        for entry in logs.drain(..) {
            match entry.payload {
                ReadPayload::Write { row_group } => {
                    let write_req = WriteRequest { row_group };
                    synchronize_state
                        .table
                        .write(write_req)
                        .await
                        .map_err(|e| Box::new(e) as _)
                        .context(WriteLogBatch)?;
                }
                ReadPayload::AlterSchema { schema: _schema } => todo!(),
                ReadPayload::AlterOptions { options: _options } => todo!(),
            }
        }
        Ok(())
    }

    /// Remove invalid tables from poll list. This function will clear the
    /// `invalid_table` vec.
    async fn purge_invalid_tables(&self, invalid_tables: &mut Vec<WalLocation>) {
        if invalid_tables.is_empty() {
            return;
        }
        debug!(
            "Removing invalid region from WAL Synchronizer: {:?}",
            invalid_tables
        );

        let mut tables = self.tables.write().await;
        for wal_location in invalid_tables.drain(..) {
            tables.remove(&wal_location);
        }
    }
}

struct SynchronizeState {
    wal_location: WalLocation,
    table: ReaderTable,
    /// Atomic version of [SequenceNumber]
    last_synced_seq: AtomicU64,
}

impl SynchronizeState {
    pub fn read_req(&self) -> ReadRequest {
        ReadRequest {
            wal_location: self.wal_location,
            start: ReadBoundary::Excluded(self.last_synced_seq.load(Ordering::Relaxed)),
            end: ReadBoundary::Max,
        }
    }

    /// Check whether the underlying table is outdated.
    pub fn check_state(&self) -> bool {
        self.table.check_state()
    }
}

// todo: remove this mock mod
#[allow(unused)]
mod role_table {
    use std::convert::Infallible;

    use table_engine::table::WriteRequest;

    pub struct ReaderTable {}

    impl ReaderTable {
        pub async fn write(&self, request: WriteRequest) -> Result<(), Infallible> {
            Ok(())
        }

        pub fn check_state(&self) -> bool {
            true
        }
    }
}

#[cfg(test)]
mod test {
    use tokio::time::sleep;
    use wal::tests::util::{MemoryTableWalBuilder, TableKvTestEnv};

    use super::*;

    fn build_env() -> TableKvTestEnv {
        TableKvTestEnv::new(1, MemoryTableWalBuilder::default())
    }

    fn build_synchronizer(env: &TableKvTestEnv) -> WalSynchronizer {
        env.runtime.block_on(async {
            let wal = env.build_wal().await;
            WalSynchronizer::new(WalSynchronizerConfig::default(), wal)
        })
    }

    #[test]
    fn synchronizer_start_stop() {
        let env = build_env();
        let runtime = env.runtime.clone();
        let mut synchronizer = build_synchronizer(&env);

        runtime.clone().block_on(async move {
            synchronizer.start(&runtime).await;
            sleep(Duration::from_secs(1)).await;
            synchronizer.stop().await.unwrap();
        });
    }
}
