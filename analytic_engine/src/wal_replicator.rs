// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! WAL Replicator implementation.

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
        RegionId, WalManagerRef,
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

    #[snafu(display("Failed to stop replicator, err:{}", source))]
    StopReplicator {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

define_result!(Error);

pub struct WalReplicatorConfig {
    /// Interval between two syncs
    interval: Duration,
    /// Used as WAL's read batch size
    batch_size: usize,
}

impl Default for WalReplicatorConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
            batch_size: 128,
        }
    }
}

/// A background replicator that keep polling WAL update.
///
/// This [WalReplicator] has a queue of [RegionId]s that need replication.
/// Others can register new region with [register_table] method. And invalid
/// table will be removed automatically. The workflow looks like:
///
/// ```plaintext
///            register IDs
///           need replication
///      ┌─────────────────────┐
///      │                     │
///      │              ┌──────▼───────┐
/// ┌────┴─────┐        │  background  │
/// │Role Table│        │WAL Replicator│
/// └────▲─────┘        └──────┬───────┘
///      │                     │
///      └─────────────────────┘
///           replicate log
///             to table
/// ```
pub struct WalReplicator {
    inner: Arc<Inner>,
    stop_sender: Sender<()>,
    join_handle: Mutex<Option<JoinHandle<()>>>,
    stop_receiver: Option<Receiver<()>>,
}

impl WalReplicator {
    pub fn new(config: WalReplicatorConfig, wal: WalManagerRef) -> Self {
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
                .context(StopReplicator)?;
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub async fn register_table(&self, region_id: RegionId, table: ReaderTable) {
        self.inner.register_table(region_id, table).await;
    }

    pub async fn start(&mut self, runtime: &Runtime) {
        let join_handle = runtime.spawn(
            self.inner
                .clone()
                .start_replicate(self.stop_receiver.take().unwrap()),
        );
        *self.join_handle.lock().await = Some(join_handle);
    }
}

pub struct Inner {
    wal: WalManagerRef,
    config: WalReplicatorConfig,
    tables: RwLock<BTreeMap<RegionId, ReplicateState>>,
}

impl Inner {
    #[allow(dead_code)]
    pub async fn register_table(&self, region_id: RegionId, table: ReaderTable) {
        let state = ReplicateState {
            region_id,
            table,
            last_synced_seq: AtomicU64::new(SequenceNumber::MIN),
        };
        self.tables.write().await.insert(region_id, state);
    }

    pub async fn start_replicate(self: Arc<Self>, mut stop_listener: Receiver<()>) {
        info!("Wal Replicator Started");

        // constants
        let read_context = ReadContext {
            batch_size: self.config.batch_size,
            ..Default::default()
        };

        loop {
            let mut invalid_regions = vec![];
            let tables = self.tables.read().await;
            // todo: consider clone [ReplicateState] out to release the read lock.
            let states = tables.values().collect::<Vec<_>>();

            // Poll WAL region by region.
            for state in states {
                // check state before polling WAL
                if !state.check_state() {
                    invalid_regions.push(state.region_id);
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
                    invalid_regions.push(state.region_id);
                    continue;
                }

                // read logs from iterator
                if let Err(e) = self.consume_logs(&mut iter, state).await {
                    error!("Failed to consume WAL, error: {:?}", e);
                }
            }

            drop(tables);
            self.purge_invalid_region(&mut invalid_regions).await;

            if time::timeout(self.config.interval, stop_listener.recv())
                .await
                .is_ok()
            {
                info!("WAL Replicator stopped");
                break;
            }
        }
    }

    async fn consume_logs(
        &self,
        iter: &mut BatchLogIteratorAdapter,
        replicate_state: &ReplicateState,
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

            self.replay_logs(&mut buf, replicate_state).await?;
        }

        // update sequence number in state
        replicate_state
            .last_synced_seq
            .fetch_max(max_seq, Ordering::Relaxed);

        Ok(())
    }

    async fn replay_logs(
        &self,
        logs: &mut VecDeque<LogEntry<ReadPayload>>,
        replicate_state: &ReplicateState,
    ) -> Result<()> {
        for entry in logs.drain(..) {
            match entry.payload {
                ReadPayload::Write { row_group } => {
                    let write_req = WriteRequest { row_group };
                    replicate_state
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

    /// Remove invalid regions from poll list. This function will clear the
    /// `invalid_region` vec.
    async fn purge_invalid_region(&self, invalid_regions: &mut Vec<RegionId>) {
        if invalid_regions.is_empty() {
            return;
        }
        debug!(
            "Removing invalid region from WAL Replicator: {:?}",
            invalid_regions
        );

        let mut tables = self.tables.write().await;
        for region in invalid_regions.drain(..) {
            tables.remove(&region);
        }
    }
}

struct ReplicateState {
    region_id: RegionId,
    table: ReaderTable,
    /// Atomic version of [SequenceNumber]
    last_synced_seq: AtomicU64,
}

impl ReplicateState {
    pub fn read_req(&self) -> ReadRequest {
        ReadRequest {
            region_id: self.region_id,
            start: ReadBoundary::Excluded(self.last_synced_seq.load(Ordering::Relaxed)),
            end: ReadBoundary::Max,
        }
    }

    /// Check whether the underlying table is outdated.
    pub fn check_state(&self) -> bool {
        self.table.check_state()
    }
}

// mock mod
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

#[cfg(all(test))]
mod test {
    use tokio::time::sleep;
    use wal::tests::util::{MemoryTableWalBuilder, TableKvTestEnv};

    use super::*;

    fn build_env() -> TableKvTestEnv {
        TableKvTestEnv::new(1, MemoryTableWalBuilder::default())
    }

    fn build_replicator(env: &TableKvTestEnv) -> WalReplicator {
        env.runtime.block_on(async {
            let wal = env.build_wal().await;
            WalReplicator::new(WalReplicatorConfig::default(), wal)
        })
    }

    #[test]
    fn replicator_start_stop() {
        let env = build_env();
        let runtime = env.runtime.clone();
        let mut replicator = build_replicator(&env);

        runtime.clone().block_on(async move {
            replicator.start(&runtime).await;
            sleep(Duration::from_secs(1)).await;
            replicator.stop().await.unwrap();
        });
    }
}
