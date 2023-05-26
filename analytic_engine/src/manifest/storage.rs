// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::collections::VecDeque;

use async_trait::async_trait;
use ceresdbproto::manifest as manifest_pb;
use common_types::SequenceNumber;
use common_util::error::BoxError;
use log::warn;
use object_store::{ObjectStoreRef, Path};
use parquet::data_type::AsBytes;
use prost::Message;
use snafu::ResultExt;
use table_engine::table::TableId;
use wal::{
    kv_encoder::LogBatchEncoder,
    log_batch::LogEntry,
    manager::{
        BatchLogIteratorAdapter, ReadBoundary, ReadContext, ReadRequest, RegionId, ScanContext,
        WalLocation, WalManagerRef, WriteContext,
    },
};

use super::{
    details::Options,
    meta_edit::{MetaUpdate, MetaUpdateDecoder, MetaUpdatePayload, Snapshot},
};
use crate::{manifest::error::*, space::SpaceId};

/// Manifest Snapshot store
#[async_trait]
pub trait MetaUpdateSnapshotStore: std::fmt::Debug + Clone {
    async fn store(&self, request: StoreSnapshotRequest) -> Result<()>;

    async fn load(&self, request: LoadSnapshotRequest) -> Result<Option<Snapshot>>;
}

pub struct StoreSnapshotRequest {
    pub snapshot: Snapshot,
    pub snapshot_path: Path,
}

pub struct LoadSnapshotRequest {
    pub snapshot_path: Path,
}

/// Manifest Log store
#[async_trait]
pub trait MetaUpdateLogStore: std::fmt::Debug + Clone {
    type Iter: MetaUpdateLogEntryIterator + Send;

    async fn scan(&self, request: ScanLogRequest) -> Result<Self::Iter>;

    async fn append(&self, request: AppendRequest) -> Result<SequenceNumber>;

    async fn delete_up_to(&self, request: DeleteRequest) -> Result<()>;
}

pub enum ScanLogRequest {
    Table(ScanTableRequest),
    Region(ScanRegionRequest),
}

pub struct ScanTableRequest {
    pub opts: Options,
    pub location: WalLocation,
    pub start: ReadBoundary,
}

pub struct ScanRegionRequest {
    pub opts: Options,
    pub region_id: RegionId,
}

pub struct AppendRequest {
    pub opts: Options,
    pub location: WalLocation,
    pub meta_update: MetaUpdate,
}

pub struct DeleteRequest {
    pub opts: Options,
    pub location: WalLocation,
    pub inclusive_end: SequenceNumber,
}

/// Object store impl for manifest snapshot store
#[derive(Debug, Clone)]
pub struct ObjectStoreBasedSnapshotStore {
    pub store: ObjectStoreRef,
}

impl ObjectStoreBasedSnapshotStore {
    const CURRENT_SNAPSHOT_NAME: &str = "current";
    const SNAPSHOT_PATH_PREFIX: &str = "manifest/snapshot";

    pub fn new(store: ObjectStoreRef) -> Self {
        Self { store }
    }

    pub fn snapshot_path(space_id: SpaceId, table_id: TableId) -> Path {
        format!(
            "{}/{}/{}/{}",
            Self::SNAPSHOT_PATH_PREFIX,
            space_id,
            table_id,
            Self::CURRENT_SNAPSHOT_NAME,
        )
        .into()
    }
}

#[async_trait]
impl MetaUpdateSnapshotStore for ObjectStoreBasedSnapshotStore {
    /// Store the latest snapshot to the underlying store by overwriting the old
    /// snapshot.
    async fn store(&self, request: StoreSnapshotRequest) -> Result<()> {
        let snapshot_pb = manifest_pb::Snapshot::from(request.snapshot);
        let payload = snapshot_pb.encode_to_vec();

        // The atomic write is ensured by the [`ObjectStore`] implementation.
        self.store
            .put(&request.snapshot_path, payload.into())
            .await
            .context(StoreSnapshot)?;

        Ok(())
    }

    /// Load the `current_snapshot` file from the underlying store, and with the
    /// mapping info in it load the latest snapshot file then.
    async fn load(&self, request: LoadSnapshotRequest) -> Result<Option<Snapshot>> {
        let get_res = self.store.get(&request.snapshot_path).await;
        if let Err(object_store::ObjectStoreError::NotFound { path, source }) = &get_res {
            warn!(
                "Current snapshot file doesn't exist, path:{}, err:{}",
                path, source
            );
            return Ok(None);
        };

        // TODO: currently, this is just a workaround to handle the case where the error
        // is not thrown as [object_store::ObjectStoreError::NotFound].
        if let Err(err) = &get_res {
            let err_msg = err.to_string().to_lowercase();
            if err_msg.contains("404") || err_msg.contains("not found") {
                warn!("Current snapshot file doesn't exist, err:{}", err);
                return Ok(None);
            }
        }

        let payload = get_res
            .context(FetchSnapshot)?
            .bytes()
            .await
            .context(FetchSnapshot)?;
        let snapshot_pb =
            manifest_pb::Snapshot::decode(payload.as_bytes()).context(DecodeSnapshot)?;
        let snapshot = Snapshot::try_from(snapshot_pb)
            .box_err()
            .context(LoadSnapshot)?;

        Ok(Some(snapshot))
    }
}

/// Wal impl for manifest log store
#[derive(Debug, Clone)]
pub struct WalBasedLogStore {
    pub wal_manager: WalManagerRef,
}

impl WalBasedLogStore {
    async fn scan_logs_of_table(&self, request: ScanTableRequest) -> Result<MetaUpdateReaderImpl> {
        let ctx = ReadContext {
            timeout: request.opts.scan_timeout.0,
            batch_size: request.opts.scan_batch_size,
        };

        let read_req = ReadRequest {
            location: request.location,
            start: request.start,
            end: ReadBoundary::Max,
        };

        let iter = self
            .wal_manager
            .read_batch(&ctx, &read_req)
            .await
            .context(ReadWal)?;

        Ok(MetaUpdateReaderImpl {
            iter,
            has_next: true,
            buffer: VecDeque::with_capacity(ctx.batch_size),
        })
    }

    async fn scan_logs_of_region(
        &self,
        request: ScanRegionRequest,
    ) -> Result<MetaUpdateReaderImpl> {
        let ctx = ScanContext {
            timeout: request.opts.scan_timeout.0,
            batch_size: request.opts.scan_batch_size,
        };

        let scan_req = wal::manager::ScanRequest {
            region_id: request.region_id,
        };

        let iter = self
            .wal_manager
            .scan(&ctx, &scan_req)
            .await
            .context(ReadWal)?;

        Ok(MetaUpdateReaderImpl {
            iter,
            has_next: true,
            buffer: VecDeque::with_capacity(ctx.batch_size),
        })
    }
}

#[async_trait]
impl MetaUpdateLogStore for WalBasedLogStore {
    type Iter = MetaUpdateReaderImpl;

    async fn scan(&self, request: ScanLogRequest) -> Result<Self::Iter> {
        match request {
            ScanLogRequest::Table(req) => self.scan_logs_of_table(req).await,
            ScanLogRequest::Region(req) => self.scan_logs_of_region(req).await,
        }
    }

    async fn append(&self, request: AppendRequest) -> Result<SequenceNumber> {
        let payload = MetaUpdatePayload::from(request.meta_update);
        let log_batch_encoder = LogBatchEncoder::create(request.location);
        let log_batch = log_batch_encoder.encode(&payload).context(EncodePayloads {
            wal_location: request.location,
        })?;

        let write_ctx = WriteContext {
            timeout: request.opts.store_timeout.0,
        };

        self.wal_manager
            .write(&write_ctx, &log_batch)
            .await
            .context(WriteWal)
    }

    async fn delete_up_to(&self, request: DeleteRequest) -> Result<()> {
        self.wal_manager
            .mark_delete_entries_up_to(request.location, request.inclusive_end)
            .await
            .context(CleanWal)
    }
}

#[async_trait]
pub trait MetaUpdateLogEntryIterator {
    async fn next_update(&mut self) -> Result<Option<MetaUpdateLogEntry>>;
}

pub struct MetaUpdateLogEntry {
    pub sequence: SequenceNumber,
    pub table_id: TableId,
    pub meta_update: MetaUpdate,
}

/// Implementation of [`MetaUpdateLogEntryIterator`].
#[derive(Debug)]
pub struct MetaUpdateReaderImpl {
    iter: BatchLogIteratorAdapter,
    has_next: bool,
    buffer: VecDeque<LogEntry<MetaUpdate>>,
}

#[async_trait]
impl MetaUpdateLogEntryIterator for MetaUpdateReaderImpl {
    async fn next_update(&mut self) -> Result<Option<MetaUpdateLogEntry>> {
        if !self.has_next {
            return Ok(None);
        }

        if self.buffer.is_empty() {
            let decoder = MetaUpdateDecoder;
            let buffer = std::mem::take(&mut self.buffer);
            self.buffer = self
                .iter
                .next_log_entries(decoder, buffer)
                .await
                .context(ReadEntry)?;
        }

        match self.buffer.pop_front() {
            Some(entry) => {
                let log_entry = MetaUpdateLogEntry {
                    sequence: entry.sequence,
                    table_id: TableId::new(entry.table_id),
                    meta_update: entry.payload,
                };
                Ok(Some(log_entry))
            }
            None => {
                self.has_next = false;
                Ok(None)
            }
        }
    }
}
