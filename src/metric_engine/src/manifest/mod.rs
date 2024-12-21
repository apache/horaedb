// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

mod encoding;
use std::{
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, LazyLock,
    },
    time::{Duration, SystemTime},
};

use anyhow::Context;
use async_scoped::TokioScope;
use bytes::Bytes;
pub use encoding::{ManifestUpdate, Snapshot};
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use object_store::{path::Path, PutPayload};
use prost::Message;
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    RwLock,
};
use tracing::{debug, error, info, trace};

use crate::{
    config::ManifestMergeOptions,
    sst::{FileId, FileMeta, SstFile},
    types::{ObjectStoreRef, RuntimeRef, TimeRange},
    AnyhowError, Result,
};

pub const PREFIX_PATH: &str = "manifest";
pub const SNAPSHOT_FILENAME: &str = "snapshot";
pub const DELTA_PREFIX: &str = "delta";

// Used for manifest delta filename
// This number mustn't go backwards on restarts, otherwise file id
// collisions are possible. So don't change time on the server
// between server restarts.
static NEXT_ID: LazyLock<AtomicU64> = LazyLock::new(|| {
    AtomicU64::new(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64,
    )
});

pub type ManifestRef = Arc<Manifest>;

pub struct Manifest {
    delta_dir: Path,
    store: ObjectStoreRef,
    merger: Arc<ManifestMerger>,

    ssts: RwLock<Vec<SstFile>>,
}

impl Manifest {
    pub async fn try_new(
        root_dir: String,
        store: ObjectStoreRef,
        runtime: RuntimeRef,
        merge_options: ManifestMergeOptions,
    ) -> Result<Self> {
        let snapshot_path = Path::from(format!("{root_dir}/{PREFIX_PATH}/{SNAPSHOT_FILENAME}"));
        let delta_dir = Path::from(format!("{root_dir}/{PREFIX_PATH}/{DELTA_PREFIX}"));

        let merger = ManifestMerger::try_new(
            snapshot_path.clone(),
            delta_dir.clone(),
            store.clone(),
            merge_options,
        )
        .await?;
        let snapshot = read_snapshot(&store, &snapshot_path).await?;
        let ssts = snapshot.into_ssts();
        debug!(sst_len = ssts.len(), "Load manifest snapshot when startup");
        {
            let merger = merger.clone();
            // Start merger in background
            runtime.spawn(async move {
                merger.run().await;
            });
        }

        Ok(Self {
            delta_dir,
            store,
            merger,
            ssts: RwLock::new(ssts),
        })
    }

    pub async fn add_file(&self, id: FileId, meta: FileMeta) -> Result<()> {
        let update = ManifestUpdate::new(vec![SstFile::new(id, meta)], Vec::new());
        self.update(update).await
    }

    pub async fn update(&self, update: ManifestUpdate) -> Result<()> {
        self.merger.maybe_schedule_merge().await?;
        self.merger.inc_delta_num();
        let res = self.update_inner(update).await;
        if res.is_err() {
            self.merger.dec_delta_num();
        }

        res
    }

    pub async fn update_inner(&self, update: ManifestUpdate) -> Result<()> {
        let path = Path::from(format!("{}/{}", self.delta_dir, Self::allocate_id()));
        let pb_update = pb_types::ManifestUpdate::from(update.clone());
        let mut buf: Vec<u8> = Vec::with_capacity(pb_update.encoded_len());
        pb_update
            .encode(&mut buf)
            .context("failed to encode manifest update")?;

        // 1. Persist the delta manifest
        self.store
            .put(&path, PutPayload::from_bytes(Bytes::from(buf)))
            .await
            .with_context(|| format!("Failed to write delta manifest, path:{}", path))?;

        // 2. Update cached payload
        {
            let mut ssts = self.ssts.write().await;
            for file in update.to_adds {
                ssts.push(file);
            }
            // TODO: sort files in payload, so we can delete files more
            // efficiently.
            ssts.retain(|file| !update.to_deletes.contains(&file.id()));
        }

        Ok(())
    }

    // TODO: avoid clone
    pub async fn all_ssts(&self) -> Vec<SstFile> {
        let ssts = self.ssts.read().await;
        ssts.clone()
    }

    pub async fn find_ssts(&self, time_range: &TimeRange) -> Vec<SstFile> {
        let ssts = self.ssts.read().await;

        ssts.iter()
            .filter(move |f| f.meta().time_range.overlaps(time_range))
            .cloned()
            .collect()
    }

    fn allocate_id() -> u64 {
        NEXT_ID.fetch_add(1, Ordering::SeqCst)
    }
}

enum MergeType {
    Hard,
    Soft,
}

struct ManifestMerger {
    snapshot_path: Path,
    delta_dir: Path,
    store: ObjectStoreRef,
    sender: Sender<MergeType>,
    receiver: RwLock<Receiver<MergeType>>,
    deltas_num: AtomicUsize,
    merge_options: ManifestMergeOptions,
}

impl ManifestMerger {
    async fn try_new(
        snapshot_path: Path,
        delta_dir: Path,
        store: ObjectStoreRef,
        merge_options: ManifestMergeOptions,
    ) -> Result<Arc<Self>> {
        let (tx, rx) = mpsc::channel(merge_options.channel_size);
        let merger = Self {
            snapshot_path,
            delta_dir,
            store,
            sender: tx,
            receiver: RwLock::new(rx),
            // Init this to 0, because we will merge all delta files when startup.
            deltas_num: AtomicUsize::new(0),
            merge_options,
        };
        // Merge all delta files when startup
        merger.do_merge(true /* first_run */).await?;

        Ok(Arc::new(merger))
    }

    async fn run(&self) {
        let merge_interval = Duration::from_secs(self.merge_options.merge_interval_seconds as u64);
        let mut receiver = self.receiver.write().await;
        info!(merge_interval = ?merge_interval, "Start manifest merge background job");
        loop {
            tokio::select! {
                _ = tokio::time::sleep(merge_interval) => {
                    if self.deltas_num.load(Ordering::Relaxed) > self.merge_options.min_merge_threshold {
                        if let Err(err) = self.do_merge(false /*first_run*/).await {
                            error!("Failed to merge delta, err:{err}");
                        }
                    }
                }
                _merge_type = receiver.recv() => {
                    if self.deltas_num.load(Ordering::Relaxed) > self.merge_options.min_merge_threshold {
                        if let Err(err) = self.do_merge(false /*first_run*/).await {
                            error!("Failed to merge delta, err:{err}");
                        }
                    }
                }
            }
        }
    }

    fn schedule_merge(&self, task: MergeType) {
        if let Err(err) = self.sender.try_send(task) {
            trace!("Failed to send merge task, err:{err}");
        }
    }

    async fn maybe_schedule_merge(&self) -> Result<()> {
        let current_num = self.deltas_num.load(Ordering::Relaxed);
        let hard_limit = self.merge_options.hard_merge_threshold;
        if current_num > hard_limit {
            self.schedule_merge(MergeType::Hard);
            return Err(AnyhowError::msg(format!(
                "Manifest has too many delta files, value:{current_num}, hard_limit:{hard_limit}"
            ))
            .into());
        } else if current_num > self.merge_options.soft_merge_threshold {
            self.schedule_merge(MergeType::Soft);
        }

        Ok(())
    }

    fn inc_delta_num(&self) {
        let prev = self.deltas_num.fetch_add(1, Ordering::Relaxed);
        trace!(prev, "inc delta num");
    }

    fn dec_delta_num(&self) {
        let prev = self.deltas_num.fetch_sub(1, Ordering::Relaxed);
        trace!(prev, "dec delta num");
    }

    async fn do_merge(&self, first_run: bool) -> Result<()> {
        let paths = list_delta_paths(&self.store, &self.delta_dir).await?;
        if paths.is_empty() {
            return Ok(());
        }
        if first_run {
            self.deltas_num.store(paths.len(), Ordering::Relaxed);
        }

        let (_, results) = TokioScope::scope_and_block(|scope| {
            for path in &paths {
                scope.spawn(async { read_delta_file(&self.store, path).await });
            }
        });

        let mut snapshot = read_snapshot(&self.store, &self.snapshot_path).await?;
        trace!(sst_ids = ?snapshot.records.iter().map(|r| r.id()).collect_vec(), "Before snapshot merge deltas");
        // Since the deltas is unsorted, so we have to first add all new files, then
        // delete old files.
        let mut to_deletes = Vec::new();
        for res in results {
            let manifest_update = res.context("Failed to join read delta files task")??;
            snapshot.add_records(manifest_update.to_adds);
            to_deletes.extend(manifest_update.to_deletes);
        }
        snapshot.delete_records(to_deletes);
        trace!(sst_ids = ?snapshot.records.iter().map(|r| r.id()).collect_vec(), "After snapshot merge deltas");
        let snapshot_bytes = snapshot.into_bytes()?;
        let put_payload = PutPayload::from_bytes(snapshot_bytes);
        // 1. Persist the snapshot
        self.store
            .put(&self.snapshot_path, put_payload)
            .await
            .with_context(|| format!("Failed to update manifest, path:{}", self.snapshot_path))?;

        // 2. Delete the merged manifest files
        let (_, results) = TokioScope::scope_and_block(|scope| {
            for path in &paths {
                trace!(path = ?path, "delete delta file");
                scope.spawn(async { delete_delta_file(&self.store, path).await });
            }
        });

        for res in results {
            match res {
                Err(e) => {
                    error!("Failed to join delete delta files task, err:{e}")
                }
                Ok(v) => {
                    if let Err(e) = v {
                        error!("Failed to delete delta, err:{e}")
                    } else {
                        self.dec_delta_num();
                    }
                }
            }
        }

        Ok(())
    }
}

async fn read_snapshot(store: &ObjectStoreRef, path: &Path) -> Result<Snapshot> {
    match store.get(path).await {
        Ok(v) => {
            let bytes = v
                .bytes()
                .await
                .with_context(|| format!("Failed to read manifest snapshot, path:{path}"))?;
            Snapshot::try_from(bytes)
        }
        Err(err) => {
            if err.to_string().contains("not found") {
                Ok(Snapshot::default())
            } else {
                let context = format!("Failed to read manifest snapshot, path:{path}");
                Err(AnyhowError::new(err).context(context).into())
            }
        }
    }
}

async fn read_delta_file(store: &ObjectStoreRef, sst_path: &Path) -> Result<ManifestUpdate> {
    let bytes = store
        .get(sst_path)
        .await
        .with_context(|| format!("failed to get delta file, path:{sst_path}"))?
        .bytes()
        .await
        .with_context(|| format!("failed to read delta file, path:{sst_path}"))?;

    let pb_update = pb_types::ManifestUpdate::decode(bytes)
        .with_context(|| format!("failed to decode delta file, path:{sst_path}"))?;

    let update = ManifestUpdate::try_from(pb_update)
        .with_context(|| format!("failed to convert delta file, path:{sst_path}"))?;
    Ok(update)
}

async fn delete_delta_file(store: &ObjectStoreRef, path: &Path) -> Result<()> {
    store
        .delete(path)
        .await
        .with_context(|| format!("Failed to delete delta files, path:{path}"))?;

    Ok(())
}

async fn list_delta_paths(store: &ObjectStoreRef, delta_dir: &Path) -> Result<Vec<Path>> {
    let paths = store
        .list(Some(delta_dir))
        .map(|value| {
            value
                .map(|v| v.location)
                .with_context(|| format!("Failed to list delta paths, delta dir:{}", delta_dir))
        })
        .try_collect::<Vec<_>>()
        .await?;

    Ok(paths)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::local::LocalFileSystem;
    use tokio::time::sleep;

    use super::*;

    #[test]
    fn test_find_manifest() {
        let root_dir = temp_dir::TempDir::new().unwrap();
        let runtime = Arc::new(tokio::runtime::Runtime::new().unwrap());
        let rt = runtime.clone();
        let store = Arc::new(LocalFileSystem::new());

        rt.block_on(async move {
            let manifest = Manifest::try_new(
                root_dir.path().to_string_lossy().to_string(),
                store,
                runtime.clone(),
                ManifestMergeOptions::default(),
            )
            .await
            .unwrap();

            for i in 0..20 {
                let time_range = (i..i + 1).into();
                let meta = FileMeta {
                    max_sequence: i as u64,
                    num_rows: i as u32,
                    size: i as u32,
                    time_range,
                };
                manifest.add_file(i as u64, meta).await.unwrap();
            }

            let find_range = (10..15).into();
            let mut ssts = manifest.find_ssts(&find_range).await;

            let mut expected_ssts = (10..15)
                .map(|i| {
                    let id = i as u64;
                    let time_range = (i..i + 1).into();
                    let meta = FileMeta {
                        max_sequence: i as u64,
                        num_rows: i as u32,
                        size: i as u32,
                        time_range,
                    };
                    SstFile::new(id, meta)
                })
                .collect::<Vec<_>>();

            expected_ssts.sort_by_key(|a| a.id());
            ssts.sort_by_key(|a| a.id());
            assert_eq!(expected_ssts, ssts);
        });
    }

    #[test]
    fn test_merge_manifest() {
        let root_dir = temp_dir::TempDir::new()
            .unwrap()
            .path()
            .to_string_lossy()
            .to_string();
        let snapshot_path = Path::from(format!("{root_dir}/{PREFIX_PATH}/{SNAPSHOT_FILENAME}"));
        let delta_dir = Path::from(format!("{root_dir}/{PREFIX_PATH}/{DELTA_PREFIX}"));
        let runtime = Arc::new(tokio::runtime::Runtime::new().unwrap());
        let rt = runtime.clone();

        rt.block_on(async move {
            let store: ObjectStoreRef = Arc::new(LocalFileSystem::new());
            let manifest = Manifest::try_new(
                root_dir,
                store.clone(),
                runtime.clone(),
                ManifestMergeOptions {
                    merge_interval_seconds: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

            // Add manifest files
            for i in 0..20 {
                let time_range = (i..i + 1).into();
                let meta = FileMeta {
                    max_sequence: i as u64,
                    num_rows: i as u32,
                    size: i as u32,
                    time_range,
                };
                manifest.add_file(i as u64, meta).await.unwrap();
            }

            // Wait for merge manifest to finish
            sleep(Duration::from_secs(2)).await;

            let mut mem_ssts = manifest.ssts.read().await.clone();
            let snapshot = read_snapshot(&store, &snapshot_path).await.unwrap();
            let mut ssts = snapshot.into_ssts();

            mem_ssts.sort_by_key(|a| a.id());
            ssts.sort_by_key(|a| a.id());
            assert_eq!(mem_ssts, ssts);

            let delta_paths = list_delta_paths(&store, &delta_dir).await.unwrap();
            assert!(delta_paths.is_empty());
        })
    }
}
