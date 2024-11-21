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

use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::Context;
use bytes::Bytes;
use futures::{stream::FuturesUnordered, StreamExt, TryStreamExt};
use object_store::{path::Path, PutPayload};
use prost::Message;
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    RwLock,
};
use tracing::error;

use crate::{
    sst::{FileId, FileMeta, SstFile},
    types::{ObjectStoreRef, TimeRange},
    AnyhowError, Error, Result,
};

pub const PREFIX_PATH: &str = "manifest";
pub const SNAPSHOT_FILENAME: &str = "snapshot";
pub const DELTA_PREFIX: &str = "delta";

pub struct Manifest {
    delta_dir: Path,
    store: ObjectStoreRef,
    merger: Arc<ManifestMerger>,

    payload: RwLock<Payload>,
}

pub struct Payload {
    files: Vec<SstFile>,
}

impl Payload {
    // TODO: we could sort sst files by name asc, then the dedup will be more
    // efficient
    pub fn dedup_files(&mut self) {
        let mut seen = HashSet::with_capacity(self.files.len());
        self.files.retain(|file| seen.insert(file.id));
    }
}

impl TryFrom<pb_types::Manifest> for Payload {
    type Error = Error;

    fn try_from(value: pb_types::Manifest) -> Result<Self> {
        let files = value
            .files
            .into_iter()
            .map(SstFile::try_from)
            .collect::<Result<Vec<_>>>()?;

        Ok(Self { files })
    }
}

impl From<Payload> for pb_types::Manifest {
    fn from(value: Payload) -> Self {
        pb_types::Manifest {
            files: value
                .files
                .into_iter()
                .map(pb_types::SstFile::from)
                .collect(),
        }
    }
}

impl Manifest {
    pub async fn try_new(
        root_dir: String,
        store: ObjectStoreRef,
        merge_options: ManifestMergeOptions,
    ) -> Result<Self> {
        let snapshot_path = Path::from(format!("{root_dir}/{SNAPSHOT_FILENAME}"));
        let delta_dir = Path::from(format!("{root_dir}/{DELTA_PREFIX}"));

        let merger = ManifestMerger::try_new(
            snapshot_path.clone(),
            delta_dir.clone(),
            store.clone(),
            merge_options,
        )
        .await?;
        let manifest_merge_clone = merger.clone();

        // Start merge manifest task
        tokio::spawn(async move {
            manifest_merge_clone.run().await;
        });

        // TODO: merge deltas with snapshot
        let payload = match store.get(&snapshot_path).await {
            Ok(v) => {
                let bytes = v
                    .bytes()
                    .await
                    .context("failed to read manifest snapshot")?;
                let pb_payload = pb_types::Manifest::decode(bytes)
                    .context("failed to decode manifest snapshot")?;
                Payload::try_from(pb_payload)?
            }
            Err(err) => {
                if err.to_string().contains("not found") {
                    Payload { files: vec![] }
                } else {
                    let context = format!("Failed to get manifest snapshot, path:{snapshot_path}");
                    return Err(AnyhowError::new(err).context(context).into());
                }
            }
        };

        Ok(Self {
            delta_dir,
            store,
            merger,
            payload: RwLock::new(payload),
        })
    }

    pub async fn add_file(&self, id: FileId, meta: FileMeta) -> Result<()> {
        // Return error when there
        self.merger.maybe_schedule_merge().await?;

        let new_sst_path = Path::from(format!("{}/{id}", self.delta_dir));
        let new_sst = SstFile { id, meta };

        let new_sst_payload = pb_types::SstFile::from(new_sst.clone());
        let mut buf: Vec<u8> = Vec::with_capacity(new_sst_payload.encoded_len());
        new_sst_payload
            .encode(&mut buf)
            .context("failed to encode manifest file")?;
        let put_payload = PutPayload::from_bytes(Bytes::from(buf));

        // 1. Persist the manifest file
        self.store
            .put(&new_sst_path, put_payload)
            .await
            .with_context(|| format!("Failed to write delta manifest, path:{}", new_sst_path))?;

        // 2. Update cached payload
        {
            let mut payload = self.payload.write().await;
            payload.files.push(new_sst);
        }

        Ok(())
    }

    pub async fn find_ssts(&self, time_range: &TimeRange) -> Vec<SstFile> {
        let payload = self.payload.read().await;

        payload
            .files
            .iter()
            .filter(move |f| f.meta.time_range.overlaps(time_range))
            .cloned()
            .collect()
    }
}

enum MergeType {
    Hard,
    Soft,
}

#[derive(Clone)]
pub struct ManifestMergeOptions {
    channel_size: usize,
    merge_interval_seconds: usize,
    min_merge_threshold: usize,
    hard_merge_threshold: usize,
    soft_merge_threshold: usize,
}

impl Default for ManifestMergeOptions {
    fn default() -> Self {
        Self {
            channel_size: 100,
            merge_interval_seconds: 5,
            min_merge_threshold: 10,
            soft_merge_threshold: 50,
            hard_merge_threshold: 90,
        }
    }
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
        let manifest_merge = Self {
            snapshot_path,
            delta_dir,
            store,
            sender: tx,
            receiver: RwLock::new(rx),
            deltas_num: AtomicUsize::new(0),
            merge_options,
        };
        // Merge all manifest files when start
        manifest_merge.do_merge().await?;

        Ok(Arc::new(manifest_merge))
    }

    async fn run(&self) {
        let merge_interval = Duration::from_secs(self.merge_options.merge_interval_seconds as u64);
        let mut receiver = self.receiver.write().await;
        loop {
            tokio::select! {
                _ = tokio::time::sleep(merge_interval) => {
                    if self.deltas_num.load(Ordering::Relaxed) > self.merge_options.min_merge_threshold {
                        if let Err(err) = self.do_merge().await {
                            error!("Failed to merge delta, err:{err}");
                        }
                    }

                }
                _merge_type = receiver.recv() => {
                    if let Err(err) = self.do_merge().await {
                        error!("Failed to merge delta, err:{err}");
                    }
                }
            }
        }
    }

    async fn sender(&self, task: MergeType) {
        if let Err(err) = self.sender.send(task).await {
            error!("Failed to send merge task, err:{err}");
        }
    }

    async fn maybe_schedule_merge(&self) -> Result<()> {
        let current_num = self.deltas_num.load(Ordering::Relaxed);

        if current_num > self.merge_options.hard_merge_threshold {
            self.sender(MergeType::Hard).await;
            return Err(AnyhowError::msg(format!(
                "Manifest has too many delta files, value:{current_num}"
            ))
            .into());
        } else if current_num > self.merge_options.soft_merge_threshold {
            self.sender(MergeType::Soft).await;
        }

        Ok(())
    }

    async fn do_merge(&self) -> Result<()> {
        let paths = self
            .store
            .list(Some(&self.delta_dir))
            .map(|value| {
                value
                    .map(|v| v.location)
                    .context("failed to get delta file")
            })
            .try_collect::<Vec<_>>()
            .await?;
        if paths.is_empty() {
            return Ok(());
        }

        let mut stream_read = FuturesUnordered::new();
        for path in paths.clone() {
            let store = self.store.clone();
            // TODO: use thread pool to read manifest files
            let handle = tokio::spawn(async move { read_delta_file(store, path).await });
            stream_read.push(handle);
        }
        let mut delta_files = Vec::with_capacity(stream_read.len());
        while let Some(res) = stream_read.next().await {
            let res = res.context("failed to read delta file")??;
            delta_files.push(res);
        }

        let mut payload = match self.store.get(&self.snapshot_path).await {
            Ok(v) => {
                let bytes = v
                    .bytes()
                    .await
                    .context("failed to read manifest snapshot")?;
                let pb_payload = pb_types::Manifest::decode(bytes)
                    .context("failed to decode manifest snapshot")?;
                Payload::try_from(pb_payload)?
            }
            Err(err) => {
                if err.to_string().contains("not found") {
                    Payload { files: vec![] }
                } else {
                    let context = format!(
                        "Failed to get manifest snapshot, path:{}",
                        self.snapshot_path
                    );
                    return Err(AnyhowError::new(err).context(context).into());
                }
            }
        };

        payload.files.extend(delta_files);
        payload.dedup_files();

        let pb_manifest = pb_types::Manifest {
            files: payload
                .files
                .into_iter()
                .map(|f| f.into())
                .collect::<Vec<_>>(),
        };
        let mut buf = Vec::with_capacity(pb_manifest.encoded_len());
        pb_manifest
            .encode(&mut buf)
            .context("failed to encode manifest")?;
        let put_payload = PutPayload::from_bytes(Bytes::from(buf));

        // 1. Persist the snapshot
        self.store
            .put(&self.snapshot_path, put_payload)
            .await
            .context("Failed to update manifest")?;

        // 2. Delete the merged manifest files
        let mut stream_delete = FuturesUnordered::new();
        for path in paths {
            let store = self.store.clone();
            // TODO: use thread pool to delete sst files
            let handle = tokio::spawn(async move { delete_delta_file(store, path).await });
            stream_delete.push(handle);
        }

        while let Some(res) = stream_delete.next().await {
            match res {
                Err(e) => {
                    error!("Failed to join delete delta task, err:{e}")
                }
                Ok(v) => {
                    if let Err(e) = v {
                        error!("Failed to delete delta, err:{e}")
                    } else {
                        self.deltas_num.fetch_sub(1, Ordering::Relaxed);
                    }
                }
            }
        }

        Ok(())
    }
}

async fn read_delta_file(store: ObjectStoreRef, sst_path: Path) -> Result<SstFile> {
    let bytes = store
        .get(&sst_path)
        .await
        .with_context(|| format!("failed to get delta file, path:{sst_path}"))?
        .bytes()
        .await
        .with_context(|| format!("failed to read delta file, path:{sst_path}"))?;

    let pb_sst = pb_types::SstFile::decode(bytes)
        .with_context(|| format!("failed to decode delta file, path:{sst_path}"))?;

    let sst = SstFile::try_from(pb_sst)
        .with_context(|| format!("failed to convert delta file, path:{sst_path}"))?;
    Ok(sst)
}

async fn delete_delta_file(store: ObjectStoreRef, path: Path) -> Result<()> {
    store
        .delete(&path)
        .await
        .with_context(|| format!("Failed to delete delta files, path:{path}"))?;

    Ok(())
}
