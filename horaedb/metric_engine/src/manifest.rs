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
use tokio::{
    sync::{
        mpsc::{self, Receiver, Sender},
        RwLock,
    },
    time::timeout,
};
use tracing::{error, info};

use crate::{
    sst::{FileId, FileMeta, SstFile},
    types::{ObjectStoreRef, TimeRange},
    AnyhowError, Error, Result,
};

pub const PREFIX_PATH: &str = "manifest";
pub const SNAPSHOT_FILENAME: &str = "snapshot";
pub const DELTA_MANIFEST_PREFIX: &str = "delta_manifest";

pub struct Manifest {
    path: String,
    snapshot_path: Path,
    delta_manifest_path: Path,
    store: ObjectStoreRef,
    manifest_merge: Arc<ManifestMerge>,

    payload: RwLock<Payload>,
}

pub struct Payload {
    files: Vec<SstFile>,
}

impl Payload {
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
        path: String,
        store: ObjectStoreRef,
        merge_options: ManifestMergeOptions,
    ) -> Result<Self> {
        let snapshot_path = Path::from(format!("{path}/{SNAPSHOT_FILENAME}"));
        let delta_manifest_path = Path::from(format!("{path}/{DELTA_MANIFEST_PREFIX}"));

        let manifest_merge = ManifestMerge::try_new(
            snapshot_path.clone(),
            delta_manifest_path.clone(),
            store.clone(),
            merge_options,
        )
        .await?;
        let manifest_merge_clone = manifest_merge.clone();

        // Start merge manifest task
        tokio::spawn(async move {
            manifest_merge_clone.run().await;
        });

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
            path,
            snapshot_path,
            delta_manifest_path,
            store,
            manifest_merge,
            payload: RwLock::new(payload),
        })
    }

    pub async fn add_file(&self, id: FileId, meta: FileMeta) -> Result<()> {
        // Schedule force merge manifest
        self.manifest_merge.schedule_force_merge().await?;

        let new_sst_path = Path::from(format!("{}/{id}", self.delta_manifest_path));
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
            .context("Failed to update manifest file")?;

        // 2. Update cached payload
        let mut payload = self.payload.write().await;
        payload.files.push(new_sst);

        // 3. Schedule manifest merge
        self.manifest_merge.schedule_merge().await;

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

enum ManifestMergeTask {
    ForceMergeManifest,
    MergeManifest,
}

#[derive(Clone)]
pub struct ManifestMergeOptions {
    channel_size: usize,
    merge_interval_seconds: usize,
    merge_threshold: usize,
    force_merge_threshold: usize,
}

impl Default for ManifestMergeOptions {
    fn default() -> Self {
        Self {
            channel_size: 100,
            merge_interval_seconds: 5,
            merge_threshold: 20,
            force_merge_threshold: 50,
        }
    }
}

struct ManifestMerge {
    snapshot_path: Path,
    delta_manifest_path: Path,
    store: ObjectStoreRef,
    sender: Sender<ManifestMergeTask>,
    receiver: RwLock<Receiver<ManifestMergeTask>>,
    sst_num: AtomicUsize,
    merge_options: ManifestMergeOptions,
}

impl ManifestMerge {
    async fn try_new(
        snapshot_path: Path,
        delta_manifest_path: Path,
        store: ObjectStoreRef,
        merge_options: ManifestMergeOptions,
    ) -> Result<Arc<Self>> {
        let (tx, rx) = mpsc::channel(merge_options.channel_size);
        let manifest_merge = Self {
            snapshot_path,
            delta_manifest_path,
            store,
            sender: tx,
            receiver: RwLock::new(rx),
            sst_num: AtomicUsize::new(0),
            merge_options,
        };
        // Merge all manifest files when start
        manifest_merge.merge_manifest().await?;

        Ok(Arc::new(manifest_merge))
    }

    async fn run(&self) {
        let merge_interval = Duration::from_secs(self.merge_options.merge_interval_seconds as u64);
        let mut receiver = self.receiver.write().await;
        loop {
            match timeout(merge_interval, receiver.recv()).await {
                Ok(Some(ManifestMergeTask::ForceMergeManifest)) => {
                    if self.sst_num.load(Ordering::Relaxed)
                        < self.merge_options.force_merge_threshold
                    {
                        continue;
                    }
                    match self.merge_manifest().await {
                        Ok(_) => {
                            self.sst_num.store(0, Ordering::Relaxed);
                        }
                        Err(err) => {
                            error!("Failed to force merge manifest, err: {:?}", err);
                        }
                    }
                }
                Ok(Some(ManifestMergeTask::MergeManifest)) => {
                    if self.sst_num.load(Ordering::Relaxed) < self.merge_options.merge_threshold {
                        continue;
                    }
                    match self.merge_manifest().await {
                        Ok(_) => {
                            self.sst_num.store(0, Ordering::Relaxed);
                        }
                        Err(err) => {
                            error!("Failed to merge manifest, err: {:?}", err);
                        }
                    }
                }
                Ok(None) => {
                    // The channel is disconnected.
                    info!("Channel disconnected, merge manifest task exit");
                    break;
                }
                Err(_) => {
                    info!("Schedule merge manifest task start");
                    match self.merge_manifest().await {
                        Ok(_) => {
                            self.sst_num.store(0, Ordering::Relaxed);
                        }
                        Err(err) => {
                            error!("Failed to merge manifest, err: {:?}", err);
                        }
                    }
                    info!("Schedule merge manifest task end");
                }
            }
        }
    }

    async fn sender(&self, task: ManifestMergeTask) {
        if let Err(err) = self.sender.send(task).await {
            error!("Failed to send merge manifest task, err: {:?}", err);
        }
    }

    async fn schedule_force_merge(&self) -> Result<()> {
        if self.sst_num.load(Ordering::Relaxed) >= self.merge_options.force_merge_threshold {
            self.sender(ManifestMergeTask::ForceMergeManifest).await;
            return Err(AnyhowError::msg("manifest files reach force merge threshold").into());
        }
        Ok(())
    }

    async fn schedule_merge(&self) {
        self.sst_num.fetch_add(1, Ordering::Relaxed);
        if self.sst_num.load(Ordering::Relaxed) >= self.merge_options.merge_threshold {
            self.sender(ManifestMergeTask::MergeManifest).await;
        }
    }

    async fn merge_manifest(&self) -> Result<()> {
        let paths = self
            .store
            .list(Some(&self.delta_manifest_path))
            .map(|value| value.map(|v| v.location).context("failed to get path"))
            .try_collect::<Vec<_>>()
            .await?;
        if paths.is_empty() {
            return Ok(());
        }

        let mut stream_read = FuturesUnordered::new();
        for path in paths.clone() {
            let store = self.store.clone();
            // TODO: use thread pool to read manifest files
            let handle = tokio::spawn(async move { read_manifest(store, path).await });
            stream_read.push(handle);
        }
        let mut sst_files = Vec::with_capacity(stream_read.len());
        while let Some(res) = stream_read.next().await {
            let sst_file = res.context("failed to read manifest files")??;
            sst_files.push(sst_file);
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

        payload.files.extend(sst_files);
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
            let handle = tokio::spawn(async move { delete_manifest(store, path).await });
            stream_delete.push(handle);
        }
        while let Some(res) = stream_delete.next().await {
            res.context("failed to delete old manifest file")??;
        }

        Ok(())
    }
}

async fn read_manifest(store: ObjectStoreRef, sst_path: Path) -> Result<SstFile> {
    let bytes = store
        .get(&sst_path)
        .await
        .context("failed to read manifest file")?
        .bytes()
        .await
        .context("failed to read manifest file")?;
    let pb_sst = pb_types::SstFile::decode(bytes).context("failed to decode manifest file")?;
    let sst = SstFile::try_from(pb_sst)?;
    Ok(sst)
}

async fn delete_manifest(store: ObjectStoreRef, sst_path: Path) -> Result<()> {
    store.delete(&sst_path).await.context(format!(
        "Failed to delete manifest files, path:{}",
        sst_path
    ))?;
    Ok(())
}
