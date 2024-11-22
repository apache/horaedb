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
use async_scoped::TokioScope;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::{path::Path, PutPayload};
use prost::Message;
use tokio::{
    runtime::Runtime,
    sync::{
        mpsc::{self, Receiver, Sender},
        RwLock,
    },
};
use tracing::error;

use crate::{
    sst::{FileId, FileMeta, SstFile},
    types::{ManifestMergeOptions, ObjectStoreRef, TimeRange},
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
        runtime: Arc<Runtime>,
        merge_options: ManifestMergeOptions,
    ) -> Result<Self> {
        let snapshot_path = Path::from(format!("{root_dir}/{SNAPSHOT_FILENAME}"));
        let delta_dir = Path::from(format!("{root_dir}/{DELTA_PREFIX}"));

        let merger = ManifestMerger::try_new(
            snapshot_path.clone(),
            delta_dir.clone(),
            store.clone(),
            runtime.clone(),
            merge_options,
        )
        .await?;

        {
            let merger = merger.clone();
            // Start merger in background
            runtime.spawn(async move {
                merger.run().await;
            });
        }

        let payload = read_snapshot(&store, &snapshot_path).await?;

        Ok(Self {
            delta_dir,
            store,
            merger,
            payload: RwLock::new(payload),
        })
    }

    pub async fn add_file(&self, id: FileId, meta: FileMeta) -> Result<()> {
        self.merger.maybe_schedule_merge().await?;

        let new_sst_path = Path::from(format!("{}/{id}", self.delta_dir));
        let new_sst = SstFile { id, meta };

        let new_sst_payload = pb_types::SstFile::from(new_sst.clone());
        let mut buf: Vec<u8> = Vec::with_capacity(new_sst_payload.encoded_len());
        new_sst_payload
            .encode(&mut buf)
            .context("failed to encode manifest file")?;
        let put_payload = PutPayload::from_bytes(Bytes::from(buf));

        // 1. Persist the delta manifest
        self.store
            .put(&new_sst_path, put_payload)
            .await
            .with_context(|| format!("Failed to write delta manifest, path:{}", new_sst_path))?;

        // 2. Update cached payload
        {
            let mut payload = self.payload.write().await;
            payload.files.push(new_sst);
        }

        // 3. Update delta files num
        self.merger.add_delta_num();

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

struct ManifestMerger {
    snapshot_path: Path,
    delta_dir: Path,
    store: ObjectStoreRef,
    runtime: Arc<Runtime>,
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
        runtime: Arc<Runtime>,
        merge_options: ManifestMergeOptions,
    ) -> Result<Arc<Self>> {
        let (tx, rx) = mpsc::channel(merge_options.channel_size);
        let merger = Self {
            snapshot_path,
            delta_dir,
            store,
            runtime,
            sender: tx,
            receiver: RwLock::new(rx),
            deltas_num: AtomicUsize::new(0),
            merge_options,
        };
        // Merge all delta files when startup
        merger.do_merge().await?;

        Ok(Arc::new(merger))
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
                    if self.deltas_num.load(Ordering::Relaxed) > self.merge_options.min_merge_threshold {
                        if let Err(err) = self.do_merge().await {
                            error!("Failed to merge delta, err:{err}");
                        }
                    }
                }
            }
        }
    }

    fn schedule_merge(&self, task: MergeType) {
        if let Err(err) = self.sender.try_send(task) {
            error!("Failed to send merge task, err:{err}");
        }
    }

    async fn maybe_schedule_merge(&self) -> Result<()> {
        let current_num = self.deltas_num.load(Ordering::Relaxed);

        if current_num > self.merge_options.hard_merge_threshold {
            self.schedule_merge(MergeType::Hard);
            return Err(AnyhowError::msg(format!(
                "Manifest has too many delta files, value:{current_num}"
            ))
            .into());
        } else if current_num > self.merge_options.soft_merge_threshold {
            self.schedule_merge(MergeType::Soft);
        }

        Ok(())
    }

    fn add_delta_num(&self) {
        self.deltas_num.fetch_add(1, Ordering::Relaxed);
    }

    async fn do_merge(&self) -> Result<()> {
        let paths = self
            .store
            .list(Some(&self.delta_dir))
            .map(|value| {
                value
                    .map(|v| v.location)
                    .with_context(|| format!("Failed to list delta files, path:{}", self.delta_dir))
            })
            .try_collect::<Vec<_>>()
            .await?;
        if paths.is_empty() {
            return Ok(());
        }

        let (_, results) = TokioScope::scope_and_block(|scope| {
            for path in &paths {
                scope.spawn(async move { read_delta_file(&self.store, path).await });
            }
        });

        let mut delta_files = Vec::with_capacity(results.len());
        for res in results {
            let sst_file = res.context("Failed to join read delta files task")??;
            delta_files.push(sst_file);
        }

        let mut payload = read_snapshot(&self.store, &self.snapshot_path).await?;
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
            .with_context(|| format!("Failed to update manifest, path:{}", self.snapshot_path))?;

        // 2. Delete the merged manifest files
        let (_, results) = TokioScope::scope_and_block(|scope| {
            for path in &paths {
                scope.spawn(async move { delete_delta_file(&self.store, path).await });
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
                        self.deltas_num.fetch_sub(1, Ordering::Relaxed);
                    }
                }
            }
        }

        Ok(())
    }
}

async fn read_snapshot(store: &ObjectStoreRef, path: &Path) -> Result<Payload> {
    match store.get(path).await {
        Ok(v) => {
            let bytes = v
                .bytes()
                .await
                .with_context(|| format!("Failed to read manifest snapshot, path:{path}"))?;
            let pb_payload = pb_types::Manifest::decode(bytes)
                .with_context(|| format!("Failed to decode manifest snapshot, path:{path}"))?;
            Payload::try_from(pb_payload)
        }
        Err(err) => {
            if err.to_string().contains("not found") {
                Ok(Payload { files: vec![] })
            } else {
                let context = format!("Failed to read manifest snapshot, path:{path}");
                Err(AnyhowError::new(err).context(context).into())
            }
        }
    }
}

async fn read_delta_file(store: &ObjectStoreRef, sst_path: &Path) -> Result<SstFile> {
    let bytes = store
        .get(sst_path)
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

async fn delete_delta_file(store: &ObjectStoreRef, path: &Path) -> Result<()> {
    store
        .delete(path)
        .await
        .with_context(|| format!("Failed to delete delta files, path:{path}"))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread::sleep};

    use object_store::local::LocalFileSystem;

    use super::*;

    #[tokio::test]
    async fn test_find_manifest() {
        let root_dir = temp_dir::TempDir::new().unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let store = Arc::new(LocalFileSystem::new());

        let manifest = Manifest::try_new(
            root_dir.path().to_string_lossy().to_string(),
            store,
            Arc::new(runtime),
            ManifestMergeOptions::default(),
        )
        .await
        .unwrap();

        for i in 0..20 {
            let time_range = TimeRange::new(i.into(), (i + 1).into());
            let meta = FileMeta {
                max_sequence: i as u64,
                num_rows: i as u32,
                size: i as u32,
                time_range,
            };
            manifest.add_file(i as u64, meta).await.unwrap();
        }

        let find_range = TimeRange::new(10.into(), 15.into());
        let mut ssts = manifest.find_ssts(&find_range).await;

        let mut expected_ssts = (10..15)
            .map(|i| {
                let id = i as u64;
                let time_range = TimeRange::new(i.into(), (i + 1).into());
                let meta = FileMeta {
                    max_sequence: i as u64,
                    num_rows: i as u32,
                    size: i as u32,
                    time_range,
                };
                SstFile { id, meta }
            })
            .collect::<Vec<_>>();

        expected_ssts.sort_by(|a, b| a.id.cmp(&b.id));
        ssts.sort_by(|a, b| a.id.cmp(&b.id));
        assert_eq!(expected_ssts, ssts);
    }

    #[tokio::test]
    async fn test_merge_manifest() {
        let root_dir = temp_dir::TempDir::new()
            .unwrap()
            .path()
            .to_string_lossy()
            .to_string();
        let snapshot_path = Path::from(format!("{root_dir}/{SNAPSHOT_FILENAME}"));
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let store: ObjectStoreRef = Arc::new(LocalFileSystem::new());

        let manifest = Manifest::try_new(
            root_dir,
            store.clone(),
            Arc::new(runtime),
            ManifestMergeOptions {
                merge_interval_seconds: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        // Add manifest files
        for i in 0..20 {
            let time_range = TimeRange::new(i.into(), (i + 1).into());
            let meta = FileMeta {
                max_sequence: i as u64,
                num_rows: i as u32,
                size: i as u32,
                time_range,
            };
            manifest.add_file(i as u64, meta).await.unwrap();
        }

        // Wait for merge manifest to finish
        sleep(Duration::from_secs(1));

        let mut mem_ssts = manifest.payload.read().await.files.clone();
        let mut ssts = read_snapshot(&store, &snapshot_path).await.unwrap().files;

        mem_ssts.sort_by(|a, b| a.id.cmp(&b.id));
        ssts.sort_by(|a, b| a.id.cmp(&b.id));
        assert_eq!(mem_ssts, ssts);
    }
}
