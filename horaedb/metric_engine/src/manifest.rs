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

use std::sync::Arc;

use anyhow::Context;
use bytes::Bytes;
use futures::StreamExt;
use log::{error, info};
use object_store::{path::Path, PutPayload};
use prost::Message;
use tokio::{
    sync::{
        mpsc::{self, Receiver, Sender},
        RwLock,
    },
    time,
};

use crate::{
    sst::{FileId, FileMeta, SstFile},
    types::{ObjectStoreRef, TimeRange},
    AnyhowError, Error, Result,
};

pub const PREFIX_PATH: &str = "manifest";
pub const SNAPSHOT_FILENAME: &str = "snapshot";
pub const SST_PREFIX: &str = "sst";

pub struct Manifest {
    path: String,
    snapshot_path: Path,
    sst_path: Path,
    store: ObjectStoreRef,
    sender: Sender<SstsMergeTask>,

    payload: Arc<RwLock<Payload>>,
}

pub struct Payload {
    files: Vec<SstFile>,
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
        merge_options: SstsMergeOptions,
    ) -> Result<Self> {
        let snapshot_path = Path::from(format!("{path}/{SNAPSHOT_FILENAME}"));
        let sst_path = Path::from(format!("{path}/{SST_PREFIX}"));

        let (tx, rx) = mpsc::channel(merge_options.channel_size);
        let mut ssts_merge = SstsMerge::try_new(
            snapshot_path.clone(),
            sst_path.clone(),
            store.clone(),
            rx,
            merge_options,
        )
        .await?;

        // Start merge ssts task
        tokio::spawn(async move {
            ssts_merge.run().await;
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
        let payload = Arc::new(RwLock::new(payload));

        Ok(Self {
            path,
            snapshot_path,
            sst_path,
            store,
            payload,
            sender: tx,
        })
    }

    pub async fn add_file(&self, id: FileId, meta: FileMeta) -> Result<()> {
        let new_sst_path = Path::from(format!("{}/{id}", self.sst_path));
        let new_sst = SstFile { id, meta };

        let new_sst_payload = pb_types::SstFile::from(new_sst.clone());
        let mut buf: Vec<u8> = Vec::with_capacity(new_sst_payload.encoded_len());
        new_sst_payload
            .encode(&mut buf)
            .context("failed to encode manifest")?;
        let put_payload = PutPayload::from_bytes(Bytes::from(buf));

        // 1. Persist the sst file
        self.store
            .put(&new_sst_path, put_payload)
            .await
            .context("Failed to update manifest")?;

        // 2. Update cached payload
        let mut payload = self.payload.write().await;
        payload.files.push(new_sst);

        // 3. Schedule ssts merge
        self.sender(SstsMergeTask::MergeSsts(1)).await;

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

    async fn sender(&self, task: SstsMergeTask) {
        if let Err(err) = self.sender.send(task).await {
            error!("Failed to send merge ssts task, err: {:?}", err);
        }
    }
}

enum SstsMergeTask {
    ForceMergeSsts,
    MergeSsts(usize),
}

pub struct SstsMergeOptions {
    channel_size: usize,
    max_interval_seconds: usize,
    merge_threshold: usize,
}

impl Default for SstsMergeOptions {
    fn default() -> Self {
        Self {
            channel_size: 100,
            max_interval_seconds: 5,
            merge_threshold: 50,
        }
    }
}

struct SstsMerge {
    snapshot_path: Path,
    sst_path: Path,
    store: ObjectStoreRef,
    receiver: Receiver<SstsMergeTask>,
    sst_num: usize,
    merge_options: SstsMergeOptions,
}

impl SstsMerge {
    async fn try_new(
        snapshot_path: Path,
        sst_path: Path,
        store: ObjectStoreRef,
        rx: Receiver<SstsMergeTask>,
        merge_options: SstsMergeOptions,
    ) -> Result<Self> {
        let ssts_merge = Self {
            snapshot_path,
            sst_path,
            store,
            receiver: rx,
            sst_num: 0,
            merge_options,
        };
        // Merge all ssts when start
        ssts_merge.merge_ssts().await?;

        Ok(ssts_merge)
    }

    async fn run(&mut self) {
        let interval = time::Duration::from_secs(self.merge_options.max_interval_seconds as u64);
        let threshold = self.merge_options.merge_threshold;

        loop {
            match time::timeout(interval, self.receiver.recv()).await {
                Ok(Some(SstsMergeTask::ForceMergeSsts)) => match self.merge_ssts().await {
                    Ok(_) => {
                        self.sst_num = 0;
                    }
                    Err(err) => {
                        error!("Failed to force merge ssts, err: {:?}", err);
                    }
                },
                Ok(Some(SstsMergeTask::MergeSsts(num))) => {
                    self.sst_num += num;
                    if self.sst_num >= threshold {
                        match self.merge_ssts().await {
                            Ok(_) => {
                                self.sst_num = 0;
                            }
                            Err(err) => {
                                error!("Failed to merge ssts, err: {:?}", err);
                            }
                        }
                    }
                }
                Ok(None) => {
                    // The channel is disconnected.
                    info!("Channel disconnected, merge ssts task exit");
                    break;
                }
                Err(_) => {
                    info!("Timeout receive merge ssts task");
                }
            }
        }
    }

    async fn merge_ssts(&self) -> Result<()> {
        let meta_infos = self
            .store
            .list(Some(&self.sst_path))
            .collect::<Vec<_>>()
            .await;
        if meta_infos.is_empty() {
            return Ok(());
        }

        let mut paths = Vec::with_capacity(meta_infos.len());
        for meta_info in meta_infos {
            let path = meta_info
                .context("failed to get path of manifest sst")?
                .location;
            paths.push(path);
        }

        let mut sst_files = Vec::with_capacity(paths.len());
        for path in &paths {
            let bytes = self
                .store
                .get(path)
                .await
                .context("failed to read sst file")?
                .bytes()
                .await
                .context("failed to read sst file")?;
            let pb_sst = pb_types::SstFile::decode(bytes).context("failed to decode sst file")?;
            sst_files.push(SstFile::try_from(pb_sst)?);
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

        payload.files.extend(sst_files.clone());
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

        // 2. Delete the old sst files
        for path in &paths {
            self.store
                .delete(path)
                .await
                .context("failed to delete sst file")?;
        }

        Ok(())
    }
}
