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

use anyhow::Context;
use bytes::Bytes;
use object_store::{path::Path, PutPayload};
use prost::Message;
use tokio::sync::RwLock;

use crate::{
    sst::{FileId, FileMeta, SstFile},
    types::ObjectStoreRef,
    AnyhowError, Error, Result,
};

pub const PREFIX_PATH: &str = "manifest";
pub const SNAPSHOT_FILENAME: &str = "snapshot";

pub struct Manifest {
    path: String,
    snapshot_path: Path,
    store: ObjectStoreRef,

    payload: RwLock<Payload>,
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

impl Manifest {
    pub async fn try_new(path: String, store: ObjectStoreRef) -> Result<Self> {
        let snapshot_path = Path::from(format!("{path}/{SNAPSHOT_FILENAME}"));
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
            store,
            payload: RwLock::new(payload),
        })
    }

    // TODO: Now this functions is poorly implemented, we concat new_sst to
    // snapshot, and upload it back in a whole.
    // In more efficient way, we can create a new diff file, and do compaction in
    // background to merge them to `snapshot`.
    pub async fn add_file(&self, id: FileId, meta: FileMeta) -> Result<()> {
        let mut payload = self.payload.write().await;
        let mut tmp_ssts = payload.files.clone();
        let new_sst = SstFile { id, meta };
        tmp_ssts.push(new_sst.clone());
        let pb_manifest = pb_types::Manifest {
            files: tmp_ssts
                .into_iter()
                .map(|f| pb_types::SstFile {
                    id: f.id,
                    meta: Some(pb_types::SstMeta {
                        max_sequence: f.meta.max_sequence,
                        num_rows: f.meta.num_rows,
                        time_range: Some(pb_types::TimeRange {
                            start: f.meta.time_range.start,
                            end: f.meta.time_range.end,
                        }),
                    }),
                })
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

        // 2. Update cached payload
        payload.files.push(new_sst);

        Ok(())
    }
}
