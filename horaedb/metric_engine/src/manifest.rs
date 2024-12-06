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
    io::Write,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::{anyhow, Context};
use async_scoped::TokioScope;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::{path::Path, PutPayload};
use parquet::data_type::AsBytes;
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
    types::{ManifestMergeOptions, ObjectStoreRef, RuntimeRef, TimeRange, Timestamp},
    AnyhowError, Error, Result,
};

pub const PREFIX_PATH: &str = "manifest";
pub const SNAPSHOT_FILENAME: &str = "snapshot";
pub const DELTA_PREFIX: &str = "delta";

pub type ManifestRef = Arc<Manifest>;

pub struct Manifest {
    delta_dir: Path,
    store: ObjectStoreRef,
    merger: Arc<ManifestMerger>,

    payload: RwLock<Payload>,
}

#[derive(Default)]
pub struct Payload {
    files: Vec<SstFile>,
}

impl Payload {
    // TODO: we could sort sst files by name asc, then the dedup will be more
    // efficient
    pub fn dedup_files(&mut self) {
        let mut seen = HashSet::with_capacity(self.files.len());
        self.files.retain(|file| seen.insert(file.id()));
    }
}

impl TryFrom<Bytes> for Payload {
    type Error = Error;

    fn try_from(value: Bytes) -> Result<Self> {
        if value.is_empty() {
            return Ok(Self::default());
        } else {
            let snapshot = Snapshot::try_from_bytes(value)?;
            let files = snapshot.to_sstfiles()?;
            Ok(Self { files })
        }
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
        let snapshot_path = Path::from(format!("{root_dir}/{PREFIX_PATH}/{SNAPSHOT_FILENAME}"));
        let delta_dir = Path::from(format!("{root_dir}/{PREFIX_PATH}/{DELTA_PREFIX}"));

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

        let bytes = read_object(&store, &snapshot_path).await?;
        // TODO: add upgrade logic
        let payload = bytes.try_into()?;

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
        let new_sst = SstFile::new(id, meta);

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

    // TODO: avoid clone
    pub async fn all_ssts(&self) -> Vec<SstFile> {
        let payload = self.payload.read().await;
        payload.files.clone()
    }

    pub async fn find_ssts(&self, time_range: &TimeRange) -> Vec<SstFile> {
        let payload = self.payload.read().await;

        payload
            .files
            .iter()
            .filter(move |f| f.meta().time_range.overlaps(time_range))
            .cloned()
            .collect()
    }
}

#[repr(packed)]
#[derive(Debug)]
struct SnapshotHeader {
    pub magic: u32,
    pub version: u8,
    pub flag: u8,
    pub length: u64,
}

impl SnapshotHeader {
    // use #[repr(packed)] to force unalignment and avoid hard code internal types
    // here, better solutions?
    pub const LENGTH: usize = size_of::<SnapshotHeader>();
    pub const MAGIC: u32 = 0x32A489BF;

    pub fn new(length: u64) -> Self {
        Self {
            magic: SnapshotHeader::MAGIC,
            version: SnapshotRecordV1::VERSION,
            flag: 0,
            length,
        }
    }

    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < Self::LENGTH {
            return Err(anyhow!("invalid bytes, length: {}", bytes.len()).into());
        } else {
            let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
            let version = bytes[5];
            let flag = bytes[6];
            let length = u64::from_le_bytes(bytes[6..14].try_into().unwrap());
            Ok(Self {
                magic,
                version,
                flag,
                length,
            })
        }
    }

    pub fn write_to(&self, mut writter: &mut &mut [u8]) -> Result<()> {
        if writter.len() < SnapshotHeader::LENGTH {
            return Err(anyhow!(
                "writter buf is too small for writing the header, length: {}",
                writter.len()
            )
            .into());
        }
        let magic = self.magic;
        let binding = self.magic.to_le_bytes();
        let xx = binding.as_slice();
        writter.write(self.magic.to_le_bytes().as_slice()).unwrap();
        writter
            .write(self.version.to_le_bytes().as_slice())
            .unwrap();
        writter.write(self.flag.to_le_bytes().as_slice()).unwrap();
        writter.write(self.length.to_le_bytes().as_slice()).unwrap();
        Ok(())
    }
}

#[repr(packed)]
#[derive(Debug)]
struct SnapshotRecordV1 {
    id: u64,
    time_range_start: i64,
    time_range_end: i64,
    size: u32,
    num_rows: u32,
}

impl SnapshotRecordV1 {
    const LENGTH: usize = size_of::<SnapshotRecordV1>();
    pub const VERSION: u8 = 1;

    pub fn write_to(&self, mut writter: &mut &mut [u8]) -> Result<()> {
        if writter.len() < SnapshotRecordV1::LENGTH {
            return Err(anyhow!(
                "writter buf is too small for writing the record, length: {}",
                writter.len()
            )
            .into());
        }
        writter.write(self.id.to_le_bytes().as_slice()).unwrap();
        writter
            .write(self.time_range_start.to_le_bytes().as_slice())
            .unwrap();
        writter
            .write(self.time_range_end.to_le_bytes().as_slice())
            .unwrap();
        writter.write(self.size.to_le_bytes().as_slice()).unwrap();
        writter
            .write(self.num_rows.to_le_bytes().as_slice())
            .unwrap();
        Ok(())
    }

    pub fn id(&self) -> u64 {
        self.id
    }
}

impl From<SstFile> for SnapshotRecordV1 {
    fn from(value: SstFile) -> Self {
        SnapshotRecordV1 {
            id: value.id(),
            time_range_start: value.meta().time_range.start.0,
            time_range_end: value.meta().time_range.end.0,
            size: value.meta().size,
            num_rows: value.meta().num_rows,
        }
    }
}

impl TryFrom<&[u8]> for SnapshotRecordV1 {
    type Error = Error;

    fn try_from(value: &[u8]) -> Result<Self> {
        if value.len() < SnapshotRecordV1::LENGTH {
            return Err(anyhow!("invalid value len: {}", value.len()).into());
        }
        let id = u64::from_le_bytes(value[0..8].try_into().unwrap());
        let time_range_start = i64::from_le_bytes(value[8..16].try_into().unwrap());
        let time_range_end = i64::from_le_bytes(value[16..24].try_into().unwrap());
        let size = u32::from_le_bytes(value[24..28].try_into().unwrap());
        let num_rows = u32::from_le_bytes(value[28..32].try_into().unwrap());
        Ok(SnapshotRecordV1 {
            id,
            time_range_start,
            time_range_end,
            size,
            num_rows,
        })
    }
}

impl Into<SstFile> for SnapshotRecordV1 {
    fn into(self) -> SstFile {
        let file_meta = FileMeta {
            max_sequence: self.id(),
            num_rows: self.num_rows,
            size: self.size,
            time_range: TimeRange::new(
                Timestamp::from(self.time_range_start),
                Timestamp::from(self.time_range_end),
            ),
        };
        SstFile::new(self.id(), file_meta)
    }
}

struct Snapshot {
    header: SnapshotHeader,
    inner: Bytes,
}

impl Default for Snapshot {
    // create an empty Snapshot
    fn default() -> Self {
        let header = SnapshotHeader::new(0);
        Self {
            header,
            inner: Bytes::new(),
        }
    }
}

impl Snapshot {
    pub fn try_from_bytes(bytes: Bytes) -> Result<Self> {
        if bytes.is_empty() {
            return Ok(Snapshot::default());
        }
        let header = SnapshotHeader::try_from_bytes(bytes.as_bytes())?;
        let header_length = header.length as usize;
        if header_length > 0
            && (header_length % SnapshotRecordV1::LENGTH != 0
                || header_length + SnapshotHeader::LENGTH != bytes.len())
        {
            return Err(anyhow!(
                "create snapshot from bytes failed, invalid bytes, header length = {}, total length: {}",
                header_length,
                bytes.len()
            ).into());
        }
        Ok(Self {
            header,
            inner: bytes,
        })
    }

    pub fn to_sstfiles(&self) -> Result<Vec<SstFile>> {
        if self.header.length == 0 {
            return Ok(Vec::new());
        } else {
            let buf = self.inner.as_bytes();
            let mut result: Vec<SstFile> = Vec::new();
            let mut index = SnapshotHeader::LENGTH;
            while index < buf.len() {
                let record =
                    SnapshotRecordV1::try_from(&buf[index..index + SnapshotRecordV1::LENGTH])?;
                index += SnapshotRecordV1::LENGTH;
                result.push(record.into());
            }

            Ok(result)
        }
    }

    pub fn dedup_sstfiles(&self, sstfiles: &mut Vec<SstFile>) -> Result<()> {
        let buf = self.inner.as_bytes();
        let mut ids = HashSet::new();
        let mut index = SnapshotHeader::LENGTH;
        while index < buf.len() {
            let record = SnapshotRecordV1::try_from(&buf[index..index + SnapshotRecordV1::LENGTH])?;
            index += SnapshotRecordV1::LENGTH;
            ids.insert(record.id());
        }
        sstfiles.retain(|item| !ids.contains(&item.id()));

        Ok(())
    }

    pub fn merge_sstfiles(&mut self, sstfiles: Vec<SstFile>) {
        // update header
        self.header.length += (sstfiles.len() * SnapshotRecordV1::LENGTH) as u64;
        // final snapshot
        let mut snapshot = vec![1u8; SnapshotHeader::LENGTH + self.header.length as usize];
        let mut writter = snapshot.as_mut_slice();

        // write new head
        self.header.write_to(&mut writter).unwrap();
        // write old records
        if !self.inner.is_empty() {
            writter
                .write(&self.inner.as_bytes()[SnapshotHeader::LENGTH..])
                .unwrap();
        }
        // write new records
        for sst in sstfiles {
            let record: SnapshotRecordV1 = sst.into();
            let written = record.write_to(&mut writter).unwrap();
        }
        self.inner = Bytes::from(snapshot);
    }

    pub fn into_bytes(self) -> Bytes {
        self.inner
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
    runtime: RuntimeRef,
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
        let paths = list_delta_paths(&self.store, &self.delta_dir).await?;
        if paths.is_empty() {
            return Ok(());
        }

        let (_, results) = TokioScope::scope_and_block(|scope| {
            for path in &paths {
                scope.spawn(async { read_delta_file(&self.store, path).await });
            }
        });

        let mut delta_files = Vec::with_capacity(results.len());
        for res in results {
            let sst_file = res.context("Failed to join read delta files task")??;
            delta_files.push(sst_file);
        }
        let snapshot_bytes = read_object(&self.store, &self.snapshot_path).await?;
        let mut snapshot = Snapshot::try_from_bytes(snapshot_bytes)?;
        // TODO: no need to dedup every time.
        snapshot.dedup_sstfiles(&mut delta_files)?;
        snapshot.merge_sstfiles(delta_files);
        let snapshot_bytes = snapshot.into_bytes();
        let put_payload = PutPayload::from_bytes(snapshot_bytes);
        // 1. Persist the snapshot
        self.store
            .put(&self.snapshot_path, put_payload)
            .await
            .with_context(|| format!("Failed to update manifest, path:{}", self.snapshot_path))?;

        // 2. Delete the merged manifest files
        let (_, results) = TokioScope::scope_and_block(|scope| {
            for path in &paths {
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
                        self.deltas_num.fetch_sub(1, Ordering::Relaxed);
                    }
                }
            }
        }

        Ok(())
    }
}

async fn read_object(store: &ObjectStoreRef, path: &Path) -> Result<Bytes> {
    match store.get(path).await {
        Ok(v) => v
            .bytes()
            .await
            .with_context(|| format!("Failed to read manifest snapshot, path:{path}"))
            .map_err(|e| e.into()),
        Err(err) => {
            if err.to_string().contains("not found") {
                Ok(Bytes::new())
            } else {
                let context = format!("Failed to read file, path:{path}");
                Err(AnyhowError::new(err).context(context).into())
            }
        }
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
    }

    #[tokio::test]
    async fn test_merge_manifest() {
        let root_dir = temp_dir::TempDir::new()
            .unwrap()
            .path()
            .to_string_lossy()
            .to_string();
        let snapshot_path = Path::from(format!("{root_dir}/{PREFIX_PATH}/{SNAPSHOT_FILENAME}"));
        let delta_dir = Path::from(format!("{root_dir}/{PREFIX_PATH}/{DELTA_PREFIX}"));
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
        sleep(Duration::from_secs(2));

        let mut mem_ssts = manifest.payload.read().await.files.clone();
        let snapshot = read_object(&store, &snapshot_path).await.unwrap();
        let payload: Payload = snapshot.try_into().unwrap();
        let mut ssts = payload.files;

        mem_ssts.sort_by_key(|a| a.id());
        ssts.sort_by_key(|a| a.id());
        assert_eq!(mem_ssts, ssts);

        let delta_paths = list_delta_paths(&store, &delta_dir).await.unwrap();
        assert!(delta_paths.is_empty());
    }

    #[test]
    fn test_snapshot_header() {
        let header = SnapshotHeader::new(257);
        let mut vec = vec![0u8; SnapshotHeader::LENGTH];
        let mut writter = vec.as_mut_slice();
        header.write_to(&mut writter).unwrap();
        assert!(writter.len() == 0);
        assert!((vec[3], vec[2], vec[1], vec[0]) == (0x32, 0xa4, 0x89, 0xbf)); // magic
        assert!(vec[4] == 1); // version
        assert!(vec[5] == 0); // flag
        assert!(vec[6] == 1); // 1
        assert!(vec[7] == 1); // 1 * 256 = 256
        assert!(vec[8] == 0); // 0
        assert!(vec[9] == 0); // 0
        assert!(vec[10] == 0); // 0
        assert!(vec[11] == 0); // 0
        assert!(vec[12] == 0); // 0
        assert!(vec[13] == 0); // 0

        let mut vec = vec![0u8; SnapshotHeader::LENGTH - 1];
        let mut writter = &mut vec[..];
        let result = header.write_to(&mut writter);
        assert!(result.is_err()); // buf not enough
    }

    #[test]
    fn test_snapshot_record() {
        let sstfile = SstFile::new(
            99,
            FileMeta {
                max_sequence: 99,
                num_rows: 100,
                size: 938,
                time_range: TimeRange::new(1i64.into(), 2i64.into()),
            },
        );
        let record: SnapshotRecordV1 = sstfile.into();
        let mut vec: Vec<u8> = vec![0u8; SnapshotRecordV1::LENGTH];
        let mut writter = vec.as_mut_slice();
        record.write_to(&mut writter).unwrap();

        assert!(writter.len() == 0);
        assert!(vec[0] == 99); // id
        assert!(vec[8] == 1); // start time
        assert!(vec[16] == 2); // end time
        assert!(vec[24] == 170); // size_byte_0 , 170
        assert!(vec[25] == 3); // size_byte_1, 3 * 256
        assert!(vec[28] == 100); // num_rows

        let mut vec = vec![0u8; SnapshotRecordV1::LENGTH - 1];
        let mut writter = vec.as_mut_slice();
        let result = record.write_to(&mut writter);
        assert!(result.is_err()); // buf not enough
    }
}
