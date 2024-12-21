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

use std::io::{Cursor, Read, Write};

use anyhow::Context;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use bytes::{Buf, Bytes};

use crate::{
    ensure,
    sst::{FileId, FileMeta, SstFile},
    types::TimeRange,
    Error, Result,
};

#[derive(Clone, Debug)]
pub struct ManifestUpdate {
    pub to_adds: Vec<SstFile>,
    pub to_deletes: Vec<FileId>,
}

impl ManifestUpdate {
    pub fn new(to_adds: Vec<SstFile>, to_deletes: Vec<FileId>) -> Self {
        Self {
            to_adds,
            to_deletes,
        }
    }
}

impl TryFrom<pb_types::ManifestUpdate> for ManifestUpdate {
    type Error = Error;

    fn try_from(value: pb_types::ManifestUpdate) -> Result<Self> {
        let to_adds = value
            .to_adds
            .into_iter()
            .map(SstFile::try_from)
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            to_adds,
            to_deletes: value.to_deletes,
        })
    }
}

impl From<ManifestUpdate> for pb_types::ManifestUpdate {
    fn from(value: ManifestUpdate) -> Self {
        let to_adds = value
            .to_adds
            .into_iter()
            .map(pb_types::SstFile::from)
            .collect();

        pb_types::ManifestUpdate {
            to_adds,
            to_deletes: value.to_deletes,
        }
    }
}

/// The layout for the header.
/// ```plaintext
/// +-------------+--------------+------------+--------------+
/// | magic(u32)  | version(u8)  | flag(u8)   | length(u32)  |
/// +-------------+--------------+------------+--------------+
/// ```
/// - The Magic field (u32) is used to ensure the validity of the data source.
/// - The Flags field (u8) is reserved for future extensibility, such as
///   enabling compression or supporting additional features.
/// - The length field (u64) represents the total length of the subsequent
///   records and serves as a straightforward method for verifying their
///   integrity. (length = record_length * record_count)
#[derive(Debug, PartialEq, Eq)]
pub struct SnapshotHeader {
    pub magic: u32,
    pub version: u8,
    pub flag: u8,
    pub length: u64,
}

impl SnapshotHeader {
    pub const LENGTH: usize = 4 /*magic*/ + 1 /*version*/ + 1 /*flag*/ + 8 /*length*/;
    pub const MAGIC: u32 = 0xCAFE_1234;

    pub fn new() -> Self {
        Self {
            magic: SnapshotHeader::MAGIC,
            version: SnapshotRecord::VERSION,
            flag: 0,
            length: 0,
        }
    }

    pub fn try_new<R>(mut reader: R) -> Result<Self>
    where
        R: Read,
    {
        let magic = reader
            .read_u32::<LittleEndian>()
            .context("read snapshot header magic")?;
        ensure!(
            magic == SnapshotHeader::MAGIC,
            "invalid bytes to convert to header."
        );
        let version = reader.read_u8().context("read snapshot header version")?;
        let flag = reader.read_u8().context("read snapshot header flag")?;
        let length = reader
            .read_u64::<LittleEndian>()
            .context("read snapshot header length")?;
        Ok(Self {
            magic,
            version,
            flag,
            length,
        })
    }

    pub fn write_to<W>(&self, mut writer: W) -> Result<()>
    where
        W: Write,
    {
        writer
            .write_u32::<LittleEndian>(self.magic)
            .context("write shall not fail.")?;
        writer
            .write_u8(self.version)
            .context("write shall not fail.")?;
        writer
            .write_u8(self.flag)
            .context("write shall not fail.")?;
        writer
            .write_u64::<LittleEndian>(self.length)
            .context("write shall not fail.")?;
        Ok(())
    }
}

/// The layout for manifest Record:
/// ```plaintext
/// +---------+-------------------+------------+-----------------+
/// | id(u64) | time_range(i64*2) | size(u32)  |  num_rows(u32)  |
/// +---------+-------------------+------------+-----------------+
/// ```
#[derive(Debug, PartialEq, Eq)]
pub struct SnapshotRecord {
    id: u64,
    time_range: TimeRange,
    size: u32,
    num_rows: u32,
}

impl SnapshotRecord {
    const LENGTH: usize = 8 /*id*/+ 16 /*time range*/ + 4 /*size*/ + 4 /*num rows*/;
    pub const VERSION: u8 = 1;

    pub fn write_to<W>(&self, mut writer: W) -> Result<()>
    where
        W: Write,
    {
        writer
            .write_u64::<LittleEndian>(self.id)
            .context("write shall not fail.")?;
        writer
            .write_i64::<LittleEndian>(*self.time_range.start)
            .context("write shall not fail.")?;
        writer
            .write_i64::<LittleEndian>(*self.time_range.end)
            .context("write shall not fail.")?;
        writer
            .write_u32::<LittleEndian>(self.size)
            .context("write shall not fail.")?;
        writer
            .write_u32::<LittleEndian>(self.num_rows)
            .context("write shall not fail.")?;
        Ok(())
    }

    pub fn id(&self) -> u64 {
        self.id
    }
}

impl From<SstFile> for SnapshotRecord {
    fn from(value: SstFile) -> Self {
        SnapshotRecord {
            id: value.id(),
            time_range: value.meta().time_range.clone(),
            size: value.meta().size,
            num_rows: value.meta().num_rows,
        }
    }
}

impl SnapshotRecord {
    fn try_new<R>(mut reader: R) -> Result<Self>
    where
        R: Read,
    {
        let id = reader
            .read_u64::<LittleEndian>()
            .context("read record id")?;
        let start = reader
            .read_i64::<LittleEndian>()
            .context("read record start")?;
        let end = reader
            .read_i64::<LittleEndian>()
            .context("read record end")?;
        let size = reader
            .read_u32::<LittleEndian>()
            .context("read record size")?;
        let num_rows = reader
            .read_u32::<LittleEndian>()
            .context("read record num_rows")?;
        Ok(SnapshotRecord {
            id,
            time_range: (start..end).into(),
            size,
            num_rows,
        })
    }
}

impl From<SnapshotRecord> for SstFile {
    fn from(record: SnapshotRecord) -> Self {
        let file_meta = FileMeta {
            max_sequence: record.id(),
            num_rows: record.num_rows,
            size: record.size,
            time_range: record.time_range.clone(),
        };
        SstFile::new(record.id(), file_meta)
    }
}

pub struct Snapshot {
    header: SnapshotHeader,
    pub records: Vec<SnapshotRecord>,
}

impl Default for Snapshot {
    // create an empty Snapshot
    fn default() -> Self {
        let header = SnapshotHeader::new();
        Self {
            header,
            records: Vec::new(),
        }
    }
}

impl TryFrom<Bytes> for Snapshot {
    type Error = Error;

    fn try_from(bytes: Bytes) -> Result<Self> {
        if bytes.is_empty() {
            return Ok(Snapshot::default());
        }
        let bytes_len = bytes.len();
        let mut cursor = Cursor::new(bytes);
        let header = SnapshotHeader::try_new(&mut cursor)?;
        let record_total_length = header.length as usize;
        ensure!(
            record_total_length > 0
                && record_total_length % SnapshotRecord::LENGTH == 0
                && record_total_length + SnapshotHeader::LENGTH == bytes_len,
            "create snapshot from bytes failed, header:{header:?}, bytes_length: {bytes_len}",
        );
        let mut records = Vec::with_capacity(record_total_length / SnapshotRecord::LENGTH);
        while cursor.has_remaining() {
            let record = SnapshotRecord::try_new(&mut cursor)?;
            records.push(record);
        }

        Ok(Self { header, records })
    }
}

impl Snapshot {
    pub fn into_ssts(self) -> Vec<SstFile> {
        if self.header.length == 0 {
            Vec::new()
        } else {
            self.records.into_iter().map(|r| r.into()).collect()
        }
    }

    // TODO: Ensure no files duplicated
    // https://github.com/apache/horaedb/issues/1608
    pub fn add_records(&mut self, ssts: Vec<SstFile>) {
        self.records
            .extend(ssts.into_iter().map(SnapshotRecord::from));
        self.header.length = (self.records.len() * SnapshotRecord::LENGTH) as u64;
    }

    pub fn delete_records(&mut self, to_deletes: Vec<FileId>) {
        // Since this may hurt performance, we only do this in debug mode.
        if cfg!(debug_assertions) {
            for id in &to_deletes {
                assert!(
                    self.records.iter().any(|r| r.id == *id),
                    "File not found in snapshot, id:{id}"
                );
            }
        }

        self.records
            .retain(|record| !to_deletes.contains(&record.id));
        self.header.length = (self.records.len() * SnapshotRecord::LENGTH) as u64;
    }

    pub fn into_bytes(self) -> Result<Bytes> {
        let buf = Vec::with_capacity(self.header.length as usize * SnapshotHeader::LENGTH);
        let mut cursor = Cursor::new(buf);

        self.header.write_to(&mut cursor)?;
        for record in self.records {
            record.write_to(&mut cursor)?;
        }
        Ok(Bytes::from(cursor.into_inner()))
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_snapshot_header() {
        let header = SnapshotHeader::new();
        let mut vec = vec![0u8; SnapshotHeader::LENGTH];
        let mut writer = vec.as_mut_slice();
        header.write_to(&mut writer).unwrap();
        assert!(writer.is_empty());
        let cursor = Cursor::new(vec);
        let header = SnapshotHeader::try_new(cursor).unwrap();

        assert_eq!(
            SnapshotHeader {
                magic: SnapshotHeader::MAGIC,
                version: 1,
                flag: 0,
                length: 0
            },
            header
        );
    }

    #[test]
    fn test_snapshot_record() {
        let sstfile = SstFile::new(
            99,
            FileMeta {
                max_sequence: 99,
                num_rows: 100,
                size: 938,
                time_range: (100..200).into(),
            },
        );
        let record: SnapshotRecord = sstfile.into();
        let mut vec: Vec<u8> = vec![0u8; SnapshotRecord::LENGTH];
        let mut writer = vec.as_mut_slice();
        record.write_to(&mut writer).unwrap();

        assert!(writer.is_empty());
        let cursor = Cursor::new(vec);
        let record = SnapshotRecord::try_new(cursor).unwrap();
        assert_eq!(
            SnapshotRecord {
                id: 99,
                time_range: (100..200).into(),
                size: 938,
                num_rows: 100
            },
            record
        );
    }
}
