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
    collections::{HashMap, VecDeque},
    fmt::Debug,
    fs,
    fs::{File, OpenOptions},
    io,
    io::Write,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use bytes_ext::{BufMut, BytesMut};
use common_types::{table::TableId, SequenceNumber, MAX_SEQUENCE_NUMBER, MIN_SEQUENCE_NUMBER};
use crc32fast::Hasher;
use generic_error::{BoxError, GenericError};
use macros::define_result;
use memmap2::{MmapMut, MmapOptions};
use runtime::Runtime;
use snafu::{ensure, Backtrace, ResultExt, Snafu};

use crate::{
    kv_encoder::{CommonLogEncoding, CommonLogKey},
    log_batch::{LogEntry, LogWriteBatch},
    manager::{
        BatchLogIteratorAdapter, Read, ReadContext, ReadRequest, RegionId, ScanContext,
        ScanRequest, SyncLogIterator, WalLocation, WriteContext,
    },
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to open or create file: {}", source))]
    FileOpen { source: io::Error },

    #[snafu(display("Failed to map file to memory: {}", source))]
    Mmap { source: io::Error },

    #[snafu(display("Segment full"))]
    SegmentFull,

    #[snafu(display("Failed to append data to segment file: {}", source))]
    SegmentAppend { source: io::Error },

    #[snafu(display("Failed to flush mmap: {}", source))]
    Flush { source: io::Error },

    #[snafu(display(
        "Attempted to read beyond segment size. Offset: {}, Size: {}, FileSize: {}",
        offset,
        size,
        file_size
    ))]
    ReadOutOfBounds {
        offset: u64,
        size: u64,
        file_size: u64,
    },

    #[snafu(display("Invalid segment header"))]
    InvalidHeader,

    #[snafu(display("Segment not open, id:{}", id))]
    SegmentNotOpen { id: u64 },

    #[snafu(display("Segment not found, id:{}", id))]
    SegmentNotFound { id: u64 },

    #[snafu(display("Unable to convert slice: {}", source))]
    Conversion {
        source: std::array::TryFromSliceError,
    },

    #[snafu(display("{}", source))]
    Encoding { source: GenericError },

    #[snafu(display("Invalid record: {}, backtrace:\n{}", source, backtrace))]
    InvalidRecord {
        source: GenericError,
        backtrace: Backtrace,
    },

    #[snafu(display("Length mismatch: expected {} but found {}", expected, actual))]
    LengthMismatch { expected: usize, actual: usize },

    #[snafu(display("Checksum mismatch: expected {}, but got {}", expected, actual))]
    ChecksumMismatch { expected: u32, actual: u32 },
}

define_result!(Error);

const HEADER: &str = "HoraeDB WAL";
const CRC_SIZE: usize = 4;
const RECORD_LENGTH_SIZE: usize = 4;
const KEY_LENGTH_SIZE: usize = 2;
const VALUE_LENGTH_SIZE: usize = 4;
// todo: make MAX_FILE_SIZE configurable
const MAX_FILE_SIZE: u64 = 64 * 1024 * 1024;

#[derive(Debug)]
pub struct Segment {
    path: String,
    id: u64,
    size: u64,
    min_seq: SequenceNumber,
    max_seq: SequenceNumber,
    is_open: bool,
    file: Option<File>,
    mmap: Option<MmapMut>,
    record_position: Option<Vec<Position>>,
}

#[derive(Debug, Clone)]
pub struct Position {
    start: u64,
    end: u64,
}

impl Segment {
    pub fn new(path: String, segment_id: u64) -> Result<Segment> {
        if !Path::new(&path).exists() {
            let mut file = File::create(&path).context(FileOpen)?;
            file.write_all(HEADER.as_bytes()).context(FileOpen)?;
        }
        Ok(Segment {
            path,
            id: segment_id,
            size: HEADER.len() as u64,
            is_open: false,
            min_seq: MAX_SEQUENCE_NUMBER,
            max_seq: MIN_SEQUENCE_NUMBER,
            file: None,
            mmap: None,
            record_position: None,
        })
    }

    pub fn open(&mut self) -> Result<()> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&self.path)
            .context(FileOpen)?;

        let metadata = file.metadata().context(FileOpen)?;
        let size = metadata.len();

        let mmap = unsafe { MmapOptions::new().map_mut(&file).context(Mmap)? };

        // Validate segment header
        let header_len = HEADER.len();
        ensure!(size >= header_len as u64, InvalidHeader);

        let header_bytes = &mmap[0..header_len];
        let header_str = std::str::from_utf8(header_bytes).map_err(|_| Error::InvalidHeader)?;

        ensure!(header_str == HEADER, InvalidHeader);

        // Read and validate all records
        let mut pos = header_len;
        let mut record_position = Vec::new();

        while pos < size as usize {
            ensure!(
                pos + CRC_SIZE + RECORD_LENGTH_SIZE <= size as usize,
                LengthMismatch {
                    expected: pos + CRC_SIZE + RECORD_LENGTH_SIZE,
                    actual: size as usize
                }
            );

            // Read the CRC
            let crc = u32::from_le_bytes(mmap[pos..pos + CRC_SIZE].try_into().context(Conversion)?);
            pos += CRC_SIZE;

            // Read the length
            let length = u32::from_le_bytes(
                mmap[pos..pos + RECORD_LENGTH_SIZE]
                    .try_into()
                    .context(Conversion)?,
            );
            pos += RECORD_LENGTH_SIZE;

            // Ensure the entire record is within the bounds of the mmap
            ensure!(
                pos + length as usize <= size as usize,
                LengthMismatch {
                    expected: pos + length as usize,
                    actual: size as usize
                }
            );

            // Verify the checksum (CRC32 of the data)
            let data = &mmap[pos..pos + length as usize];
            let computed_crc = crc32fast::hash(data);
            ensure!(
                computed_crc == crc,
                ChecksumMismatch {
                    expected: crc,
                    actual: computed_crc
                }
            );

            record_position.push(Position {
                start: (pos - CRC_SIZE - RECORD_LENGTH_SIZE) as u64,
                end: (pos + length as usize) as u64,
            });
            // Move to the next record
            pos += length as usize;
        }

        self.is_open = true;
        self.file = Some(file);
        self.mmap = Some(mmap);
        self.record_position = Some(record_position);
        self.size = size;
        Ok(())
    }

    pub fn close(&mut self) -> Result<()> {
        self.is_open = false;
        self.file.take();
        self.mmap.take();
        self.record_position.take();
        Ok(())
    }

    pub fn append(&mut self, data: &[u8]) -> Result<()> {
        ensure!(self.is_open, SegmentNotOpen { id: self.id });
        ensure!(self.size + data.len() as u64 <= MAX_FILE_SIZE, SegmentFull);

        let Some(file) = &mut self.file else {
            return SegmentNotOpen { id: self.id }.fail();
        };
        file.write_all(data).context(SegmentAppend)?;
        file.flush().context(Flush)?;

        // Remap
        let mmap = unsafe { MmapOptions::new().map_mut(&*file).context(Mmap)? };
        self.mmap = Some(mmap);
        self.size += data.len() as u64;

        Ok(())
    }

    pub fn read(&self, offset: u64, size: u64) -> Result<Vec<u8>> {
        ensure!(self.is_open, SegmentNotOpen { id: self.id });
        ensure!(
            offset + size <= self.size,
            LengthMismatch {
                expected: (offset + size) as usize,
                actual: self.size as usize
            }
        );

        let start = offset as usize;
        let end = start + size as usize;
        match &self.mmap {
            Some(mmap) => Ok(mmap[start..end].to_vec()),
            None => SegmentNotOpen { id: self.id }.fail(),
        }
    }

    pub fn append_record_position(&mut self, pos: &mut Vec<Position>) -> Result<()> {
        ensure!(self.is_open, SegmentNotOpen { id: self.id });
        match self.record_position.as_mut() {
            Some(record_position) => {
                record_position.append(pos);
                Ok(())
            }
            None => SegmentNotOpen { id: self.id }.fail(),
        }
    }

    pub fn update_seq(&mut self, min_seq: u64, max_seq: u64) -> Result<()> {
        ensure!(self.is_open, SegmentNotOpen { id: self.id });
        if min_seq < self.min_seq {
            self.min_seq = min_seq;
        }
        if max_seq > self.max_seq {
            self.max_seq = max_seq;
        }
        Ok(())
    }
}

pub struct SegmentManager {
    /// All segments protected by a mutex
    /// todo: maybe use a RWLock?
    all_segments: Mutex<HashMap<u64, Arc<Mutex<Segment>>>>,

    /// Cache for opened segments
    cache: Mutex<VecDeque<u64>>,

    /// Maximum size of the cache
    cache_size: usize,

    /// Directory for segment storage
    _segment_dir: String,

    /// Index of the latest segment for appending logs
    latest_segment_idx: AtomicU64,

    /// Encoding method for logs
    log_encoding: CommonLogEncoding,

    /// Sequence number for the next log
    next_sequence_num: AtomicU64,

    /// Runtime for handling write requests
    runtime: Arc<Runtime>,
}

impl SegmentManager {
    pub fn new(cache_size: usize, segment_dir: String, runtime: Arc<Runtime>) -> Result<Self> {
        let mut all_segments = HashMap::new();

        // Scan the directory for existing WAL files
        let mut max_segment_id: i32 = -1;

        // Segment file naming convention: segment_<id>.wal
        for entry in fs::read_dir(&segment_dir).context(FileOpen)? {
            let entry = entry.context(FileOpen)?;

            let path = entry.path();

            if !path.is_file() {
                continue;
            }

            match path.extension() {
                Some(ext) if ext == "wal" => ext,
                _ => continue,
            };

            let file_name = match path.file_name().and_then(|name| name.to_str()) {
                Some(name) => name,
                None => continue,
            };

            let segment_id = match file_name
                .trim_start_matches("segment_")
                .trim_end_matches(".wal")
                .parse::<u64>()
                .ok()
            {
                Some(id) => id,
                None => continue,
            };

            let segment = Segment::new(path.to_string_lossy().to_string(), segment_id)?;
            let segment_arc = Arc::new(Mutex::new(segment));

            if segment_id as i32 > max_segment_id {
                max_segment_id = segment_id as i32;
            }
            all_segments.insert(segment_id, segment_arc);
        }

        // If no existing segments, create a new one
        if max_segment_id == -1 {
            max_segment_id = 0;
            let path = format!("{}/segment_{}.wal", segment_dir, max_segment_id);
            let new_segment = Segment::new(path, max_segment_id as u64)?;
            let segment_arc = Arc::new(Mutex::new(new_segment));
            all_segments.insert(0, segment_arc);
        }

        Ok(Self {
            all_segments: Mutex::new(all_segments),
            cache: Mutex::new(VecDeque::new()),
            cache_size,
            _segment_dir: segment_dir,
            latest_segment_idx: AtomicU64::new(max_segment_id as u64),
            log_encoding: CommonLogEncoding::newest(),
            // todo: do not use MIN_SEQUENCE_NUMBER, read from the latest record
            next_sequence_num: AtomicU64::new(MIN_SEQUENCE_NUMBER + 1),
            runtime,
        })
    }

    fn get_segment(&self, segment_id: u64) -> Result<Arc<Mutex<Segment>>> {
        let mut cache = self.cache.lock().unwrap();
        let all_segments = self.all_segments.lock().unwrap();

        let segment = all_segments.get(&segment_id);

        let segment = match segment {
            Some(segment_arc) => segment_arc,
            None => return SegmentNotFound { id: segment_id }.fail(),
        };

        // Check if segment is already in cache
        if cache.iter().any(|id| *id == segment_id) {
            let segment = all_segments.get(&segment_id);
            return match segment {
                Some(segment_arc) => Ok(segment_arc.clone()),
                None => SegmentNotFound { id: segment_id }.fail(),
            };
        }

        // If not in cache, load from disk
        segment.lock().unwrap().open()?;

        // Add to cache
        if cache.len() == self.cache_size {
            let evicted_segment_id = cache.pop_front();
            // TODO: if the evicted segment is being read or written, wait for it to finish
            if let Some(evicted_segment_id) = evicted_segment_id {
                let evicted_segment = all_segments.get(&evicted_segment_id);
                if let Some(evicted_segment) = evicted_segment {
                    evicted_segment.lock().unwrap().close()?;
                } else {
                    return SegmentNotFound {
                        id: evicted_segment_id,
                    }
                    .fail();
                }
            }
        }
        cache.push_back(segment_id);

        Ok(segment.clone())
    }

    pub fn write(&self, _ctx: &WriteContext, batch: &LogWriteBatch) -> Result<SequenceNumber> {
        let segment = self.get_segment(self.latest_segment_idx.load(Ordering::Relaxed))?;
        let mut segment = segment.lock().unwrap();

        let entries_num = batch.len() as u64;
        let region_id = batch.location.region_id;

        let mut key_buf = BytesMut::new();
        let prev_sequence_num = self.alloc_sequence_num(entries_num);
        let mut next_sequence_num = prev_sequence_num;
        let mut data = Vec::new();
        let mut record_position = Vec::new();

        for entry in &batch.entries {
            let mut record_content = Vec::new();

            self.log_encoding
                .encode_key(
                    &mut key_buf,
                    &CommonLogKey::new(region_id, batch.location.table_id, next_sequence_num),
                )
                .box_err()
                .context(Encoding)?;

            let key_len = key_buf.len() as u16;
            record_content.put_u16_le(key_len);
            record_content.extend_from_slice(&key_buf);

            let value_len = entry.payload.len() as u32;
            record_content.put_u32_le(value_len);
            record_content.extend_from_slice(&entry.payload);

            record_position.push(Position {
                start: data.len() as u64,
                end: (data.len() + record_content.len() + CRC_SIZE + RECORD_LENGTH_SIZE) as u64,
            });

            // Calculate and encode the CRC
            let mut hasher = Hasher::new();
            hasher.update(&record_content);
            let crc = hasher.finalize();
            data.put_u32_le(crc);

            // Add length
            let record_len = record_content.len() as u32;
            data.put_u32_le(record_len);

            data.extend_from_slice(&record_content);

            next_sequence_num += 1;
        }

        // TODO: spawn a new task to write to segment
        // TODO: maybe need a write mutex?

        for pos in record_position.iter_mut() {
            pos.start += segment.size;
            pos.end += segment.size;
        }

        // Update the record position
        segment.append_record_position(&mut record_position)?;

        // Update the min and max sequence numbers
        segment.update_seq(prev_sequence_num, next_sequence_num - 1)?;

        // Append logs to segment file
        segment.append(&data)?;
        Ok(next_sequence_num - 1)
    }

    pub fn read(&self, ctx: &ReadContext, req: &ReadRequest) -> Result<BatchLogIteratorAdapter> {
        // Check read range's validity.
        let start = if let Some(start) = req.start.as_start_sequence_number() {
            start
        } else {
            MAX_SEQUENCE_NUMBER
        };
        let end = if let Some(end) = req.end.as_end_sequence_number() {
            end
        } else {
            MIN_SEQUENCE_NUMBER
        };
        if start > end {
            return Ok(BatchLogIteratorAdapter::empty());
        }
        let iter = SegmentLogIterator::new(
            self.log_encoding.clone(),
            self.get_segment(0)?,
            Some(req.location.region_id),
            Some(req.location.table_id),
            start,
            end,
        );
        Ok(BatchLogIteratorAdapter::new_with_sync(
            Box::new(iter),
            self.runtime.clone(),
            ctx.batch_size,
        ))
    }

    pub fn scan(&self, ctx: &ScanContext, req: &ScanRequest) -> Result<BatchLogIteratorAdapter> {
        let iter = SegmentLogIterator::new(
            self.log_encoding.clone(),
            self.get_segment(0)?,
            Some(req.region_id),
            None,
            MIN_SEQUENCE_NUMBER,
            MAX_SEQUENCE_NUMBER,
        );
        Ok(BatchLogIteratorAdapter::new_with_sync(
            Box::new(iter),
            self.runtime.clone(),
            ctx.batch_size,
        ))
    }

    pub fn mark_delete_entries_up_to(
        &self,
        _location: WalLocation,
        _sequence_num: SequenceNumber,
    ) -> Result<()> {
        todo!()
    }

    #[inline]
    fn alloc_sequence_num(&self, number: u64) -> SequenceNumber {
        self.next_sequence_num.fetch_add(number, Ordering::Relaxed)
    }

    pub fn sequence_num(&self, _location: WalLocation) -> Result<SequenceNumber> {
        let next_seq_num = self.next_sequence_num.load(Ordering::Relaxed);
        debug_assert!(next_seq_num > 0);
        Ok(next_seq_num - 1)
    }
}

// TODO: handle the case when read requests involving multiple segments
#[derive(Debug)]
pub struct SegmentLogIterator {
    log_encoding: CommonLogEncoding,
    segment: Arc<Mutex<Segment>>,
    region_id: Option<RegionId>,
    table_id: Option<TableId>,
    start: SequenceNumber,
    end: SequenceNumber,
    current_record_idx: usize,
    current_payload: Vec<u8>,
    no_more_data: bool,
}

impl SegmentLogIterator {
    pub fn new(
        log_encoding: CommonLogEncoding,
        segment: Arc<Mutex<Segment>>,
        region_id: Option<RegionId>,
        table_id: Option<TableId>,
        start: SequenceNumber,
        end: SequenceNumber,
    ) -> Self {
        SegmentLogIterator {
            log_encoding,
            segment,
            region_id,
            table_id,
            start,
            end,
            current_record_idx: 0,
            current_payload: Vec::new(),
            no_more_data: false,
        }
    }

    fn next(&mut self) -> Result<Option<LogEntry<&'_ [u8]>>> {
        if self.no_more_data {
            return Ok(None);
        }

        // todo: ensure that this segment is not evicted from the cache during the read
        // process, or that it is reloaded into the cache as needed
        let segment = self.segment.lock().unwrap();

        loop {
            let Some(record_position) = &segment.record_position else {
                self.no_more_data = true;
                return Ok(None);
            };
            let Some(pos) = record_position.get(self.current_record_idx) else {
                self.no_more_data = true;
                return Ok(None);
            };

            self.current_record_idx += 1;
            let record = segment.read(pos.start, pos.end - pos.start)?;

            ensure!(
                record.len() > CRC_SIZE + RECORD_LENGTH_SIZE,
                LengthMismatch {
                    expected: CRC_SIZE + RECORD_LENGTH_SIZE,
                    actual: record.len()
                }
            );

            let mut pos = 0;
            let crc_bytes = record[pos..pos + CRC_SIZE]
                .try_into()
                .box_err()
                .context(InvalidRecord)?;
            // No need to verify the checksum, as it has already been validated when the
            // segment was opened.
            let _crc = u32::from_le_bytes(crc_bytes);
            pos += CRC_SIZE;

            let length_bytes = record[pos..pos + RECORD_LENGTH_SIZE]
                .try_into()
                .box_err()
                .context(InvalidRecord)?;
            let length = u32::from_le_bytes(length_bytes);
            pos += RECORD_LENGTH_SIZE;

            ensure!(
                record.len() == length as usize + CRC_SIZE + RECORD_LENGTH_SIZE,
                LengthMismatch {
                    expected: length as usize + CRC_SIZE + RECORD_LENGTH_SIZE,
                    actual: record.len()
                }
            );

            let key_length_bytes = record[pos..pos + KEY_LENGTH_SIZE]
                .try_into()
                .box_err()
                .context(InvalidRecord)?;
            let key_length = u16::from_le_bytes(key_length_bytes);
            pos += KEY_LENGTH_SIZE;

            let key_bytes = record[pos..pos + key_length as usize]
                .try_into()
                .box_err()
                .context(InvalidRecord)?;
            let key = self
                .log_encoding
                .decode_key(key_bytes)
                .box_err()
                .context(InvalidRecord)?;
            pos += key_length as usize;

            let value_length_bytes = record[pos..pos + VALUE_LENGTH_SIZE]
                .try_into()
                .box_err()
                .context(InvalidRecord)?;
            let value_length = u32::from_le_bytes(value_length_bytes);
            pos += VALUE_LENGTH_SIZE;

            let value_bytes = record[pos..pos + value_length as usize]
                .try_into()
                .box_err()
                .context(InvalidRecord)?;
            let value = self
                .log_encoding
                .decode_value(value_bytes)
                .box_err()
                .context(InvalidRecord)?;

            if key.sequence_num < self.start {
                continue;
            }
            if key.sequence_num > self.end {
                self.no_more_data = true;
                return Ok(None);
            }
            if let Some(region_id) = self.region_id {
                if key.region_id != region_id {
                    continue;
                }
            }
            if let Some(table_id) = self.table_id {
                if key.table_id != table_id {
                    continue;
                }
            }

            self.current_payload = value.to_owned();

            return Ok(Some(LogEntry {
                table_id: key.table_id,
                sequence: key.sequence_num,
                payload: self.current_payload.as_slice(),
            }));
        }
    }
}

impl SyncLogIterator for SegmentLogIterator {
    fn next_log_entry(&mut self) -> crate::manager::Result<Option<LogEntry<&'_ [u8]>>> {
        self.next().box_err().context(Read)
    }
}
