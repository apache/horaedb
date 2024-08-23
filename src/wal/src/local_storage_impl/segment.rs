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
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc
    },
};

use async_trait::async_trait;
use codec::Encoder;
use common_types::{table::TableId, SequenceNumber, MAX_SEQUENCE_NUMBER, MIN_SEQUENCE_NUMBER};
use generic_error::{BoxError, GenericError};
use macros::define_result;
use memmap2::{MmapMut, MmapOptions};
use runtime::Runtime;
use snafu::{ensure, Backtrace, ResultExt, Snafu};
use tokio::sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{
    kv_encoder::CommonLogEncoding,
    local_storage_impl::record_encoding::{Record, RecordEncoding},
    log_batch::{LogEntry, LogWriteBatch},
    manager::{
        AsyncLogIterator, BatchLogIteratorAdapter, Read, ReadContext, ReadRequest, RegionId, ScanContext, ScanRequest, SyncLogIterator, WalLocation, WriteContext
    },
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to open or create file: {}", source))]
    FileOpen { source: io::Error },

    #[snafu(display("Failed to open or create dir: {}", source))]
    DirOpen { source: io::Error },

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

    #[snafu(display(
        "Failed to write log entries, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    WriteExec {
        source: GenericError,
        backtrace: Backtrace,
    },
}

define_result!(Error);

const SEGMENT_HEADER: &[u8] = b"HoraeDBWAL";
const WAL_SEGMENT_V0: u8 = 0;
const NEWEST_WAL_SEGMENT_VERSION: u8 = WAL_SEGMENT_V0;
const VERSION_SIZE: usize = 1;

/// Segment file format:
///
/// ```text
/// +-------------+--------+--------+--------+---+--------+
/// | Version(u8) | Header | Record | Record |...| Record |
/// +-------------+--------+--------+--------+---+--------+
/// ```
///
/// The `Header` is a fixed string. The format of the `Record` can be referenced
/// in the struct `Record`.
#[derive(Debug)]
pub struct Segment {
    /// The version of the segment.
    version: u8,

    /// The file path of the segment.
    path: String,

    /// A unique identifier for the segment.
    id: u64,

    /// The size of the segment in bytes.
    size: u64,

    /// The maximum size of the segment file.
    max_file_size: u64,

    /// The minimum sequence number of records within this segment.
    min_seq: SequenceNumber,

    /// The maximum sequence number of records within this segment.
    max_seq: SequenceNumber,

    /// The encoding format used for records within this segment.
    record_encoding: RecordEncoding,

    /// An optional file handle for the segment.
    /// This may be `None` if the file is not currently open.
    file: Option<File>,

    /// An optional memory-mapped mutable buffer of the segment.
    /// This may be `None` if the segment is not memory-mapped.
    mmap: Option<MmapMut>,

    /// An optional vector of positions within the segment.
    /// This may be `None` if the segment is not memory-mapped.
    record_position: Option<Vec<Position>>,
}

#[derive(Debug, Clone)]
pub struct Position {
    start: u64,
    end: u64,
}

impl Segment {
    pub fn new(path: String, segment_id: u64, max_file_size: u64) -> Result<Segment> {
        let mut segment = Segment {
            version: NEWEST_WAL_SEGMENT_VERSION,
            path: path.clone(),
            id: segment_id,
            size: SEGMENT_HEADER.len() as u64,
            max_file_size,
            min_seq: MAX_SEQUENCE_NUMBER,
            max_seq: MIN_SEQUENCE_NUMBER,
            record_encoding: RecordEncoding::newest(),
            file: None,
            mmap: None,
            record_position: None,
        };

        if !Path::new(&path).exists() {
            // If the file does not exist, create a new one
            let mut file = File::create(&path).context(FileOpen)?;
            file.write_all(&[NEWEST_WAL_SEGMENT_VERSION])
                .context(FileOpen)?;
            file.write_all(SEGMENT_HEADER).context(FileOpen)?;
            file.set_len(max_file_size).context(FileOpen)?;
            return Ok(segment);
        }

        // Open the segment file to update min and max sequence number and file size
        segment.open()?;

        // Close the segment file. If the segment is to be used for read or write, it will be opened again
        segment.close()?;


        Ok(segment)
    }

    pub fn open(&mut self) -> Result<()> {
        // Open the segment file
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.path)
            .context(FileOpen)?;
    
        let metadata = file.metadata().context(FileOpen)?;
        let file_size = metadata.len();
    
        // Map the file to memory
        let mmap = unsafe { MmapOptions::new().map_mut(&file).context(Mmap)? };
    
        // Validate segment version
        let version = mmap[0];
        ensure!(version == self.version, InvalidHeader);
    
        // Validate segment header
        let header_len = SEGMENT_HEADER.len();
        ensure!(file_size >= (VERSION_SIZE + header_len) as u64, InvalidHeader);
        let header = &mmap[VERSION_SIZE..VERSION_SIZE + header_len];
        ensure!(header == SEGMENT_HEADER, InvalidHeader);
    
        // Read and validate all records
        let mut pos = VERSION_SIZE + header_len;
        let mut record_position = Vec::new();

        // Update min and max sequence number
        let mut min_seq = MAX_SEQUENCE_NUMBER;
        let mut max_seq = MIN_SEQUENCE_NUMBER;

        while pos < file_size as usize {
            let data = &mmap[pos..];
    
            match self.record_encoding.decode(data).box_err() {
                Ok(record) => {
                    record_position.push(Position {
                        start: pos as u64,
                        end: (pos + record.len()) as u64,
                    });
                    min_seq = min_seq.min(record.sequence_num);
                    max_seq = max_seq.max(record.sequence_num);
                    pos += record.len();
                }
                Err(_) => {
                    // If decoding fails, we've reached the end of valid data
                    break;
                }
            }
        }
    
        self.file = Some(file);
        self.mmap = Some(mmap);
        self.record_position = Some(record_position);
        self.size = pos as u64;
        self.min_seq = min_seq;
        self.max_seq = max_seq;
        Ok(())
    }

    pub fn close(&mut self) -> Result<()> {
        self.file.take();
        self.mmap.take();
        self.record_position.take();
        Ok(())
    }

    /// Append a slice to the segment file.
    pub fn append(&mut self, data: &[u8]) -> Result<()> {
        ensure!(self.size + data.len() as u64 <= self.max_file_size, SegmentFull);
    
        // Ensure the segment file is open
        let Some(mmap) = &mut self.mmap else {
            return SegmentNotOpen { id: self.id }.fail();
        };
    
        // Append to mmap
        let start = self.size as usize;
        let end = start + data.len();
        mmap[start..end].copy_from_slice(data);
        mmap.flush_range(start, data.len()).context(Flush)?;
    
        self.size += data.len() as u64;
    
        Ok(())
    }

    pub fn read(&self, offset: u64, size: u64) -> Result<Vec<u8>> {
        // Ensure that the reading range is within the file
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
        match self.record_position.as_mut() {
            Some(record_position) => {
                record_position.append(pos);
                Ok(())
            }
            None => SegmentNotOpen { id: self.id }.fail(),
        }
    }

    pub fn update_seq(&mut self, min_seq: u64, max_seq: u64) -> Result<()> {
        if min_seq < self.min_seq {
            self.min_seq = min_seq;
        }
        if max_seq > self.max_seq {
            self.max_seq = max_seq;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct SegmentManager {
    /// All segments protected by a mutex
    segments: Mutex<HashMap<u64, Arc<RwLock<Segment>>>>,

    /// Cache for opened segments
    cache: Mutex<VecDeque<u64>>,

    /// Maximum size of the cache
    cache_size: usize,

    /// Maximum size of a segment file
    max_file_size: u64,

    /// Directory for segment storage
    _segment_dir: String,
}

#[derive(Debug)]
pub struct Region {
    /// Identifier for regions.
    _region_id: u64,

    /// All segments protected by a mutex
    all_segments: Mutex<HashMap<u64, Arc<RwLock<Segment>>>>,

    /// Cache for opened segments
    cache: Mutex<VecDeque<u64>>,

    /// Maximum size of the cache
    cache_size: usize,

    /// Maximum size of a segment file
    max_file_size: u64,

    /// Directory for segment storage
    _segment_dir: String,

    /// Index of the latest segment for appending logs
    current: Mutex<u64>,

    /// Encoding method for logs
    log_encoding: CommonLogEncoding,

    /// Encoding method for records
    record_encoding: RecordEncoding,

    /// Sequence number for the next log
    next_sequence_num: AtomicU64,

    /// Runtime for handling write requests
    runtime: Arc<Runtime>,
}

impl Region {
    pub fn new(
        region_id: u64,
        cache_size: usize,
        max_file_size: u64,
        segment_dir: String,
        runtime: Arc<Runtime>,
    ) -> Result<Self> {
        let mut all_segments = HashMap::new();

        // Scan the directory for existing WAL files
        let mut max_segment_id: i32 = -1;
        let mut next_sequence_num: u64 = MIN_SEQUENCE_NUMBER + 1;

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

            let segment = Segment::new(path.to_string_lossy().to_string(), segment_id, max_file_size)?;
            next_sequence_num = next_sequence_num.max(segment.max_seq + 1);
            let segment = Arc::new(RwLock::new(segment));

            if segment_id as i32 > max_segment_id {
                max_segment_id = segment_id as i32;
            }
            all_segments.insert(segment_id, segment);
        }

        // If no existing segments, create a new one
        if max_segment_id == -1 {
            max_segment_id = 0;
            let path = format!("{}/segment_{}.wal", segment_dir, max_segment_id);
            let new_segment = Segment::new(path, max_segment_id as u64, max_file_size)?;
            let new_segment = Arc::new(RwLock::new(new_segment));
            all_segments.insert(0, new_segment);
        }

        Ok(Self {
            _region_id: region_id,
            all_segments: Mutex::new(all_segments),
            cache: Mutex::new(VecDeque::new()),
            cache_size,
            max_file_size,
            _segment_dir: segment_dir,
            current: Mutex::new(max_segment_id as u64),
            log_encoding: CommonLogEncoding::newest(),
            record_encoding: RecordEncoding::newest(),
            next_sequence_num: AtomicU64::new(next_sequence_num),
            runtime,
        })
    }

    /// Obtain the target segment. If it is not open, then open it and put it to
    /// the cache.
    /// todo: need to re-require the lock, cache may be unconsistent with segment
    async fn get_segment(&self, segment_id: u64) -> Result<Arc<RwLock<Segment>>> {
        let mut cache = self.cache.lock().await;
        let all_segments = self.all_segments.lock().await;

        let segment = all_segments.get(&segment_id);

        let segment = match segment {
            Some(segment) => segment,
            None => return SegmentNotFound { id: segment_id }.fail(),
        };

        // Check if segment is already in cache
        if cache.iter().any(|id| *id == segment_id) {
            let segment = all_segments.get(&segment_id);
            return match segment {
                Some(segment) => Ok(segment.clone()),
                None => SegmentNotFound { id: segment_id }.fail(),
            };
        }

        // If not in cache, load from disk
        segment.write().await.open()?;

        // Add to cache
        if cache.len() == self.cache_size {
            let evicted_segment_id = cache.pop_front();
            if let Some(evicted_segment_id) = evicted_segment_id {
                let evicted_segment = all_segments.get(&evicted_segment_id);
                if let Some(evicted_segment) = evicted_segment {
                    evicted_segment.write().await.close()?;
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

    pub async fn write(&self, _ctx: &WriteContext, batch: &LogWriteBatch) -> Result<SequenceNumber> {
        // Lock
        let mut current = self.current.lock().await;

        let entries_num = batch.len() as u64;
        let table_id = batch.location.table_id;

        // Allocate sequence number
        let prev_sequence_num = self.alloc_sequence_num(entries_num);
        let mut next_sequence_num = prev_sequence_num;

        let mut data = Vec::new();
        let mut record_position = Vec::new();

        for entry in &batch.entries {
            // Encode the record
            let record = Record::new(table_id, next_sequence_num, &entry.payload)
                .box_err()
                .context(Encoding)?;
            self.record_encoding
                .encode(&mut data, &record)
                .box_err()
                .context(Encoding)?;

            record_position.push(Position {
                start: (data.len() - record.len()) as u64,
                end: data.len() as u64,
            });

            next_sequence_num += 1;
        }

        {
            let segment = self.get_segment(*current).await?;
            let segment = segment.read().await;
    
            // Check if the current segment has enough space for the new data
            // If not, create a new segment and update the current segment ID
            if segment.size + data.len() as u64 > segment.max_file_size {
                let new_segment_id = *current + 1;
                let new_segment = Segment::new(format!("{}/segment_{}.wal", self._segment_dir, new_segment_id), new_segment_id, self.max_file_size)?;
                let new_segment = Arc::new(RwLock::new(new_segment));
    
                let mut all_segments = self.all_segments.lock().await;
                all_segments.insert(new_segment_id, new_segment.clone());
                *current = new_segment_id;
            }
        }


        let segment = self.get_segment(*current).await?;
        let mut segment = segment.write().await;


        for pos in record_position.iter_mut() {
            pos.start += segment.size;
            pos.end += segment.size;
        }

        // TODO: spawn a new task to write to segment
        // Update the record position
        segment.append_record_position(&mut record_position)?;

        // Update the min and max sequence numbers
        segment.update_seq(prev_sequence_num, next_sequence_num - 1)?;

        // Append logs to segment file
        segment.append(&data)?;

        Ok(next_sequence_num - 1)

    }

    pub async fn read(&self, ctx: &ReadContext, req: &ReadRequest, region: Arc<Region>) -> Result<BatchLogIteratorAdapter> {
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

        let iter = MultiSegmentLogIterator::new(
            region,
            self.log_encoding.clone(),
            self.record_encoding.clone(),
            Some(req.location.table_id),
            start,
            end,
        ).await?;

        Ok(BatchLogIteratorAdapter::new_with_async(
            Box::new(iter),
            ctx.batch_size,
        ))
    }

    pub async fn scan(&self, ctx: &ScanContext, _req: &ScanRequest, region: Arc<Region>) -> Result<BatchLogIteratorAdapter> {        
        let iter = MultiSegmentLogIterator::new(
            region,
            self.log_encoding.clone(),
            self.record_encoding.clone(),
            None,
            MIN_SEQUENCE_NUMBER,
            MAX_SEQUENCE_NUMBER,
        ).await?;
        Ok(BatchLogIteratorAdapter::new_with_async(
            Box::new(iter),
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

pub struct RegionManager {
    root_dir: String,
    regions: Mutex<HashMap<u64, Arc<Region>>>,
    cache_size: usize,
    max_file_size: u64,
    runtime: Arc<Runtime>,
}

impl RegionManager {
    // Create a RegionManager, and scans all the region folders located under
    // root_dir.
    pub fn new(root_dir: String, cache_size: usize, max_file_size: u64, runtime: Arc<Runtime>) -> Result<Self> {
        let mut regions = HashMap::new();

        // Naming conversion: <root_dir>/<region_id>
        for entry in fs::read_dir(&root_dir).context(DirOpen)? {
            let entry = entry.context(DirOpen)?;

            let path = entry.path();
            if path.is_file() {
                continue;
            }

            let dir_name = match path.file_name().and_then(|name| name.to_str()) {
                Some(name) => name,
                None => continue,
            };

            // Parse region id from dir name
            let region_id = match dir_name.parse::<u64>().ok() {
                Some(id) => id,
                None => continue,
            };

            let region = Region::new(
                region_id,
                cache_size,
                max_file_size,
                path.to_string_lossy().to_string(),
                runtime.clone(),
            )?;
            regions.insert(region_id, Arc::new(region));
        }

        Ok(Self {
            root_dir,
            regions: Mutex::new(regions),
            cache_size,
            max_file_size,
            runtime,
        })
    }

    /// Retrieve a region by its `region_id`. If the region does not exist,
    /// create a new one.
    async fn get_region(&self, region_id: RegionId) -> Result<Arc<Region>> {
        let mut regions = self.regions.lock().await;
        if let Some(region) = regions.get(&region_id) {
            return Ok(region.clone());
        }

        let region_dir = Path::new(&self.root_dir).join(region_id.to_string());
        fs::create_dir_all(&region_dir).context(DirOpen)?;

        let region = Region::new(
            region_id,
            self.cache_size,
            self.max_file_size,
            region_dir.to_string_lossy().to_string(),
            self.runtime.clone(),
        )?;

        Ok(regions.entry(region_id).or_insert(Arc::new(region)).clone())
    }

    pub async fn write(&self, ctx: &WriteContext, batch: &LogWriteBatch) -> Result<SequenceNumber> {
        let region = self.get_region(batch.location.region_id).await?;
        region.write(ctx, batch).await
    }

    pub async fn read(&self, ctx: &ReadContext, req: &ReadRequest) -> Result<BatchLogIteratorAdapter> {
        let region = self.get_region(req.location.region_id).await?;
        region.read(ctx, req, region.clone()).await
    }

    pub async fn scan(&self, ctx: &ScanContext, req: &ScanRequest) -> Result<BatchLogIteratorAdapter> {
        let region = self.get_region(req.region_id).await?;
        region.scan(ctx, req, region.clone()).await
    }

    pub async fn mark_delete_entries_up_to(
        &self,
        location: WalLocation,
        sequence_num: SequenceNumber,
    ) -> Result<()> {
        let region = self.get_region(location.region_id).await?;
        region.mark_delete_entries_up_to(location, sequence_num)
    }

    pub async fn sequence_num(&self, location: WalLocation) -> Result<SequenceNumber> {
        let region = self.get_region(location.region_id).await?;
        region.sequence_num(location)
    }
}

// TODO: handle the case when read requests involving multiple segments
#[derive(Debug)]
struct SegmentLogIterator {
    /// Encoding method for common log.
    log_encoding: CommonLogEncoding,

    /// Encoding method for records.
    record_encoding: RecordEncoding,

    /// Raw content of the segment.
    segment_content: Vec<u8>,

    /// Positions of records within the segment content.
    record_positions: Vec<Position>,

    /// Optional identifier for the table, which is used to filter logs.
    table_id: Option<TableId>,

    /// Starting sequence number for log iteration.
    start: SequenceNumber,

    /// Ending sequence number for log iteration.
    end: SequenceNumber,

    /// Index of the current record within the segment.
    current_record_idx: usize,

    /// Flag indicating whether there is no more data to read.
    no_more_data: bool,
}

impl SegmentLogIterator {
    pub async fn new(
        log_encoding: CommonLogEncoding,
        record_encoding: RecordEncoding,
        segment: Arc<RwLock<Segment>>,
        table_id: Option<TableId>,
        start: SequenceNumber,
        end: SequenceNumber,
    ) -> Result<Self> {
        let segment = segment.read().await;
        
        // Read the entire content of the segment
        let segment_content = segment.read(0, segment.size)?;
        
        // Get record positions
        let record_positions = if let Some(positions) = segment.record_position.as_ref() {
            positions.clone()
        } else {
            Vec::new()
        };

        Ok(Self {
            log_encoding,
            record_encoding,
            segment_content,
            record_positions,
            table_id,
            start,
            end,
            current_record_idx: 0,
            no_more_data: false,
        })
    }

    async fn next(&mut self) -> Result<Option<LogEntry<Vec<u8>>>> {
        if self.no_more_data {
            return Ok(None);
        }

        loop {
            // Get the next record position
            let Some(pos) = self.record_positions.get(self.current_record_idx) else {
                self.no_more_data = true;
                return Ok(None);
            };

            self.current_record_idx += 1;

            // Extract the record data from the segment content
            let record_data = &self.segment_content[pos.start as usize..pos.end as usize];

            // Decode the record
            let record = self.record_encoding
                .decode(record_data)
                .box_err()
                .context(InvalidRecord)?;

            // Filter by sequence number
            if record.sequence_num < self.start {
                continue;
            }
            if record.sequence_num > self.end {
                self.no_more_data = true;
                return Ok(None);
            }

            // Filter by table_id
            if let Some(table_id) = self.table_id {
                if record.table_id != table_id {
                    continue;
                }
            }

            // Decode the value
            let value = self.log_encoding
                .decode_value(&record.value)
                .box_err()
                .context(InvalidRecord)?;

            return Ok(Some(LogEntry {
                table_id: record.table_id,
                sequence: record.sequence_num,
                payload: value.to_owned(),
            }));
        }
    }
}

#[derive(Debug)]
pub struct MultiSegmentLogIterator {
    /// Region that contains all segments involved in this read operation.
    region: Arc<Region>,

    /// All segments involved in this read operation.
    segments: Vec<u64>,

    /// Current segment index.
    current_segment_idx: usize,

    /// Current segment iterator.
    current_iterator: Option<SegmentLogIterator>,

    /// Encoding method for common log.
    log_encoding: CommonLogEncoding,

    /// Encoding method for records.
    record_encoding: RecordEncoding,

    /// Optional identifier for the table, which is used to filter logs.
    table_id: Option<TableId>,

    /// Starting sequence number for log iteration.
    start: SequenceNumber,

    /// Ending sequence number for log iteration.
    end: SequenceNumber,

    /// The raw payload data of the current record.
    current_payload: Vec<u8>,
}

impl MultiSegmentLogIterator {
    pub async fn new(
        region: Arc<Region>,
        log_encoding: CommonLogEncoding,
        record_encoding: RecordEncoding,
        table_id: Option<TableId>,
        start: SequenceNumber,
        end: SequenceNumber,
    ) -> Result<Self> {
        // Find all segments that contain the requested sequence numbers
        let mut relevant_segments = Vec::new();

        {
            let all_segments = region.all_segments.lock().await;

            for (_, segment) in all_segments.iter() {
                let segment: RwLockReadGuard<Segment> = segment.read().await;
                if segment.min_seq <= end && segment.max_seq >= start {
                    relevant_segments.push(segment.id);
                }
            }
        }

        let mut iter = Self {
            region,
            segments: relevant_segments,
            current_segment_idx: 0,
            current_iterator: None,
            log_encoding,
            record_encoding,
            table_id,
            start,
            end,
            current_payload: Vec::new(),
        };

        // Load the first segment iterator
        iter.load_next_segment_iterator().await?;

        Ok(iter)
    }

    async fn load_next_segment_iterator(&mut self) -> Result<bool> {
        if self.current_segment_idx >= self.segments.len() {
            self.current_iterator = None;
            return Ok(false);
        }

        let segment = self.segments[self.current_segment_idx].clone();
        let segment = self.region.get_segment(segment).await?;
        let iterator = SegmentLogIterator::new(
            self.log_encoding.clone(),
            self.record_encoding.clone(),
            segment,
            self.table_id,
            self.start,
            self.end,
        ).await?;

        self.current_iterator = Some(iterator);
        self.current_segment_idx += 1;

        Ok(true)
    }

    async fn next(&mut self) -> Result<Option<LogEntry<&'_ [u8]>>> {
        loop {
            if let Some(ref mut iterator) = self.current_iterator {
                if let Some(entry) = iterator.next().await? {
                    self.current_payload = entry.payload.to_owned();
                    return Ok(Some(LogEntry{
                        table_id: entry.table_id,
                        sequence: entry.sequence,
                        payload: &self.current_payload,
                    }));
                }
            } 
            if !self.load_next_segment_iterator().await? {
                return Ok(None);
            }
        }
    }
}

#[async_trait]
impl AsyncLogIterator for MultiSegmentLogIterator {
    async fn next_log_entry(&mut self) -> crate::manager::Result<Option<LogEntry<&'_ [u8]>>> {
        self.next().await.box_err().context(Read)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use runtime::Builder;
    use tempfile::tempdir;

    use super::*;

    const MAX_FILE_SIZE :u64 = 64 * 1024 * 1024;

    #[tokio::test]
    async fn test_segment_creation() {
        let dir = tempdir().unwrap();
        let path = dir
            .path()
            .join("segment_0.wal")
            .to_str()
            .unwrap()
            .to_string();

        let segment = Segment::new(path.clone(), 0, MAX_FILE_SIZE);
        assert!(segment.is_ok());

        let segment = segment.unwrap();
        assert_eq!(segment.version, NEWEST_WAL_SEGMENT_VERSION);
        assert_eq!(segment.path, path);
        assert_eq!(segment.id, 0);
        assert_eq!(segment.size, SEGMENT_HEADER.len() as u64);

        let segment_content = fs::read(path).unwrap();
        assert_eq!(segment_content[0], NEWEST_WAL_SEGMENT_VERSION);
        assert_eq!(
            &segment_content[VERSION_SIZE..VERSION_SIZE + SEGMENT_HEADER.len()],
            SEGMENT_HEADER
        );
    }

    #[tokio::test]
    async fn test_segment_open() {
        let dir = tempdir().unwrap();
        let path = dir
            .path()
            .join("segment_0.wal")
            .to_str()
            .unwrap()
            .to_string();
        let mut segment = Segment::new(path.clone(), 0, MAX_FILE_SIZE).unwrap();

        let result = segment.open();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_segment_append_and_read() {
        let dir = tempdir().unwrap();
        let path = dir
            .path()
            .join("segment_0.wal")
            .to_str()
            .unwrap()
            .to_string();
        let mut segment = Segment::new(path.clone(), 0, MAX_FILE_SIZE).unwrap();
        segment.open().unwrap();

        let data = b"test_data";
        let append_result = segment.append(data);
        assert!(append_result.is_ok());

        let read_result = segment.read(
            (VERSION_SIZE + SEGMENT_HEADER.len()) as u64,
            data.len() as u64,
        );
        assert!(read_result.is_ok());
        assert_eq!(read_result.unwrap(), data);
    }

    #[tokio::test]
    async fn test_region_creation() {
        let dir = tempdir().unwrap();
        let runtime = Arc::new(Builder::default().build().unwrap());

        let region = Region::new(1, 1, MAX_FILE_SIZE, dir.path().to_str().unwrap().to_string(), runtime);
        assert!(region.is_ok());

        let segment_manager = region.unwrap();
        let segment = segment_manager.get_segment(0).await;
        assert!(segment.is_ok());
    }
}

