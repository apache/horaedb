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

use codec::Encoder;
use common_types::{table::TableId, SequenceNumber, MAX_SEQUENCE_NUMBER, MIN_SEQUENCE_NUMBER};
use generic_error::{BoxError, GenericError};
use macros::define_result;
use memmap2::{MmapMut, MmapOptions};
use runtime::Runtime;
use snafu::{ensure, Backtrace, ResultExt, Snafu};
use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use crate::{
    kv_encoder::CommonLogEncoding,
    local_storage_impl::record_encoding::{Record, RecordEncoding},
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

    /// The current size in use.
    current_size: u64,

    /// The size of the segment file.
    segment_size: u64,

    /// The minimum sequence number of records within this segment.
    min_seq: SequenceNumber,

    /// The maximum sequence number of records within this segment.
    max_seq: SequenceNumber,

    /// The encoding format used for records within this segment.
    record_encoding: RecordEncoding,

    /// An optional memory-mapped mutable buffer of the segment.
    /// This may be `None` if the segment is not memory-mapped.
    mmap: Option<MmapMut>,

    /// An optional vector of positions within the segment.
    record_position: Vec<Position>,
}

#[derive(Debug, Clone)]
pub struct Position {
    start: u64,
    end: u64,
}

impl Segment {
    pub fn new(path: String, segment_id: u64, segment_size: u64) -> Result<Segment> {
        let mut segment = Segment {
            version: NEWEST_WAL_SEGMENT_VERSION,
            path: path.clone(),
            id: segment_id,
            current_size: SEGMENT_HEADER.len() as u64,
            segment_size,
            min_seq: MAX_SEQUENCE_NUMBER,
            max_seq: MIN_SEQUENCE_NUMBER,
            record_encoding: RecordEncoding::newest(),
            mmap: None,
            record_position: Vec::new(),
        };

        if !Path::new(&path).exists() {
            // If the file does not exist, create a new one
            let mut file = File::create(&path).context(FileOpen)?;
            file.write_all(&[NEWEST_WAL_SEGMENT_VERSION])
                .context(FileOpen)?;
            file.write_all(SEGMENT_HEADER).context(FileOpen)?;
            file.set_len(segment_size).context(FileOpen)?;
            return Ok(segment);
        }

        // Open the segment file to update min and max sequence number and file size
        segment.open()?;

        // Close the segment file. If the segment is to be used for read or write, it
        // will be opened again
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
        ensure!(
            file_size >= (VERSION_SIZE + header_len) as u64,
            InvalidHeader
        );
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

        self.mmap = Some(mmap);
        self.record_position = record_position;
        self.current_size = pos as u64;
        self.min_seq = min_seq;
        self.max_seq = max_seq;
        Ok(())
    }

    pub fn close(&mut self) -> Result<()> {
        self.mmap.take();
        Ok(())
    }

    pub fn is_open(&self) -> bool {
        self.mmap.is_some()
    }

    /// Append a slice to the segment file.
    fn append(&mut self, data: &[u8]) -> Result<()> {
        ensure!(
            self.current_size + data.len() as u64 <= self.segment_size,
            SegmentFull
        );

        // Ensure the segment file is open
        let Some(mmap) = &mut self.mmap else {
            return SegmentNotOpen { id: self.id }.fail();
        };

        // Append to mmap
        let start = self.current_size as usize;
        let end = start + data.len();
        mmap[start..end].copy_from_slice(data);
        mmap.flush_range(start, data.len()).context(Flush)?;

        self.current_size += data.len() as u64;

        Ok(())
    }

    pub fn read(&self, offset: u64, size: u64) -> Result<Vec<u8>> {
        // Ensure that the reading range is within the file
        ensure!(
            offset + size <= self.current_size,
            LengthMismatch {
                expected: (offset + size) as usize,
                actual: self.current_size as usize
            }
        );

        let start = offset as usize;
        let end = start + size as usize;
        match &self.mmap {
            Some(mmap) => Ok(mmap[start..end].to_vec()),
            None => SegmentNotOpen { id: self.id }.fail(),
        }
    }

    pub fn append_records(
        &mut self,
        data: &[u8],
        positions: &mut Vec<Position>,
        prev_sequence_num: SequenceNumber,
        next_sequence_num: SequenceNumber,
    ) -> Result<()> {
        // Append logs to segment file
        self.append(data)?;

        // Update record position
        self.record_position.append(positions);

        // Update min and max sequence number
        self.min_seq = self.min_seq.min(prev_sequence_num);
        self.max_seq = self.max_seq.max(next_sequence_num - 1);

        Ok(())
    }
}

#[derive(Debug)]
pub struct SegmentManager {
    /// All segments protected by a mutex
    all_segments: Mutex<HashMap<u64, Arc<Mutex<Segment>>>>,

    /// Cache for opened segments
    cache: Mutex<VecDeque<u64>>,

    /// Maximum size of the cache
    cache_size: usize,
}

impl SegmentManager {
    fn add_segment(&self, id: u64, segment: Arc<Mutex<Segment>>) -> Result<()> {
        let mut all_segments = self.all_segments.lock().unwrap();
        all_segments.insert(id, segment);
        Ok(())
    }

    /// Obtain the target segment
    fn get_segment(&self, segment_id: u64) -> Result<Arc<Mutex<Segment>>> {
        let all_segments = self.all_segments.lock().unwrap();

        let segment = all_segments.get(&segment_id);

        let segment = match segment {
            Some(segment) => segment,
            None => return SegmentNotFound { id: segment_id }.fail(),
        };

        Ok(segment.clone())
    }

    /// Open segment if it is not in cache, need to acquire the lock outside
    fn open_segment(&self, segment: &mut Segment) -> Result<()> {
        let mut cache = self.cache.lock().unwrap();

        // Check if segment is already in cache
        if cache.iter().any(|id| *id == segment.id) {
            return Ok(());
        }

        // If not in cache, load from disk
        segment.open()?;

        // Add to cache
        if cache.len() == self.cache_size {
            let evicted_segment_id = cache.pop_front();
            if let Some(evicted_segment_id) = evicted_segment_id {
                // The evicted segment should be closed first
                let evicted_segment = self.get_segment(evicted_segment_id)?;
                let mut evicted_segment = evicted_segment.lock().unwrap();
                evicted_segment.close()?;
            }
        }
        cache.push_back(segment.id);

        Ok(())
    }

    pub fn mark_delete_entries_up_to(
        &self,
        _location: WalLocation,
        _sequence_num: SequenceNumber,
    ) -> Result<()> {
        todo!()
    }

    pub fn close_all(&self) -> Result<()> {
        let mut cache = self.cache.lock().unwrap();
        cache.clear();
        let all_segments = self.all_segments.lock().unwrap();
        for segment in all_segments.values() {
            segment.lock().unwrap().close()?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct Region {
    /// Identifier for regions.
    _region_id: u64,

    /// Contains all segments and manages the opening and closing of each
    /// segment
    segment_manager: Arc<SegmentManager>,

    /// Encoding method for logs
    log_encoding: CommonLogEncoding,

    /// Encoding method for records
    record_encoding: RecordEncoding,

    /// Runtime for handling write requests
    runtime: Arc<Runtime>,

    /// All segments are fixed size
    segment_size: u64,

    /// Directory of segment files
    region_dir: String,

    /// Sequence number for the next log
    next_sequence_num: AtomicU64,

    /// The latest segment for appending logs
    current_segment: Mutex<Arc<Mutex<Segment>>>,
}

impl Region {
    pub fn new(
        region_id: u64,
        cache_size: usize,
        segment_size: u64,
        region_dir: String,
        runtime: Arc<Runtime>,
    ) -> Result<Self> {
        let mut all_segments = HashMap::new();

        // Scan the directory for existing WAL files
        let mut max_segment_id: i32 = -1;
        let mut next_sequence_num: u64 = MIN_SEQUENCE_NUMBER + 1;

        // Segment file naming convention: segment_<id>.wal
        for entry in fs::read_dir(&region_dir).context(FileOpen)? {
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

            let segment =
                Segment::new(path.to_string_lossy().to_string(), segment_id, segment_size)?;
            next_sequence_num = next_sequence_num.max(segment.max_seq + 1);
            let segment = Arc::new(Mutex::new(segment));

            if segment_id as i32 > max_segment_id {
                max_segment_id = segment_id as i32;
            }
            all_segments.insert(segment_id, segment);
        }

        // If no existing segments, create a new one
        if max_segment_id == -1 {
            max_segment_id = 0;
            let path = format!("{}/segment_{}.wal", region_dir, max_segment_id);
            let new_segment = Segment::new(path, max_segment_id as u64, segment_size)?;
            let new_segment = Arc::new(Mutex::new(new_segment));
            all_segments.insert(0, new_segment);
        }

        let latest_segment = all_segments.get(&(max_segment_id as u64)).unwrap().clone();

        let segment_manager = SegmentManager {
            all_segments: Mutex::new(all_segments),
            cache: Mutex::new(VecDeque::new()),
            cache_size,
        };

        Ok(Self {
            _region_id: region_id,
            segment_manager: Arc::new(segment_manager),
            log_encoding: CommonLogEncoding::newest(),
            record_encoding: RecordEncoding::newest(),
            segment_size,
            region_dir,
            next_sequence_num: AtomicU64::new(next_sequence_num),
            runtime,
            current_segment: Mutex::new(latest_segment),
        })
    }

    pub fn write(&self, _ctx: &WriteContext, batch: &LogWriteBatch) -> Result<SequenceNumber> {
        // In the WAL based on local storage, we need to ensure the sequence number in
        // segment is monotonically increasing. So we need to acquire a lock here.
        // Perhaps we could avoid acquiring the lock here and instead allocate the
        // position that needs to be written in the segment, then fill it within
        // spawn_blocking. However, I’m not sure about the correctness of this approach.
        let mut current_segment = self.current_segment.lock().unwrap();

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

        let guard = current_segment.lock().unwrap();

        // Check if the current segment has enough space for the new data
        // If not, create a new segment and update current_segment
        if guard.current_size + data.len() as u64 > guard.segment_size {
            let new_segment_id = guard.id + 1;
            // We need to drop guard to allow the update of current_segment
            drop(guard);

            // Create a new segment
            let new_segment = Segment::new(
                format!("{}/segment_{}.wal", self.region_dir, new_segment_id),
                new_segment_id,
                self.segment_size,
            )?;
            let new_segment = Arc::new(Mutex::new(new_segment));
            self.segment_manager.add_segment(new_segment_id, new_segment.clone())?;

            // Update current segment
            *current_segment = new_segment;
        } else {
            drop(guard);
        }

        let mut guard = current_segment.lock().unwrap();

        // Open the segment if not opened
        self.segment_manager.open_segment(&mut guard)?;
        for pos in record_position.iter_mut() {
            pos.start += guard.current_size;
            pos.end += guard.current_size;
        }

        // Append logs to segment file
        guard.append_records(
            &data,
            &mut record_position,
            prev_sequence_num,
            next_sequence_num - 1,
        )?;
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

        let iter = MultiSegmentLogIterator::new(
            self.segment_manager.clone(),
            self.log_encoding.clone(),
            self.record_encoding.clone(),
            Some(req.location.table_id),
            start,
            end,
        )?;

        Ok(BatchLogIteratorAdapter::new_with_sync(
            Box::new(iter),
            self.runtime.clone(),
            ctx.batch_size,
        ))
    }

    pub fn scan(&self, ctx: &ScanContext, _req: &ScanRequest) -> Result<BatchLogIteratorAdapter> {
        let iter = MultiSegmentLogIterator::new(
            self.segment_manager.clone(),
            self.log_encoding.clone(),
            self.record_encoding.clone(),
            None,
            MIN_SEQUENCE_NUMBER,
            MAX_SEQUENCE_NUMBER,
        )?;
        Ok(BatchLogIteratorAdapter::new_with_sync(
            Box::new(iter),
            self.runtime.clone(),
            ctx.batch_size,
        ))
    }

    pub fn mark_delete_entries_up_to(
        &self,
        location: WalLocation,
        sequence_num: SequenceNumber,
    ) -> Result<()> {
        self.segment_manager
            .mark_delete_entries_up_to(location, sequence_num)
    }

    pub fn close(&self) -> Result<()> {
        self.segment_manager.close_all()
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
    segment_size: u64,
    runtime: Arc<Runtime>,
}

impl RegionManager {
    // Create a RegionManager, and scans all the region folders located under
    // root_dir.
    pub fn new(
        root_dir: String,
        cache_size: usize,
        segment_size: u64,
        runtime: Arc<Runtime>,
    ) -> Result<Self> {
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
                segment_size,
                path.to_string_lossy().to_string(),
                runtime.clone(),
            )?;
            regions.insert(region_id, Arc::new(region));
        }

        Ok(Self {
            root_dir,
            regions: Mutex::new(regions),
            cache_size,
            segment_size,
            runtime,
        })
    }

    /// Retrieve a region by its `region_id`. If the region does not exist,
    /// create a new one.
    fn get_region(&self, region_id: RegionId) -> Result<Arc<Region>> {
        let mut regions = self.regions.lock().unwrap();
        if let Some(region) = regions.get(&region_id) {
            return Ok(region.clone());
        }

        let region_dir = Path::new(&self.root_dir).join(region_id.to_string());
        fs::create_dir_all(&region_dir).context(DirOpen)?;

        let region = Region::new(
            region_id,
            self.cache_size,
            self.segment_size,
            region_dir.to_string_lossy().to_string(),
            self.runtime.clone(),
        )?;

        Ok(regions.entry(region_id).or_insert(Arc::new(region)).clone())
    }

    pub fn write(&self, ctx: &WriteContext, batch: &LogWriteBatch) -> Result<SequenceNumber> {
        let region = self.get_region(batch.location.region_id)?;
        region.write(ctx, batch)
    }

    pub fn read(&self, ctx: &ReadContext, req: &ReadRequest) -> Result<BatchLogIteratorAdapter> {
        let region = self.get_region(req.location.region_id)?;
        region.read(ctx, req)
    }

    pub fn scan(&self, ctx: &ScanContext, req: &ScanRequest) -> Result<BatchLogIteratorAdapter> {
        let region = self.get_region(req.region_id)?;
        region.scan(ctx, req)
    }

    pub fn mark_delete_entries_up_to(
        &self,
        location: WalLocation,
        sequence_num: SequenceNumber,
    ) -> Result<()> {
        let region = self.get_region(location.region_id)?;
        region.mark_delete_entries_up_to(location, sequence_num)
    }

    pub fn close(&self, region_id: RegionId) -> Result<()> {
        let region = self.get_region(region_id)?;
        region.close()
    }

    pub fn close_all(&self) -> Result<()> {
        for region in self.regions.lock().unwrap().values() {
            region.close()?;
        }
        Ok(())
    }

    pub fn sequence_num(&self, location: WalLocation) -> Result<SequenceNumber> {
        let region = self.get_region(location.region_id)?;
        region.sequence_num(location)
    }
}

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
    pub fn new(
        log_encoding: CommonLogEncoding,
        record_encoding: RecordEncoding,
        segment: Arc<Mutex<Segment>>,
        segment_manager: Arc<SegmentManager>,
        table_id: Option<TableId>,
        start: SequenceNumber,
        end: SequenceNumber,
    ) -> Result<Self> {
        // Open the segment if it is not open
        let mut segment = segment.lock().unwrap();
        if !segment.is_open() {
            segment_manager.open_segment(&mut segment)?;
        }

        // Read the entire content of the segment
        let segment_content = segment.read(0, segment.current_size)?;

        // Get record positions
        let record_positions = segment.record_position.clone();

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

    fn next(&mut self) -> Result<Option<LogEntry<Vec<u8>>>> {
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
            let record = self
                .record_encoding
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
            let value = self
                .log_encoding
                .decode_value(record.value)
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
    /// Segment manager that contains all segments involved in this read
    /// operation.
    segment_manager: Arc<SegmentManager>,

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
    pub fn new(
        segment_manager: Arc<SegmentManager>,
        log_encoding: CommonLogEncoding,
        record_encoding: RecordEncoding,
        table_id: Option<TableId>,
        start: SequenceNumber,
        end: SequenceNumber,
    ) -> Result<Self> {
        // Find all segments that contain the requested sequence numbers
        let mut relevant_segments = Vec::new();

        {
            let all_segments = segment_manager.all_segments.lock().unwrap();

            for (_, segment) in all_segments.iter() {
                let segment = segment.lock().unwrap();
                if segment.min_seq <= end && segment.max_seq >= start {
                    relevant_segments.push(segment.id);
                }
            }
        }

        // Sort by segment id
        relevant_segments.sort_unstable();

        let mut iter = Self {
            segment_manager,
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
        iter.load_next_segment_iterator()?;

        Ok(iter)
    }

    fn load_next_segment_iterator(&mut self) -> Result<bool> {
        if self.current_segment_idx >= self.segments.len() {
            self.current_iterator = None;
            return Ok(false);
        }

        let segment = self.segments[self.current_segment_idx];
        let segment = self.segment_manager.get_segment(segment)?;
        let iterator = SegmentLogIterator::new(
            self.log_encoding.clone(),
            self.record_encoding.clone(),
            segment,
            self.segment_manager.clone(),
            self.table_id,
            self.start,
            self.end,
        )?;

        self.current_iterator = Some(iterator);
        self.current_segment_idx += 1;

        Ok(true)
    }

    fn next(&mut self) -> Result<Option<LogEntry<&'_ [u8]>>> {
        loop {
            if let Some(ref mut iterator) = self.current_iterator {
                if let Some(entry) = iterator.next()? {
                    self.current_payload = entry.payload.to_owned();
                    return Ok(Some(LogEntry {
                        table_id: entry.table_id,
                        sequence: entry.sequence,
                        payload: &self.current_payload,
                    }));
                }
            }
            if !self.load_next_segment_iterator()? {
                return Ok(None);
            }
        }
    }
}

impl SyncLogIterator for MultiSegmentLogIterator {
    fn next_log_entry(&mut self) -> crate::manager::Result<Option<LogEntry<&'_ [u8]>>> {
        self.next().box_err().context(Read)
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use runtime::Builder;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        kv_encoder::LogBatchEncoder,
        log_batch::{MemoryPayload, MemoryPayloadDecoder},
        manager::ReadBoundary,
    };

    const SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

    #[test]
    fn test_segment_creation() {
        let dir = tempdir().unwrap();
        let path = dir
            .path()
            .join("segment_0.wal")
            .to_str()
            .unwrap()
            .to_string();

        let segment = Segment::new(path.clone(), 0, SEGMENT_SIZE);
        assert!(segment.is_ok());

        let segment = segment.unwrap();
        assert_eq!(segment.version, NEWEST_WAL_SEGMENT_VERSION);
        assert_eq!(segment.path, path);
        assert_eq!(segment.id, 0);
        assert_eq!(segment.current_size, SEGMENT_HEADER.len() as u64);

        let segment_content = fs::read(path).unwrap();
        assert_eq!(segment_content[0], NEWEST_WAL_SEGMENT_VERSION);
        assert_eq!(
            &segment_content[VERSION_SIZE..VERSION_SIZE + SEGMENT_HEADER.len()],
            SEGMENT_HEADER
        );
    }

    #[test]
    fn test_segment_open() {
        let dir = tempdir().unwrap();
        let path = dir
            .path()
            .join("segment_0.wal")
            .to_str()
            .unwrap()
            .to_string();
        let mut segment = Segment::new(path.clone(), 0, SEGMENT_SIZE).unwrap();

        segment
            .open()
            .expect("Expected to open segment successfully");
    }

    #[test]
    fn test_segment_append_and_read() {
        let dir = tempdir().unwrap();
        let path = dir
            .path()
            .join("segment_0.wal")
            .to_str()
            .unwrap()
            .to_string();
        let mut segment = Segment::new(path.clone(), 0, SEGMENT_SIZE).unwrap();
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

    #[test]
    fn test_region_create_and_close() {
        let dir = tempdir().unwrap();
        let runtime = Arc::new(Builder::default().build().unwrap());

        let region = Region::new(
            1,
            1,
            SEGMENT_SIZE,
            dir.path().to_str().unwrap().to_string(),
            runtime,
        )
            .unwrap();

        let _segment = region.segment_manager.get_segment(0).unwrap();

        region.close().unwrap()
    }

    #[test]
    fn test_region_manager_create_and_close() {
        let dir = tempdir().unwrap();
        let runtime = Arc::new(Builder::default().build().unwrap());

        let region_manager = RegionManager::new(
            dir.path().to_str().unwrap().to_string(),
            1,
            SEGMENT_SIZE,
            runtime,
        )
            .unwrap();

        region_manager.close_all().unwrap();
    }

    async fn test_multi_segment_write_and_read_inner(runtime: Arc<Runtime>) {
        // Set a small max segment size
        const SEGMENT_SIZE: u64 = 4096;

        // Create a temporary directory
        let dir = tempdir().unwrap();

        // Create a new Region
        let region = Region::new(
            1,
            2,
            SEGMENT_SIZE,
            dir.path().to_str().unwrap().to_string(),
            runtime.clone(),
        )
            .unwrap();
        let region = Arc::new(region);

        // Write data
        let mut expected_entries = Vec::new();
        let mut sequence = MIN_SEQUENCE_NUMBER + 1;
        let location = WalLocation::new(1, 1);
        for _i in 0..10 {
            // Write 10 batches, 100 entries each
            let log_entries = 0..100;
            let log_batch_encoder = LogBatchEncoder::create(location);
            let log_batch = log_batch_encoder
                .encode_batch(log_entries.clone().map(|v| MemoryPayload { val: v }))
                .expect("should succeed to encode payloads");
            for j in log_entries {
                let payload = MemoryPayload { val: j };
                expected_entries.push(LogEntry {
                    table_id: 1,
                    sequence,
                    payload,
                });
                sequence += 1;
            }

            let write_ctx = WriteContext::default();
            region.write(&write_ctx, &log_batch).unwrap();
        }

        // Read data
        let read_ctx = ReadContext {
            timeout: Duration::from_secs(5),
            batch_size: 1000,
        };
        let read_req = ReadRequest {
            location: WalLocation::new(1, 1),
            start: ReadBoundary::Min,
            end: ReadBoundary::Max,
        };
        let mut iterator = region.read(&read_ctx, &read_req).unwrap();

        // Collect read entries
        let dec = MemoryPayloadDecoder;
        let read_entries = iterator
            .next_log_entries(dec, |_| true, VecDeque::new())
            .await
            .unwrap();

        // Verify that read data matches written data
        assert_eq!(expected_entries.len(), read_entries.len());

        for (expected, actual) in expected_entries.iter().zip(read_entries.iter()) {
            assert_eq!(expected.table_id, actual.table_id);
            assert_eq!(expected.sequence, actual.sequence);
            assert_eq!(expected.payload, actual.payload);
        }

        {
            // Verify that multiple segments were created
            let all_segments = region.segment_manager.all_segments.lock().unwrap();
            assert!(
                all_segments.len() > 1,
                "Expected multiple segments, but got {}",
                all_segments.len()
            );
        }

        region.close().unwrap()
    }

    #[test]
    fn test_multi_segment_write_and_read() {
        let runtime = Arc::new(Builder::default().build().unwrap());
        runtime.block_on(test_multi_segment_write_and_read_inner(runtime.clone()));
    }
}
