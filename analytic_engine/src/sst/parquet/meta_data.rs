// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// MetaData for SST based on parquet.

use std::{fmt, sync::Arc};

use bytes::Bytes;
use common_types::{schema::Schema, time::TimeRange, SequenceNumber};
use common_util::define_result;
use ethbloom::Bloom;
use proto::{common as common_pb, sst as sst_pb};
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};

use crate::sst::writer::MetaData;

/// Error of sst file.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Time range is not found.\nBacktrace\n:{}", backtrace))]
    TimeRangeNotFound { backtrace: Backtrace },

    #[snafu(display("Table schema is not found.\nBacktrace\n:{}", backtrace))]
    TableSchemaNotFound { backtrace: Backtrace },

    #[snafu(display(
        "Bloom filter should be 256 byte, current:{}.\nBacktrace\n:{}",
        size,
        backtrace
    ))]
    InvalidBloomFilterSize { size: usize, backtrace: Backtrace },

    #[snafu(display("Failed to convert time range, err:{}", source))]
    ConvertTimeRange { source: common_types::time::Error },

    #[snafu(display("Failed to convert table schema, err:{}", source))]
    ConvertTableSchema { source: common_types::schema::Error },
}

define_result!(Error);

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BloomFilter {
    // Two level vector means
    // 1. row group
    // 2. column
    filters: Vec<Vec<Bloom>>,
}

impl BloomFilter {
    pub fn new(filters: Vec<Vec<Bloom>>) -> Self {
        Self { filters }
    }

    #[inline]
    pub fn filters(&self) -> &[Vec<Bloom>] {
        &self.filters
    }
}

impl From<BloomFilter> for sst_pb::SstBloomFilter {
    fn from(bloom_filter: BloomFilter) -> Self {
        let row_group_filters = bloom_filter
            .filters
            .iter()
            .map(|row_group_filter| {
                let column_filters = row_group_filter
                    .iter()
                    .map(|column_filter| column_filter.data().to_vec())
                    .collect::<Vec<_>>();
                sst_pb::sst_bloom_filter::RowGroupFilter { column_filters }
            })
            .collect::<Vec<_>>();

        sst_pb::SstBloomFilter { row_group_filters }
    }
}

impl TryFrom<sst_pb::SstBloomFilter> for BloomFilter {
    type Error = Error;

    fn try_from(src: sst_pb::SstBloomFilter) -> Result<Self> {
        let filters = src
            .row_group_filters
            .into_iter()
            .map(|row_group_filter| {
                row_group_filter
                    .column_filters
                    .into_iter()
                    .map(|encoded_bytes| {
                        let size = encoded_bytes.len();
                        let bs: [u8; 256] = encoded_bytes
                            .try_into()
                            .ok()
                            .context(InvalidBloomFilterSize { size })?;

                        Ok(Bloom::from(bs))
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(BloomFilter { filters })
    }
}

/// Meta data of a sst file
#[derive(Clone, PartialEq)]
pub struct ParquetMetaData {
    pub min_key: Bytes,
    pub max_key: Bytes,
    /// Time Range of the sst
    pub time_range: TimeRange,
    /// Max sequence number in the sst
    pub max_sequence: SequenceNumber,
    pub schema: Schema,
    pub bloom_filter: Option<BloomFilter>,
    pub collapsible_cols_idx: Vec<u32>,
}

pub type ParquetMetaDataRef = Arc<ParquetMetaData>;

impl From<MetaData> for ParquetMetaData {
    fn from(meta: MetaData) -> Self {
        Self {
            min_key: meta.min_key,
            max_key: meta.max_key,
            time_range: meta.time_range,
            max_sequence: meta.max_sequence,
            schema: meta.schema,
            bloom_filter: None,
            collapsible_cols_idx: Vec::new(),
        }
    }
}

impl From<ParquetMetaData> for MetaData {
    fn from(meta: ParquetMetaData) -> Self {
        Self {
            min_key: meta.min_key,
            max_key: meta.max_key,
            time_range: meta.time_range,
            max_sequence: meta.max_sequence,
            schema: meta.schema,
        }
    }
}

impl fmt::Debug for ParquetMetaData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParquetMetaData")
            .field("min_key", &self.min_key)
            .field("max_key", &self.max_key)
            .field("time_range", &self.time_range)
            .field("max_sequence", &self.max_sequence)
            .field("schema", &self.schema)
            // Avoid the messy output from bloom filter.
            .field("has_bloom_filter", &self.bloom_filter.is_some())
            .field("collapsible_cols_idx", &self.collapsible_cols_idx)
            .finish()
    }
}

impl From<ParquetMetaData> for sst_pb::ParquetMetaData {
    fn from(src: ParquetMetaData) -> Self {
        sst_pb::ParquetMetaData {
            min_key: src.min_key.to_vec(),
            max_key: src.max_key.to_vec(),
            max_sequence: src.max_sequence,
            time_range: Some(src.time_range.into()),
            schema: Some(common_pb::TableSchema::from(&src.schema)),
            bloom_filter: src.bloom_filter.map(|v| v.into()),
            collapsible_cols_idx: src.collapsible_cols_idx,
        }
    }
}

impl TryFrom<sst_pb::ParquetMetaData> for ParquetMetaData {
    type Error = Error;

    fn try_from(src: sst_pb::ParquetMetaData) -> Result<Self> {
        let time_range = {
            let time_range = src.time_range.context(TimeRangeNotFound)?;
            TimeRange::try_from(time_range).context(ConvertTimeRange)?
        };
        let schema = {
            let schema = src.schema.context(TableSchemaNotFound)?;
            Schema::try_from(schema).context(ConvertTableSchema)?
        };
        let bloom_filter = src.bloom_filter.map(BloomFilter::try_from).transpose()?;

        Ok(Self {
            min_key: src.min_key.into(),
            max_key: src.max_key.into(),
            time_range,
            max_sequence: src.max_sequence,
            schema,
            bloom_filter,
            collapsible_cols_idx: src.collapsible_cols_idx,
        })
    }
}
