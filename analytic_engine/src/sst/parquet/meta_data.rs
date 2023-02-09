// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// MetaData for SST based on parquet.

use std::{fmt, ops::Index, sync::Arc};

use bytes::Bytes;
use common_types::{schema::Schema, time::TimeRange, SequenceNumber};
use common_util::define_result;
use proto::{common as common_pb, sst as sst_pb};
use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};
use xorfilter::{Xor8, Xor8Builder};

use crate::sst::writer::MetaData;

/// Error of sst file.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Time range is not found.\nBacktrace\n:{}", backtrace))]
    TimeRangeNotFound { backtrace: Backtrace },

    #[snafu(display("Table schema is not found.\nBacktrace\n:{}", backtrace))]
    TableSchemaNotFound { backtrace: Backtrace },

    #[snafu(display(
        "Failed to parse Xor8Filter from bytes, err:{}.\nBacktrace\n:{}",
        source,
        backtrace
    ))]
    ParseXor8Filter {
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to build Xor8Filter, err:{}.\nBacktrace\n:{}",
        source,
        backtrace
    ))]
    BuildXor8Filter {
        source: xorfilter::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Unsupported sst_filter version, version:{}.\nBacktrace\n:{}",
        version,
        backtrace
    ))]
    UnsupportedSstFilter { version: u32, backtrace: Backtrace },

    #[snafu(display("Failed to convert time range, err:{}", source))]
    ConvertTimeRange { source: common_types::time::Error },

    #[snafu(display("Failed to convert table schema, err:{}", source))]
    ConvertTableSchema { source: common_types::schema::Error },
}

define_result!(Error);

const DEFAULT_FILTER_VERSION: u32 = 0;

/// Filter can be used to test whether an element is a member of a set.
/// False positive matches are possible if space-efficient probabilistic data
/// structure are used.
trait Filter: fmt::Debug {
    /// Check the key is in the bitmap index.
    fn contains(&self, key: &[u8]) -> bool;

    /// Serialize the bitmap index to binary array.
    fn to_bytes(&self) -> Vec<u8>;

    /// Deserialize the binary array to bitmap index.
    fn from_bytes(buf: Vec<u8>) -> Result<Self>
    where
        Self: Sized;
}

/// Filter based on https://docs.rs/xorfilter-rs/latest/xorfilter/struct.Xor8.html
#[derive(Default)]
struct Xor8Filter {
    xor8: Xor8,
}

impl fmt::Debug for Xor8Filter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("XorFilter")
    }
}

impl Filter for Xor8Filter {
    fn contains(&self, key: &[u8]) -> bool {
        self.xor8.contains(key)
    }

    fn to_bytes(&self) -> Vec<u8> {
        self.xor8.to_bytes()
    }

    fn from_bytes(buf: Vec<u8>) -> Result<Self>
    where
        Self: Sized,
    {
        Xor8::from_bytes(buf)
            .context(ParseXor8Filter)
            .map(|xor8| Self { xor8 })
    }
}

pub struct RowGroupFilterBuilder {
    builders: Vec<Option<Xor8Builder>>,
}

impl RowGroupFilterBuilder {
    pub(crate) fn with_num_columns(num_col: usize) -> Self {
        Self {
            builders: vec![None; num_col],
        }
    }

    pub(crate) fn add_key(&mut self, col_idx: usize, key: &[u8]) {
        if let Some(b) = self.builders[col_idx].as_mut() {
            b.insert(key)
        }
    }

    pub(crate) fn build(self) -> Result<RowGroupFilter> {
        self.builders
            .into_iter()
            .map(|b| {
                if let Some(mut b) = b {
                    Some(
                        b.build()
                            .context(BuildXor8Filter)
                            .map(|xor8| Box::new(Xor8Filter { xor8 }) as Box<_>),
                    )
                    .transpose()
                } else {
                    Ok(None)
                }
            })
            .collect::<Result<Vec<_>>>()
            .map(|column_filters| RowGroupFilter { column_filters })
    }
}

#[derive(Debug, Default)]
pub struct RowGroupFilter {
    // The column filter can be None if the column is not indexed.
    column_filters: Vec<Option<Box<dyn Filter + Send + Sync>>>,
}

impl PartialEq for RowGroupFilter {
    fn eq(&self, other: &Self) -> bool {
        if self.column_filters.len() != other.column_filters.len() {
            return false;
        }

        for (a, b) in self.column_filters.iter().zip(other.column_filters.iter()) {
            if !a
                .as_ref()
                .map(|a| a.to_bytes())
                .eq(&b.as_ref().map(|b| b.to_bytes()))
            {
                return false;
            }
        }

        true
    }
}

impl Clone for RowGroupFilter {
    fn clone(&self) -> Self {
        let column_filters = self
            .column_filters
            .iter()
            .map(|f| {
                f.as_ref()
                    .map(|f| Box::new(Xor8Filter::from_bytes(f.to_bytes()).unwrap()) as Box<_>)
            })
            .collect();

        Self { column_filters }
    }
}

impl RowGroupFilter {
    /// Return None if the column is not indexed.
    pub fn contains_column_data(&self, column_idx: usize, data: &[u8]) -> Option<bool> {
        self.column_filters[column_idx]
            .as_ref()
            .map(|v| v.contains(data))
    }
}

// TODO: move this to sst module
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SstFilter {
    /// Every filter is a row group filter consists of column filters.
    row_group_filters: Vec<RowGroupFilter>,
}

impl SstFilter {
    pub fn new(row_group_filters: Vec<RowGroupFilter>) -> Self {
        Self { row_group_filters }
    }

    pub fn len(&self) -> usize {
        self.row_group_filters.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn row_group_filters(&self) -> &[RowGroupFilter] {
        &self.row_group_filters
    }
}

impl Index<usize> for SstFilter {
    type Output = RowGroupFilter;

    fn index(&self, index: usize) -> &Self::Output {
        &self.row_group_filters[index]
    }
}

impl From<SstFilter> for sst_pb::SstFilter {
    fn from(sst_filter: SstFilter) -> Self {
        let row_group_filters = sst_filter
            .row_group_filters
            .into_iter()
            .map(|row_group_filter| {
                let column_filters = row_group_filter
                    .column_filters
                    .into_iter()
                    .map(|column_filter| {
                        column_filter
                            .map(|v| v.to_bytes())
                            // If the column filter does not exist, use an empty vector for it.
                            .unwrap_or_default()
                    })
                    .collect::<Vec<_>>();
                sst_pb::sst_filter::RowGroupFilter { column_filters }
            })
            .collect::<Vec<_>>();

        sst_pb::SstFilter {
            version: DEFAULT_FILTER_VERSION,
            row_group_filters,
        }
    }
}

impl TryFrom<sst_pb::SstFilter> for SstFilter {
    type Error = Error;

    fn try_from(src: sst_pb::SstFilter) -> Result<Self> {
        ensure!(
            src.version == DEFAULT_FILTER_VERSION,
            UnsupportedSstFilter {
                version: src.version
            }
        );

        let row_group_filters = src
            .row_group_filters
            .into_iter()
            .map(|row_group_filter| {
                let column_filters = row_group_filter
                    .column_filters
                    .into_iter()
                    .map(|encoded_bytes| {
                        if encoded_bytes.is_empty() {
                            Ok(None)
                        } else {
                            Some(
                                Xor8Filter::from_bytes(encoded_bytes)
                                    .map(|e| Box::new(e) as Box<_>),
                            )
                            .transpose()
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(RowGroupFilter { column_filters })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(SstFilter { row_group_filters })
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
    pub sst_filter: Option<SstFilter>,
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
            sst_filter: None,
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
            // Avoid the messy output from filter.
            .field("has_filter", &self.sst_filter.is_some())
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
            filter: src.sst_filter.map(|v| v.into()),
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
        let sst_filter = src.filter.map(SstFilter::try_from).transpose()?;

        Ok(Self {
            min_key: src.min_key.into(),
            max_key: src.max_key.into(),
            time_range,
            max_sequence: src.max_sequence,
            schema,
            sst_filter,
            collapsible_cols_idx: src.collapsible_cols_idx,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_conversion_sst_filter() {
        let sst_filter = SstFilter {
            row_group_filters: vec![
                RowGroupFilter {
                    column_filters: vec![None, Some(Box::new(Xor8Filter::default()))],
                },
                RowGroupFilter {
                    column_filters: vec![Some(Box::new(Xor8Filter::default())), None],
                },
            ],
        };

        let sst_filter_pb: sst_pb::SstFilter = sst_filter.clone().into();
        assert_eq!(sst_filter_pb.version, DEFAULT_FILTER_VERSION);
        assert_eq!(sst_filter_pb.row_group_filters.len(), 2);
        assert_eq!(sst_filter_pb.row_group_filters[0].column_filters.len(), 2);
        assert_eq!(sst_filter_pb.row_group_filters[1].column_filters.len(), 2);
        assert!(sst_filter_pb.row_group_filters[0].column_filters[0].is_empty());
        assert_eq!(
            sst_filter_pb.row_group_filters[0].column_filters[1].len(),
            24
        );
        assert_eq!(
            sst_filter_pb.row_group_filters[1].column_filters[0].len(),
            24
        );
        assert!(sst_filter_pb.row_group_filters[1].column_filters[1].is_empty());

        let decoded_sst_filter = SstFilter::try_from(sst_filter_pb).unwrap();
        assert_eq!(decoded_sst_filter, sst_filter);
    }
}
