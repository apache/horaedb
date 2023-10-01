// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// MetaData for SST based on parquet.

use std::{collections::HashSet, fmt, ops::Index, sync::Arc};

use bytes_ext::Bytes;
use ceresdbproto::{schema as schema_pb, sst as sst_pb};
use common_types::{
    datum::DatumKind,
    schema::{RecordSchemaWithKey, Schema},
    time::TimeRange,
    SequenceNumber,
};
use macros::define_result;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use xorfilter::xor8::{Xor8, Xor8Builder};

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

    #[snafu(display("Failed to convert time range, err:{}", source))]
    ConvertTimeRange { source: common_types::time::Error },

    #[snafu(display("Failed to convert table schema, err:{}", source))]
    ConvertTableSchema { source: common_types::schema::Error },
}

define_result!(Error);

/// Filter can be used to test whether an element is a member of a set.
/// False positive matches are possible if space-efficient probabilistic data
/// structure are used.
// TODO: move this to sst module, and add a FilterBuild trait
trait Filter: fmt::Debug {
    fn r#type(&self) -> FilterType;

    /// Check the key is in the bitmap index.
    fn contains(&self, key: &[u8]) -> bool;

    /// Serialize the bitmap index to binary array.
    fn to_bytes(&self) -> Vec<u8>;

    /// Serialized size
    fn size(&self) -> usize {
        self.to_bytes().len()
    }

    /// Deserialize the binary array to bitmap index.
    fn from_bytes(buf: Vec<u8>) -> Result<Self>
    where
        Self: Sized;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilterType {
    Xor8,
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
    fn r#type(&self) -> FilterType {
        FilterType::Xor8
    }

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
    pub(crate) fn new(record_schema: &RecordSchemaWithKey) -> Self {
        let builders = record_schema
            .columns()
            .iter()
            .enumerate()
            .map(|(i, col)| {
                if record_schema.is_primary_key_index(i) {
                    return None;
                }

                if matches!(
                    col.data_type,
                    DatumKind::Null
                        | DatumKind::Double
                        | DatumKind::Float
                        | DatumKind::Varbinary
                        | DatumKind::Boolean
                ) {
                    return None;
                }

                Some(Xor8Builder::default())
            })
            .collect();

        Self { builders }
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
                b.map(|mut b| {
                    b.build()
                        .context(BuildXor8Filter)
                        .map(|xor8| Box::new(Xor8Filter { xor8 }) as _)
                })
                .transpose()
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

    fn size(&self) -> usize {
        self.column_filters
            .iter()
            .map(|cf| cf.as_ref().map(|cf| cf.size()).unwrap_or(0))
            .sum()
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ParquetFilter {
    /// Every filter is a row group filter consists of column filters.
    row_group_filters: Vec<RowGroupFilter>,
}

impl ParquetFilter {
    pub fn push_row_group_filter(&mut self, row_group_filter: RowGroupFilter) {
        self.row_group_filters.push(row_group_filter);
    }

    pub fn len(&self) -> usize {
        self.row_group_filters.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn size(&self) -> usize {
        self.row_group_filters.iter().map(|f| f.size()).sum()
    }
}

impl Index<usize> for ParquetFilter {
    type Output = RowGroupFilter;

    fn index(&self, index: usize) -> &Self::Output {
        &self.row_group_filters[index]
    }
}

impl From<ParquetFilter> for sst_pb::ParquetFilter {
    fn from(parquet_filter: ParquetFilter) -> Self {
        let row_group_filters = parquet_filter
            .row_group_filters
            .into_iter()
            .map(|row_group_filter| {
                let column_filters = row_group_filter
                    .column_filters
                    .into_iter()
                    .map(|column_filter| match column_filter {
                        Some(v) => {
                            let encoded_filter = v.to_bytes();
                            match v.r#type() {
                                FilterType::Xor8 => sst_pb::ColumnFilter {
                                    filter: Some(sst_pb::column_filter::Filter::Xor(
                                        encoded_filter,
                                    )),
                                },
                            }
                        }
                        None => sst_pb::ColumnFilter { filter: None },
                    })
                    .collect::<Vec<_>>();

                sst_pb::RowGroupFilter { column_filters }
            })
            .collect::<Vec<_>>();

        sst_pb::ParquetFilter { row_group_filters }
    }
}

impl TryFrom<sst_pb::ParquetFilter> for ParquetFilter {
    type Error = Error;

    fn try_from(src: sst_pb::ParquetFilter) -> Result<Self> {
        let row_group_filters = src
            .row_group_filters
            .into_iter()
            .map(|row_group_filter| {
                let column_filters = row_group_filter
                    .column_filters
                    .into_iter()
                    .map(|column_filter| match column_filter.filter {
                        Some(v) => match v {
                            sst_pb::column_filter::Filter::Xor(encoded_bytes) => {
                                Xor8Filter::from_bytes(encoded_bytes)
                                    .map(|v| Some(Box::new(v) as _))
                            }
                        },
                        None => Ok(None),
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(RowGroupFilter { column_filters })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(ParquetFilter { row_group_filters })
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
    pub parquet_filter: Option<ParquetFilter>,
    pub column_values: Vec<Option<ColumnValue>>,
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
            parquet_filter: None,
            column_values: Vec::new(),
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

impl From<Arc<ParquetMetaData>> for MetaData {
    fn from(meta: Arc<ParquetMetaData>) -> Self {
        Self {
            min_key: meta.min_key.clone(),
            max_key: meta.max_key.clone(),
            time_range: meta.time_range,
            max_sequence: meta.max_sequence,
            schema: meta.schema.clone(),
        }
    }
}

impl fmt::Debug for ParquetMetaData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParquetMetaData")
            .field("min_key", &hex::encode(&self.min_key))
            .field("max_key", &hex::encode(&self.max_key))
            .field("time_range", &self.time_range)
            .field("max_sequence", &self.max_sequence)
            .field("schema", &self.schema)
            .field(
                "filter_size",
                &self
                    .parquet_filter
                    .as_ref()
                    .map(|filter| filter.size())
                    .unwrap_or(0),
            )
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
            schema: Some(schema_pb::TableSchema::from(&src.schema)),
            filter: src.parquet_filter.map(|v| v.into()),
            // collapsible_cols_idx is used in hybrid format ,and it's deprecated.
            collapsible_cols_idx: Vec::new(),
            column_values: src
                .column_values
                .into_iter()
                .map(|col| sst_pb::ColumnValue {
                    value: col.map(|col| col.into()),
                })
                .collect(),
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
        let parquet_filter = src.filter.map(ParquetFilter::try_from).transpose()?;

        Ok(Self {
            min_key: src.min_key.into(),
            max_key: src.max_key.into(),
            time_range,
            max_sequence: src.max_sequence,
            schema,
            parquet_filter,
            column_values: src
                .column_values
                .into_iter()
                .map(|v| v.value.map(|v| v.into()))
                .collect(),
        })
    }
}

#[derive(PartialEq, Clone)]
pub enum ColumnValue {
    StringValue(HashSet<String>),
}

impl From<ColumnValue> for sst_pb::column_value::Value {
    fn from(value: ColumnValue) -> Self {
        match value {
            ColumnValue::StringValue(values) => {
                let values = values.into_iter().collect();
                sst_pb::column_value::Value::StringSet(sst_pb::column_value::StringSet { values })
            }
        }
    }
}

impl From<sst_pb::column_value::Value> for ColumnValue {
    fn from(value: sst_pb::column_value::Value) -> Self {
        match value {
            sst_pb::column_value::Value::StringSet(ss) => {
                ColumnValue::StringValue(HashSet::from_iter(ss.values))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use common_types::tests::build_schema;

    use super::*;

    #[test]
    fn test_conversion_parquet_filter() {
        let parquet_filter = ParquetFilter {
            row_group_filters: vec![
                RowGroupFilter {
                    column_filters: vec![None, Some(Box::<Xor8Filter>::default() as _)],
                },
                RowGroupFilter {
                    column_filters: vec![Some(Box::<Xor8Filter>::default() as _), None],
                },
            ],
        };

        let parquet_filter_pb: sst_pb::ParquetFilter = parquet_filter.clone().into();
        assert_eq!(parquet_filter_pb.row_group_filters.len(), 2);
        assert_eq!(
            parquet_filter_pb.row_group_filters[0].column_filters.len(),
            2
        );
        assert_eq!(
            parquet_filter_pb.row_group_filters[1].column_filters.len(),
            2
        );
        assert!(parquet_filter_pb.row_group_filters[0].column_filters[0]
            .filter
            .is_none());
        assert!(parquet_filter_pb.row_group_filters[0].column_filters[1]
            .filter
            .is_some(),);
        assert!(parquet_filter_pb.row_group_filters[1].column_filters[0]
            .filter
            .is_some(),);
        assert!(parquet_filter_pb.row_group_filters[1].column_filters[1]
            .filter
            .is_none());

        let decoded_parquet_filter = ParquetFilter::try_from(parquet_filter_pb).unwrap();
        assert_eq!(decoded_parquet_filter, parquet_filter);
    }

    #[test]
    fn test_row_group_filter_builder() {
        // (key1(varbinary), key2(timestamp), field1(double), field2(string))
        let schema = build_schema();
        let record_schema = schema.to_record_schema_with_key();
        let mut builders = RowGroupFilterBuilder::new(&record_schema);
        for key in ["host-123", "host-456", "host-789"] {
            builders.add_key(3, key.as_bytes());
        }
        let row_group_filter = builders.build().unwrap();
        for i in 0..3 {
            assert!(row_group_filter.column_filters[i].is_none());
        }

        let testcase = [("host-123", true), ("host-321", false)];
        for (key, expected) in testcase {
            let actual = row_group_filter
                .contains_column_data(3, key.as_bytes())
                .unwrap();

            assert_eq!(expected, actual);
        }
    }
}
