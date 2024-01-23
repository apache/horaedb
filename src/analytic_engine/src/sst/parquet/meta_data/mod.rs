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

// MetaData for SST based on parquet.

use std::{collections::HashSet, fmt, sync::Arc};

use bytes_ext::Bytes;
use common_types::{schema::Schema, time::TimeRange, SequenceNumber};
use horaedbproto::{schema as schema_pb, sst as sst_pb};
use macros::define_result;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};

use crate::sst::{parquet::meta_data::filter::ParquetFilter, writer::MetaData};

pub mod filter;

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
    pub column_values: Option<Vec<Option<ColumnValueSet>>>,
}

pub type ParquetMetaDataRef = Arc<ParquetMetaData>;

impl From<&MetaData> for ParquetMetaData {
    fn from(meta: &MetaData) -> Self {
        Self {
            min_key: meta.min_key.clone(),
            max_key: meta.max_key.clone(),
            time_range: meta.time_range,
            max_sequence: meta.max_sequence,
            schema: meta.schema.clone(),
            parquet_filter: None,
            column_values: None,
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
            .field("column_values", &self.column_values)
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
        let column_values = if let Some(v) = src.column_values {
            v.into_iter()
                .map(|col| sst_pb::ColumnValueSet {
                    value: col.map(|col| col.into()),
                })
                .collect()
        } else {
            Vec::new()
        };
        sst_pb::ParquetMetaData {
            min_key: src.min_key.to_vec(),
            max_key: src.max_key.to_vec(),
            max_sequence: src.max_sequence,
            time_range: Some(src.time_range.into()),
            schema: Some(schema_pb::TableSchema::from(&src.schema)),
            filter: src.parquet_filter.map(|v| v.into()),
            // collapsible_cols_idx is used in hybrid format ,and it's deprecated.
            collapsible_cols_idx: Vec::new(),
            column_values,
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
        let column_values = if src.column_values.is_empty() {
            // Old version sst don't has this, so set to none.
            None
        } else {
            Some(
                src.column_values
                    .into_iter()
                    .map(|v| v.value.map(|v| v.into()))
                    .collect(),
            )
        };

        Ok(Self {
            min_key: src.min_key.into(),
            max_key: src.max_key.into(),
            time_range,
            max_sequence: src.max_sequence,
            schema,
            parquet_filter,
            column_values,
        })
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum ColumnValueSet {
    StringValue(HashSet<String>),
}

impl ColumnValueSet {
    pub fn is_empty(&self) -> bool {
        match self {
            Self::StringValue(sv) => sv.is_empty(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::StringValue(sv) => sv.len(),
        }
    }
}

impl From<ColumnValueSet> for sst_pb::column_value_set::Value {
    fn from(value: ColumnValueSet) -> Self {
        match value {
            ColumnValueSet::StringValue(values) => {
                let values = values.into_iter().collect();
                sst_pb::column_value_set::Value::StringSet(sst_pb::column_value_set::StringSet {
                    values,
                })
            }
        }
    }
}

impl From<sst_pb::column_value_set::Value> for ColumnValueSet {
    fn from(value: sst_pb::column_value_set::Value) -> Self {
        match value {
            sst_pb::column_value_set::Value::StringSet(ss) => {
                ColumnValueSet::StringValue(HashSet::from_iter(ss.values))
            }
        }
    }
}
