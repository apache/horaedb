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

// TODO: Better module name should be index.

use std::{fmt, ops::Index};

use common_types::{datum::DatumKind, schema::Schema};
use horaedbproto::sst as sst_pb;
use snafu::ResultExt;
use xorfilter::xor8::{Xor8, Xor8Builder};

use crate::sst::parquet::meta_data::{BuildXor8Filter, Error, ParseXor8Filter, Result};

// TODO: move this to sst module, and add a FilterBuild trait
/// Filter can be used to test whether an element is a member of a set.
/// False positive matches are possible if space-efficient probabilistic data
/// structure are used.
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

    /// Deserialize the binary array to specific filter.
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
    pub(crate) fn new(schema: &Schema) -> Self {
        let builders = schema
            .columns()
            .iter()
            .enumerate()
            .map(|(i, col)| {
                // No need to create filter index over the timestamp column.
                if schema.timestamp_index() == i {
                    return None;
                }

                // No need to create filter index over the tsid column.
                if schema.index_of_tsid().map(|idx| idx == i).unwrap_or(false) {
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
        let mut builders = RowGroupFilterBuilder::new(&schema);
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
