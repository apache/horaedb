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

//! Partition rule datafusion adapter

use common_types::{row::RowGroup, schema::Schema};
use datafusion::logical_expr::Expr;

use self::extractor::{KeyExtractor, NoopExtractor};
use super::PartitionedRowGroup;
use crate::partition::{
    rule::{
        df_adapter::extractor::FilterExtractorRef, factory::PartitionRuleFactory, PartitionRuleRef,
    },
    BuildPartitionRule, PartitionInfo, Result,
};

mod extractor;

/// Partition rule's adapter for datafusion
pub struct DfPartitionRuleAdapter {
    /// Partition rule
    rule: PartitionRuleRef,

    /// `PartitionFilter` extractor for datafusion `Expr`
    extractor: FilterExtractorRef,
}

impl DfPartitionRuleAdapter {
    pub fn new(partition_info: PartitionInfo, schema: &Schema) -> Result<Self> {
        let extractor = Self::create_extractor(&partition_info)?;
        let rule = PartitionRuleFactory::create(partition_info, schema)?;

        Ok(Self { rule, extractor })
    }

    pub fn columns(&self) -> &[String] {
        self.rule.columns()
    }

    pub fn locate_partitions_for_write(&self, row_group: RowGroup) -> Result<PartitionedRowGroup> {
        self.rule.locate_partitions_for_write(row_group)
    }

    pub fn locate_partitions_for_read(&self, filters: &[Expr]) -> Result<Vec<usize>> {
        // Extract partition filters from datafusion filters.
        let partition_filters = self.extractor.extract(filters, self.columns());

        // Locate partitions from filters.
        self.rule.locate_partitions_for_read(&partition_filters)
    }

    fn create_extractor(partition_info: &PartitionInfo) -> Result<FilterExtractorRef> {
        match partition_info {
            PartitionInfo::Key(_) => Ok(Box::new(KeyExtractor)),
            PartitionInfo::Hash(_) => BuildPartitionRule {
                msg: format!("unsupported partition strategy, strategy:{partition_info:?}"),
            }
            .fail(),
            PartitionInfo::Random(_) => Ok(Box::new(NoopExtractor)),
        }
    }
}

#[cfg(test)]
mod tests {
    use common_types::{
        column_schema,
        datum::{Datum, DatumKind},
        row::RowGroupBuilder,
        schema::{Builder, Schema, TSID_COLUMN},
        string::StringBytes,
        time::Timestamp,
    };
    use datafusion::logical_expr::{col, lit};
    use itertools::Itertools;

    use super::*;
    use crate::partition::{
        rule::key::{compute_partition, DEFAULT_PARTITION_VERSION},
        KeyPartitionInfo, PartitionDefinition,
    };

    // TODO: this test maybe not reasonable to place here.
    #[test]
    fn test_locate_partitions_for_read() {
        let schema = build_schema();
        let partition_num = 16;
        let filter1 = col("col1").eq(lit(1_i32));
        let filter2 = col("col2").eq(lit("test".to_string()));
        let filter3 = col("col3").eq(lit(42_u64));
        let filter4 = col("col1").eq(lit(3_i32));
        let valid_filters_1 = vec![filter1.clone(), filter2.clone(), filter3.clone()];
        let valid_filters_2 = vec![filter1, filter2, filter3, filter4];
        let ket_partition = KeyPartitionInfo {
            version: DEFAULT_PARTITION_VERSION,
            definitions: vec![PartitionDefinition::default(); partition_num],
            partition_key: vec!["col1".to_string(), "col2".to_string(), "col3".to_string()],
            linear: false,
        };

        // Basic flow
        let key_rule_adapter =
            DfPartitionRuleAdapter::new(PartitionInfo::Key(ket_partition), &schema).unwrap();
        let partitions = key_rule_adapter
            .locate_partitions_for_read(&valid_filters_1)
            .unwrap();

        let partition_keys = vec![
            Datum::Int32(1),
            Datum::String(StringBytes::from("test")),
            Datum::UInt64(42),
        ];
        let partition_key_refs = partition_keys.iter().map(Datum::as_view);
        let expected = compute_partition(partition_key_refs, partition_num);

        assert_eq!(partitions[0], expected);

        // Conflict filter and empty partitions
        let partitions = key_rule_adapter
            .locate_partitions_for_read(&valid_filters_2)
            .unwrap();

        assert!(partitions.is_empty());
    }

    // TODO: this test maybe not reasonable to place here.
    #[test]
    fn test_locate_partitions_for_read_invalid() {
        let schema = build_schema();
        let partition_num = 16;
        let filter1 = col("col1").eq(lit(1_i32));
        let filter2 = col("col2").eq(lit("test".to_string()));
        let filter3 = col("col3").gt(lit(42_u64));
        let filter4 = col("col4").eq(lit(42_u64));

        let invalid_filters_1 = vec![filter1.clone(), filter2.clone(), filter3];
        let invalid_filters_2 = vec![filter1, filter2, filter4];
        let ket_partition = KeyPartitionInfo {
            version: DEFAULT_PARTITION_VERSION,
            definitions: vec![PartitionDefinition::default(); partition_num],
            partition_key: vec!["col1".to_string(), "col2".to_string(), "col3".to_string()],
            linear: false,
        };

        // Locate for invalid filters
        let key_rule_adapter =
            DfPartitionRuleAdapter::new(PartitionInfo::Key(ket_partition), &schema).unwrap();

        // Partitions located from invalid filters.
        let partitions_1 = key_rule_adapter
            .locate_partitions_for_read(&invalid_filters_1)
            .unwrap();
        let partitions_2 = key_rule_adapter
            .locate_partitions_for_read(&invalid_filters_2)
            .unwrap();

        // Expected
        let all_partitions = (0..partition_num).collect::<Vec<_>>();
        assert_eq!(partitions_1, all_partitions);
        assert_eq!(partitions_2, all_partitions);
    }

    // TODO: this test maybe not reasonable to place here.
    #[test]
    fn test_locate_partitions_for_write() {
        // Basic flow
        let schema = build_schema();
        let partition_num = 16;
        let ket_partition = KeyPartitionInfo {
            version: DEFAULT_PARTITION_VERSION,
            definitions: vec![PartitionDefinition::default(); partition_num],
            partition_key: vec!["col1".to_string(), "col2".to_string(), "col3".to_string()],
            linear: false,
        };

        // Build `RowGroup`
        let test_datums = vec![
            vec![
                Datum::Int32(1),
                Datum::String(StringBytes::from("test1")),
                Datum::UInt64(42),
            ],
            vec![
                Datum::Int32(4),
                Datum::String(StringBytes::from("test2")),
                Datum::UInt64(4242),
            ],
        ];

        let mut row_group_builder = RowGroupBuilder::new(schema.clone());
        row_group_builder
            .row_builder()
            .append_datum(Datum::UInt64(0))
            .unwrap()
            .append_datum(Datum::Timestamp(Timestamp::new(0)))
            .unwrap()
            .append_datum(test_datums[0][0].clone())
            .unwrap()
            .append_datum(test_datums[0][1].clone())
            .unwrap()
            .append_datum(test_datums[0][2].clone())
            .unwrap()
            .finish()
            .unwrap();
        row_group_builder
            .row_builder()
            .append_datum(Datum::UInt64(1))
            .unwrap()
            .append_datum(Datum::Timestamp(Timestamp::new(1)))
            .unwrap()
            .append_datum(test_datums[1][0].clone())
            .unwrap()
            .append_datum(test_datums[1][1].clone())
            .unwrap()
            .append_datum(test_datums[1][2].clone())
            .unwrap()
            .finish()
            .unwrap();
        let row_group = row_group_builder.build();

        // Basic flow
        let key_rule_adapter =
            DfPartitionRuleAdapter::new(PartitionInfo::Key(ket_partition), &schema).unwrap();
        let partitioned_rows = key_rule_adapter
            .locate_partitions_for_write(row_group)
            .unwrap();
        let partition_idxs = match partitioned_rows {
            PartitionedRowGroup::Multiple(iter) => iter.map(|v| v.partition_idx).collect_vec(),
            _ => panic!("invalid partitioned rows"),
        };

        // Expected
        let partition_keys_1 = test_datums[0].clone();
        let partition_key_refs_1 = partition_keys_1.iter().map(Datum::as_view);
        let partition_keys_2 = test_datums[1].clone();
        let partition_key_refs_2 = partition_keys_2.iter().map(Datum::as_view);
        let expected_1 = compute_partition(partition_key_refs_1, partition_num);
        let expected_2 = compute_partition(partition_key_refs_2, partition_num);
        let expecteds = vec![expected_1, expected_2];

        assert_eq!(partition_idxs, expecteds);
    }

    fn build_schema() -> Schema {
        Builder::new()
            .auto_increment_column_id(true)
            .add_key_column(
                column_schema::Builder::new(TSID_COLUMN.to_string(), DatumKind::UInt64)
                    .build()
                    .expect("should succeed build column schema"),
            )
            .unwrap()
            .add_key_column(
                column_schema::Builder::new("timestamp".to_string(), DatumKind::Timestamp)
                    .build()
                    .expect("should succeed build column schema"),
            )
            .unwrap()
            .add_normal_column(
                column_schema::Builder::new("col1".to_string(), DatumKind::Int32)
                    .build()
                    .expect("should succeed build column schema"),
            )
            .unwrap()
            .add_normal_column(
                column_schema::Builder::new("col2".to_string(), DatumKind::String)
                    .build()
                    .expect("should succeed build column schema"),
            )
            .unwrap()
            .add_normal_column(
                column_schema::Builder::new("col3".to_string(), DatumKind::UInt64)
                    .build()
                    .expect("should succeed build column schema"),
            )
            .unwrap()
            .build()
            .expect("should succeed to build schema")
    }
}
