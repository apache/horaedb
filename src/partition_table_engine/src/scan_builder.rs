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

//! Partitioned table scan builder

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    error::{DataFusionError, Result},
    physical_plan::ExecutionPlan,
};
use df_engine_extensions::dist_sql_query::physical_plan::UnresolvedPartitionedScan;
use table_engine::{
    partition::{
        format_sub_partition_table_name,
        rule::df_adapter::{DfPartitionRuleAdapter, PartitionedFilterKeyIndex},
        PartitionInfo,
    },
    provider::TableScanBuilder,
    remote::model::TableIdentifier,
    table::ReadRequest,
};

use crate::partitioned_predicates;
#[derive(Debug)]
pub struct PartitionedTableScanBuilder {
    table_name: String,
    catalog_name: String,
    schema_name: String,
    partition_info: PartitionInfo,
}

impl PartitionedTableScanBuilder {
    pub fn new(
        table_name: String,
        catalog_name: String,
        schema_name: String,
        partition_info: PartitionInfo,
    ) -> Self {
        Self {
            table_name,
            catalog_name,
            schema_name,
            partition_info,
        }
    }

    fn get_sub_table_idents(
        &self,
        table_name: &str,
        partition_info: &PartitionInfo,
        partitions: &[usize],
    ) -> Vec<TableIdentifier> {
        let definitions = partition_info.get_definitions();
        partitions
            .iter()
            .map(|p| {
                let partition_name = &definitions[*p].name;
                TableIdentifier {
                    catalog: self.catalog_name.clone(),
                    schema: self.schema_name.clone(),
                    table: format_sub_partition_table_name(table_name, partition_name),
                }
            })
            .collect()
    }
}

#[async_trait]
impl TableScanBuilder for PartitionedTableScanBuilder {
    async fn build(&self, request: ReadRequest) -> Result<Arc<dyn ExecutionPlan>> {
        // Build partition rule.
        let table_schema_snapshot = request.projected_schema.table_schema();
        let df_partition_rule =
            DfPartitionRuleAdapter::new(self.partition_info.clone(), table_schema_snapshot)
                .map_err(|e| {
                    DataFusionError::Internal(format!("failed to build partition rule, err:{e}"))
                })?;

        let mut partitioned_key_indices = PartitionedFilterKeyIndex::new();
        // Evaluate expr and locate partition.
        let partitions = df_partition_rule
            .locate_partitions_for_read(request.predicate.exprs(), &mut partitioned_key_indices)
            .map_err(|e| {
                DataFusionError::Internal(format!("failed to locate partition for read, err:{e}"))
            })?;

        let sub_tables =
            self.get_sub_table_idents(&self.table_name, &self.partition_info, &partitions);

        let predicates = if partitioned_key_indices.len() == partitions.len() {
            Some(
                partitioned_predicates(
                    request.predicate.clone(),
                    &partitions,
                    &mut partitioned_key_indices,
                )
                .map_err(|e| {
                    DataFusionError::Internal(format!("partition predicates failed, err:{e}"))
                })?,
            )
        } else {
            // since FilterExtractor.extract only cover some specific expr
            // cases, partitioned_key_indices.len() could be 0.
            // All partition requests  will have the same predicate.
            None
        };

        // Build plan.
        let plan =
            UnresolvedPartitionedScan::new(&self.table_name, sub_tables, request, predicates);

        Ok(Arc::new(plan))
    }
}

#[cfg(test)]
mod tests {
    use common_types::{column_schema::Builder as ColBuilder, datum::DatumKind, schema::Builder};
    use datafusion::logical_expr::{
        binary_expr,
        expr::{BinaryExpr, InList},
        in_list, Expr, Operator,
    };
    use table_engine::{
        partition::{
            rule::df_adapter::{DfPartitionRuleAdapter, PartitionedFilterKeyIndex},
            KeyPartitionInfo, PartitionDefinition, PartitionInfo,
        },
        predicate::PredicateBuilder,
    };

    use crate::partitioned_predicates;

    #[test]
    fn test_partitioned_predicate() {
        // conditions:
        //   1) table schema: col_ts, col1, col2, in which col1 and col2 are both keys,
        //      and with two partitions
        //   2) sql: select * from table where col1 = '33' and col2 in ("aa", "bb",
        //      "cc", "dd")
        // partition expectations:
        //   1) query fit in two partitions
        //   2) yield two predicates,  p0: col1 = '33' and col2 in ("aa", "bb", "cc");
        //      p1: col1 = '33' and col2 in ("dd")
        let definitions = vec![
            PartitionDefinition {
                name: "p1".to_string(),
                origin_name: None,
            },
            PartitionDefinition {
                name: "p2".to_string(),
                origin_name: None,
            },
        ];

        let partition_info = PartitionInfo::Key(KeyPartitionInfo {
            version: 0,
            definitions,
            partition_key: vec!["col1".to_string(), "col2".to_string()],
            linear: false,
        });

        let schema = {
            let builder = Builder::new();
            let col_ts = ColBuilder::new("col_ts".to_string(), DatumKind::Timestamp)
                .build()
                .expect("ts");
            let col1 = ColBuilder::new("col1".to_string(), DatumKind::String)
                .build()
                .expect("should succeed to build column schema");
            let col2 = ColBuilder::new("col2".to_string(), DatumKind::String)
                .build()
                .expect("should succeed to build column schema");
            let col3 = ColBuilder::new("col3".to_string(), DatumKind::String)
                .build()
                .expect("should succeed to build column schema");
            builder
                .auto_increment_column_id(true)
                .add_key_column(col_ts)
                .unwrap()
                .add_key_column(col1)
                .unwrap()
                .add_key_column(col2)
                .unwrap()
                .add_key_column(col3)
                .unwrap()
                .primary_key_indexes(vec![1, 2])
                .build()
                .unwrap()
        };

        let df_partition_rule = DfPartitionRuleAdapter::new(partition_info, &schema).unwrap();

        let exprs = vec![
            binary_expr(
                Expr::Column("col1".into()),
                Operator::Eq,
                Expr::Literal("33".into()),
            ),
            in_list(
                Expr::Column("col2".into()),
                vec![
                    Expr::Literal("aa".into()),
                    Expr::Literal("bb".into()),
                    Expr::Literal("cc".into()),
                    Expr::Literal("dd".into()),
                ],
                false,
            ),
            in_list(
                Expr::Column("col3".into()),
                vec![
                    Expr::Literal("1".into()),
                    Expr::Literal("2".into()),
                    Expr::Literal("3".into()),
                    Expr::Literal("4".into()),
                ],
                false,
            ),
        ];
        let mut partitioned_key_indices = PartitionedFilterKeyIndex::new();
        let partitions = df_partition_rule
            .locate_partitions_for_read(&exprs, &mut partitioned_key_indices)
            .unwrap();
        assert!(partitions.len() == 2);
        assert!(partitioned_key_indices.len() == 2);

        let predicate = PredicateBuilder::default()
            .add_pushdown_exprs(exprs.as_slice())
            .build();

        let predicates = partitioned_predicates(
            predicate,
            partitions.as_slice(),
            &mut partitioned_key_indices,
        );
        assert!(predicates.is_ok());
        let predicates = predicates.unwrap();
        assert!(predicates.len() == 2);

        assert!(predicates[0].exprs().len() == 3);
        assert!(
            predicates[0].exprs()[0]
                == binary_expr(
                    Expr::Column("col1".into()),
                    Operator::Eq,
                    Expr::Literal("33".into())
                )
        );
        assert!(
            predicates[0].exprs()[1]
                == in_list(
                    Expr::Column("col2".into()),
                    vec![
                        Expr::Literal("aa".into()),
                        Expr::Literal("bb".into()),
                        Expr::Literal("cc".into()),
                    ],
                    false,
                )
        );
        assert!(
            predicates[0].exprs()[2]
                == in_list(
                    Expr::Column("col3".into()),
                    vec![
                        Expr::Literal("1".into()),
                        Expr::Literal("2".into()),
                        Expr::Literal("3".into()),
                        Expr::Literal("4".into()),
                    ],
                    false,
                )
        );
        assert!(
            predicates[1].exprs()[0]
                == binary_expr(
                    Expr::Column("col1".into()),
                    Operator::Eq,
                    Expr::Literal("33".into())
                )
        );
        assert!(
            predicates[1].exprs()[1]
                == in_list(
                    Expr::Column("col2".into()),
                    vec![Expr::Literal("dd".into()),],
                    false,
                )
        );
        assert!(
            predicates[1].exprs()[2]
                == in_list(
                    Expr::Column("col3".into()),
                    vec![
                        Expr::Literal("1".into()),
                        Expr::Literal("2".into()),
                        Expr::Literal("3".into()),
                        Expr::Literal("4".into()),
                    ],
                    false,
                )
        );
    }
}
