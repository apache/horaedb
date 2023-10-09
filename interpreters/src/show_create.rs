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

use std::{collections::HashMap, convert::TryInto, sync::Arc};

use arrow::{
    array::StringArray,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::logical_expr::Expr;
use datafusion_proto::bytes::Serializeable;
use logger::error;
use query_frontend::{ast::ShowCreateObject, plan::ShowCreatePlan};
use snafu::ensure;
use table_engine::{partition::PartitionInfo, table::TableRef};

use crate::{
    interpreter::Output,
    show::{Result, UnsupportedType},
    RecordBatchVec,
};

pub struct ShowCreateInterpreter {
    plan: ShowCreatePlan,
}

impl ShowCreateInterpreter {
    pub fn create(plan: ShowCreatePlan) -> ShowCreateInterpreter {
        Self { plan }
    }

    pub fn execute_show_create(self) -> Result<Output> {
        let ShowCreatePlan { table, obj_type } = self.plan;

        ensure!(
            obj_type == ShowCreateObject::Table,
            UnsupportedType { obj_type }
        );

        Self::table_ref_to_record_batch(table).map(Output::Records)
    }

    fn table_ref_to_record_batch(table_ref: TableRef) -> Result<RecordBatchVec> {
        let tables = vec![table_ref.name().to_string()];
        let sqls = vec![Self::render_table_sql(table_ref)];

        let schema = Schema::new(vec![
            Field::new("Table", DataType::Utf8, false),
            Field::new("Create Table", DataType::Utf8, false),
        ]);

        let arrow_record_batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(tables)),
                Arc::new(StringArray::from(sqls)),
            ],
        )
        .unwrap();

        let record_batch = arrow_record_batch.try_into().unwrap();

        Ok(vec![record_batch])
    }

    fn render_table_sql(table_ref: TableRef) -> String {
        //TODO(boyan) pretty output
        format!(
            "CREATE TABLE `{}` ({}){} ENGINE={}{}",
            table_ref.name(),
            Self::render_columns_and_constrains(&table_ref),
            Self::render_partition_info(table_ref.partition_info()),
            table_ref.engine_type(),
            Self::render_options(table_ref.options())
        )
    }

    fn render_columns_and_constrains(table_ref: &TableRef) -> String {
        let table_schema = table_ref.schema();
        let key_columns = table_schema.key_columns();
        let timestamp_key = table_schema.timestamp_name();

        let mut res = String::new();
        for col in table_schema.columns() {
            res += format!("`{}` {}", col.name, col.data_type).as_str();
            if col.is_tag {
                res += " TAG";
            }

            if col.is_dictionary {
                res += " DICTIONARY";
            }

            if !col.is_nullable {
                res += " NOT NULL";
            }

            if let Some(expr) = &col.default_value {
                res += format!(" DEFAULT {expr}").as_str();
            }

            if !col.comment.is_empty() {
                res += format!(" COMMENT '{}'", col.comment).as_str();
            }
            res += ", ";
        }
        let keys: Vec<String> = key_columns.iter().map(|col| col.name.to_string()).collect();
        res += format!("PRIMARY KEY({}), ", keys.join(",")).as_str();
        res += format!("TIMESTAMP KEY({timestamp_key})").as_str();

        res
    }

    fn render_partition_info(partition_info: Option<PartitionInfo>) -> String {
        if partition_info.is_none() {
            return String::new();
        }

        let partition_info = partition_info.unwrap();
        match partition_info {
            PartitionInfo::Hash(v) => {
                let expr = match Expr::from_bytes(&v.expr) {
                    Ok(expr) => expr,
                    Err(e) => {
                        error!("show create table parse partition info failed, err:{}", e);
                        return String::new();
                    }
                };

                if v.linear {
                    format!(
                        " PARTITION BY LINEAR HASH({expr}) PARTITIONS {}",
                        v.definitions.len()
                    )
                } else {
                    format!(
                        " PARTITION BY HASH({expr}) PARTITIONS {}",
                        v.definitions.len()
                    )
                }
            }
            PartitionInfo::Key(v) => {
                let rendered_partition_key = v.partition_key.join(",");
                if v.linear {
                    format!(
                        " PARTITION BY LINEAR KEY({rendered_partition_key}) PARTITIONS {}",
                        v.definitions.len()
                    )
                } else {
                    format!(
                        " PARTITION BY KEY({rendered_partition_key}) PARTITIONS {}",
                        v.definitions.len()
                    )
                }
            }
            PartitionInfo::Random(v) => {
                format!(" PARTITION BY RANDOM PARTITIONS {}", v.definitions.len())
            }
        }
    }

    fn render_options(opts: HashMap<String, String>) -> String {
        if !opts.is_empty() {
            let mut v: Vec<String> = opts
                .into_iter()
                .map(|(k, v)| format!("{k}='{v}'"))
                .collect();
            // sorted by option name
            v.sort();
            format!(" WITH({})", v.join(", "))
        } else {
            "".to_string()
        }
    }
}

#[cfg(test)]
mod test {
    use std::ops::Add;

    use datafusion::logical_expr::col;
    use datafusion_proto::bytes::Serializeable;
    use table_engine::partition::{
        HashPartitionInfo, KeyPartitionInfo, PartitionDefinition, PartitionInfo,
    };

    use super::*;

    #[test]
    fn test_render_hash_partition_info() {
        let expr = col("col1").add(col("col2"));
        let partition_info = PartitionInfo::Hash(HashPartitionInfo {
            version: 0,
            definitions: vec![
                PartitionDefinition {
                    name: "p0".to_string(),
                    origin_name: None,
                },
                PartitionDefinition {
                    name: "p1".to_string(),
                    origin_name: None,
                },
            ],
            expr: expr.to_bytes().unwrap(),
            linear: false,
        });

        let expected = " PARTITION BY HASH(col1 + col2) PARTITIONS 2".to_string();
        assert_eq!(
            expected,
            ShowCreateInterpreter::render_partition_info(Some(partition_info))
        );
    }

    #[test]
    fn test_render_key_partition_info() {
        let partition_key_col_name = "col1";
        let partition_info = PartitionInfo::Key(KeyPartitionInfo {
            version: 0,
            definitions: vec![
                PartitionDefinition {
                    name: "p0".to_string(),
                    origin_name: None,
                },
                PartitionDefinition {
                    name: "p1".to_string(),
                    origin_name: None,
                },
            ],
            partition_key: vec![partition_key_col_name.to_string()],
            linear: false,
        });

        let expected = " PARTITION BY KEY(col1) PARTITIONS 2".to_string();
        assert_eq!(
            expected,
            ShowCreateInterpreter::render_partition_info(Some(partition_info))
        );
    }
}
