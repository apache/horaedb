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

//! Partition rule factory

use common_types::schema::Schema;
use snafu::{ensure, OptionExt};

use crate::partition::{
    rule::{
        key::{KeyRule, DEFAULT_PARTITION_VERSION},
        ColumnWithType, PartitionRuleRef,
    },
    BuildPartitionRule, KeyPartitionInfo, PartitionInfo, Result,
};

pub struct PartitionRuleFactory;

impl PartitionRuleFactory {
    pub fn create(partition_info: PartitionInfo, schema: &Schema) -> Result<PartitionRuleRef> {
        match partition_info {
            PartitionInfo::Key(key_info) => Self::create_key_rule(key_info, schema),
            _ => BuildPartitionRule {
                msg: format!("unsupported partition strategy, strategy:{partition_info:?}"),
            }
            .fail(),
        }
    }

    fn create_key_rule(key_info: KeyPartitionInfo, schema: &Schema) -> Result<PartitionRuleRef> {
        ensure!(
            key_info.version == DEFAULT_PARTITION_VERSION,
            BuildPartitionRule {
                msg: format!(
                    "only support key partition info version:{:?}, input_version:{}",
                    DEFAULT_PARTITION_VERSION, key_info.version
                )
            }
        );
        let typed_key_columns = key_info
            .partition_key
            .into_iter()
            .map(|col| {
                schema
                    .column_with_name(col.as_str())
                    .with_context(|| BuildPartitionRule {
                        msg: format!(
                            "column in key partition info not found in schema, column:{col}"
                        ),
                    })
                    .map(|col_schema| ColumnWithType::new(col, col_schema.data_type))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Box::new(KeyRule {
            typed_key_columns,
            partition_num: key_info.definitions.len(),
        }))
    }
}
