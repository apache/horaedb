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
use snafu::ensure;

use crate::partition::{
    rule::{
        key::{KeyRule, DEFAULT_PARTITION_VERSION},
        random::RandomRule,
        PartitionRuleRef,
    },
    BuildPartitionRule, InvalidPartitionKey, KeyPartitionInfo, PartitionInfo, RandomPartitionInfo,
    Result,
};

pub struct PartitionRuleFactory;

impl PartitionRuleFactory {
    pub fn create(partition_info: PartitionInfo, schema: &Schema) -> Result<PartitionRuleRef> {
        match partition_info {
            PartitionInfo::Key(key_info) => Self::create_key_rule(key_info, schema),
            PartitionInfo::Random(random_info) => Self::create_random_rule(random_info),
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
        let valid_partition_key = key_info
            .partition_key
            .iter()
            .all(|col| schema.column_with_name(col.as_str()).is_some());
        ensure!(valid_partition_key, InvalidPartitionKey);

        Ok(Box::new(KeyRule::new(
            key_info.definitions.len(),
            key_info.partition_key,
        )))
    }

    fn create_random_rule(random_info: RandomPartitionInfo) -> Result<PartitionRuleRef> {
        Ok(Box::new(RandomRule {
            partition_num: random_info.definitions.len(),
        }))
    }
}
