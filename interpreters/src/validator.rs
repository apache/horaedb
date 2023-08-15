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

use query_frontend::plan::{Plan, ShowPlan};
use table_engine::partition;

use crate::interpreter::{PermissionDenied, Result};

macro_rules! is_sub_table {
    ($table_name:expr) => {{
        let table_name = $table_name;
        partition::is_sub_partition_table(table_name)
    }};
}

/// Validator for [Plan]
#[derive(Debug)]
pub(crate) struct Validator {
    ctx: ValidateContext,
}

impl Validator {
    pub fn new(ctx: ValidateContext) -> Self {
        Self { ctx }
    }

    pub fn validate(&self, plan: &Plan) -> Result<()> {
        self.validate_partition_table_access(plan)?;

        Ok(())
    }

    fn validate_partition_table_access(&self, plan: &Plan) -> Result<()> {
        // Only can operate the sub tables(table partition) directly while enable
        // partition table access.
        if !self.ctx.enable_partition_table_access && Validator::contains_sub_tables(plan) {
            PermissionDenied {
                msg: "only can process sub tables in table partition directly when enable partition table access",
            }
            .fail()
        } else {
            Ok(())
        }
    }

    // TODO: reduce duplicated codes.
    fn contains_sub_tables(plan: &Plan) -> bool {
        match plan {
            Plan::Query(plan) => {
                let res = plan.tables.visit::<_, ()>(|name, _| {
                    if partition::is_sub_partition_table(name.table.as_ref()) {
                        Err(())
                    } else {
                        Ok(())
                    }
                });

                res.is_err()
            }

            Plan::Create(plan) => {
                is_sub_table!(&plan.table)
            }

            Plan::Drop(plan) => {
                is_sub_table!(&plan.table)
            }

            Plan::Insert(plan) => {
                is_sub_table!(plan.table.name())
            }

            Plan::Describe(plan) => {
                is_sub_table!(plan.table.name())
            }

            Plan::AlterTable(plan) => {
                is_sub_table!(plan.table.name())
            }

            Plan::Show(show_plan) => {
                if let ShowPlan::ShowCreatePlan(show_create_plan) = show_plan {
                    is_sub_table!(show_create_plan.table.name())
                } else {
                    false
                }
            }

            Plan::Exists(_) => false,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct ValidateContext {
    pub enable_partition_table_access: bool,
}
