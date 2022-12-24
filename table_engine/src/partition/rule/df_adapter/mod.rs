// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Partition rule datafusion adapter

use common_types::{row::RowGroup, schema::Schema};
use datafusion_expr::Expr;

use self::extractor::KeyExtractor;
use super::factory::PartitionRuleFactory;
use crate::partition::{
    rule::{df_adapter::extractor::FilterExtractorRef, factory::PartitionRuleRef},
    BuildPartitionRule, PartitionInfo, Result,
};

pub(crate) mod extractor;

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

    pub fn columns(&self) -> Vec<String> {
        self.rule.columns()
    }

    pub fn locate_partitions_for_write(&self, row_group: &RowGroup) -> Result<Vec<usize>> {
        self.rule.locate_partitions_for_write(row_group)
    }

    pub fn locate_partitions_for_read(&self, filters: &[Expr]) -> Result<Vec<usize>> {
        // Extract partition filters from datafusion filters.
        let columns = self.columns();
        let partition_filters = self.extractor.extract(filters, &columns);

        // Locate partitions from filters.
        self.rule.locate_partitions_for_read(&partition_filters)
    }

    fn create_extractor(partition_info: &PartitionInfo) -> Result<FilterExtractorRef> {
        match partition_info {
            PartitionInfo::Key(_) => Ok(Box::new(KeyExtractor)),
            PartitionInfo::Hash(_) => BuildPartitionRule {
                msg: format!(
                    "unsupported partition strategy, strategy:{:?}",
                    partition_info
                ),
            }
            .fail(),
        }
    }
}
