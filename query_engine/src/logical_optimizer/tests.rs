// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! test utils for logical optimizer

use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use common_types::schema::Schema;
use datafusion::{
    common::{DFSchemaRef, ToDFSchema},
    datasource::TableProvider,
    execution::context::SessionState,
    physical_plan::ExecutionPlan,
};
use datafusion_expr::{
    Expr, Extension, Filter, Limit, LogicalPlan, Projection, Sort, TableScan, TableSource,
    TableType,
};

use crate::df_planner_extension::table_scan_by_primary_key::TableScanByPrimaryKey;

#[derive(Clone, Debug)]
#[must_use]
pub struct LogicalPlanNodeBuilder {
    pub schema: Schema,
    pub table_name: String,
    pub plan: Option<Arc<LogicalPlan>>,
}

pub struct MockTableProvider {
    schema: Schema,
}

#[async_trait]
impl TableProvider for MockTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    fn schema(&self) -> SchemaRef {
        self.schema.to_arrow_schema_ref()
    }

    async fn scan(
        &self,
        _state: &SessionState,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        unimplemented!("not support")
    }
}

impl TableSource for MockTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.to_arrow_schema_ref()
    }
}

impl LogicalPlanNodeBuilder {
    pub fn new(table_name: String, schema: Schema) -> Self {
        Self {
            schema,
            table_name,
            plan: None,
        }
    }

    // caller should ensure the sub plan exists.
    pub fn take_plan(&mut self) -> Arc<LogicalPlan> {
        self.plan.take().unwrap()
    }

    pub fn df_schema_ref(&self) -> DFSchemaRef {
        self.schema
            .to_arrow_schema_ref()
            .to_dfschema_ref()
            .expect("Build dfschema")
    }

    pub fn filter(mut self, predicate: Expr) -> Self {
        let plan = LogicalPlan::Filter(Filter::try_new(predicate, self.take_plan()).unwrap());

        self.plan = Some(Arc::new(plan));

        self
    }

    pub fn projection(mut self, proj_exprs: Vec<Expr>) -> Self {
        let plan =
            LogicalPlan::Projection(Projection::try_new(proj_exprs, self.take_plan()).unwrap());

        self.plan = Some(Arc::new(plan));

        self
    }

    pub fn limit(mut self, skip: usize, fetch: Option<usize>) -> Self {
        let plan = LogicalPlan::Limit(Limit {
            skip,
            fetch,
            input: self.take_plan(),
        });

        self.plan = Some(Arc::new(plan));

        self
    }

    pub fn sort(mut self, sort_exprs: Vec<Expr>) -> Self {
        let plan = LogicalPlan::Sort(Sort {
            expr: sort_exprs,
            input: self.take_plan(),
            fetch: None,
        });

        self.plan = Some(Arc::new(plan));

        self
    }

    pub fn table_scan(mut self) -> Self {
        let provider = MockTableProvider {
            schema: self.schema.clone(),
        };
        let projected_schema = self.df_schema_ref();

        let plan = LogicalPlan::TableScan(TableScan {
            table_name: self.table_name.clone(),
            source: Arc::new(provider),
            projection: None,
            projected_schema,
            filters: vec![],
            fetch: None,
        });

        self.plan = Some(Arc::new(plan));

        self
    }

    pub fn table_scan_in_primary_key_order(mut self, asc: bool) -> Self {
        let sub_plan = self.take_plan();
        let node = TableScanByPrimaryKey::new_from_scan_plan(asc, sub_plan);
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        });
        self.plan = Some(Arc::new(plan));

        self
    }
}

/// Check whether the logical plans are equal.
pub fn assert_logical_plan_eq(left: &LogicalPlan, right: &LogicalPlan) {
    let left_plan_str = format!("{:#?}", left);
    let right_plan_str = format!("{:#?}", right);
    assert_eq!(left_plan_str, right_plan_str)
}
