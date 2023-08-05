// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Interpreter factory

use catalog::manager::ManagerRef;
use query_engine::{executor::Executor, physical_planner::PhysicalPlanner};
use query_frontend::plan::Plan;
use table_engine::engine::TableEngineRef;

use crate::{
    alter_table::AlterTableInterpreter,
    context::Context,
    create::CreateInterpreter,
    describe::DescribeInterpreter,
    drop::DropInterpreter,
    exists::ExistsInterpreter,
    insert::InsertInterpreter,
    interpreter::{InterpreterPtr, Result},
    select::SelectInterpreter,
    show::ShowInterpreter,
    table_manipulator::TableManipulatorRef,
    validator::{ValidateContext, Validator},
};

/// A factory to create interpreters
pub struct Factory<Q, P> {
    query_executor: Q,
    physical_planner: P,
    catalog_manager: ManagerRef,
    table_engine: TableEngineRef,
    table_manipulator: TableManipulatorRef,
}

impl<Q: Executor + 'static, P: PhysicalPlanner> Factory<Q, P> {
    pub fn new(
        query_executor: Q,
        physical_planner: P,
        catalog_manager: ManagerRef,
        table_engine: TableEngineRef,
        table_manipulator: TableManipulatorRef,
    ) -> Self {
        Self {
            query_executor,
            physical_planner,
            catalog_manager,
            table_engine,
            table_manipulator,
        }
    }

    pub fn create(self, ctx: Context, plan: Plan) -> Result<InterpreterPtr> {
        let validate_ctx = ValidateContext {
            enable_partition_table_access: ctx.enable_partition_table_access(),
        };
        let validator = Validator::new(validate_ctx);
        validator.validate(&plan)?;

        let interpreter = match plan {
            Plan::Query(p) => {
                SelectInterpreter::create(ctx, p, self.query_executor, self.physical_planner)
            }
            Plan::Insert(p) => InsertInterpreter::create(ctx, p),
            Plan::Create(p) => {
                CreateInterpreter::create(ctx, p, self.table_engine, self.table_manipulator)
            }
            Plan::Drop(p) => {
                DropInterpreter::create(ctx, p, self.table_engine, self.table_manipulator)
            }
            Plan::Describe(p) => DescribeInterpreter::create(p),
            Plan::AlterTable(p) => AlterTableInterpreter::create(p),
            Plan::Show(p) => ShowInterpreter::create(ctx, p, self.catalog_manager),
            Plan::Exists(p) => ExistsInterpreter::create(p),
        };

        Ok(interpreter)
    }
}
