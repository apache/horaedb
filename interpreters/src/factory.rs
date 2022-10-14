// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Interpreter factory

use catalog::manager::ManagerRef;
use query_engine::executor::Executor;
use sql::plan::Plan;
use table_engine::engine::TableEngineRef;

use crate::{
    alter_table::AlterTableInterpreter, context::Context, create::CreateInterpreter,
    describe::DescribeInterpreter, drop::DropInterpreter, exists::ExistsInterpreter,
    insert::InsertInterpreter, interpreter::InterpreterPtr, select::SelectInterpreter,
    show::ShowInterpreter, table_creator::TableCreatorRef, table_dropper::TableDropperRef,
};

/// A factory to create interpreters
pub struct Factory<Q> {
    query_executor: Q,
    catalog_manager: ManagerRef,
    table_engine: TableEngineRef,
    table_creator: TableCreatorRef,
    table_dropper: TableDropperRef,
}

impl<Q: Executor + 'static> Factory<Q> {
    pub fn new(
        query_executor: Q,
        catalog_manager: ManagerRef,
        table_engine: TableEngineRef,
        table_creator: TableCreatorRef,
        table_dropper: TableDropperRef,
    ) -> Self {
        Self {
            query_executor,
            catalog_manager,
            table_engine,
            table_creator,
            table_dropper,
        }
    }

    pub fn create(self, ctx: Context, plan: Plan) -> InterpreterPtr {
        match plan {
            Plan::Query(p) => SelectInterpreter::create(ctx, p, self.query_executor),
            Plan::Insert(p) => InsertInterpreter::create(ctx, p),
            Plan::Create(p) => {
                CreateInterpreter::create(ctx, p, self.table_engine, self.table_creator)
            }
            Plan::Drop(p) => DropInterpreter::create(ctx, p, self.table_engine, self.table_dropper),
            Plan::Describe(p) => DescribeInterpreter::create(p),
            Plan::AlterTable(p) => AlterTableInterpreter::create(p),
            Plan::Show(p) => ShowInterpreter::create(ctx, p, self.catalog_manager),
            Plan::Exists(p) => ExistsInterpreter::create(p),
        }
    }
}
