// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Interpreter trait

use async_trait::async_trait;
use query_engine::executor::RecordBatchVec;
use snafu::Snafu;

// Make the variant closer to actual error code like invalid arguments.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Failed to execute select, err:{}", source))]
    Select { source: crate::select::Error },

    #[snafu(display("Failed to execute create table, err:{}", source))]
    Create { source: crate::create::Error },

    #[snafu(display("Failed to execute drop table, err:{}", source))]
    Drop { source: crate::drop::Error },

    #[snafu(display("Failed to execute insert, err:{}", source))]
    Insert { source: crate::insert::Error },

    #[snafu(display("Failed to execute describe, err:{}", source))]
    Describe { source: crate::describe::Error },

    #[snafu(display("Failed to execute alter table, err:{}", source))]
    AlterTable { source: crate::alter_table::Error },

    #[snafu(display("Failed to execute show create tables, err:{}", source))]
    ShowCreateTable { source: crate::show::Error },

    #[snafu(display("Failed to execute show tables, err:{}", source))]
    ShowTables { source: crate::show::Error },

    #[snafu(display("Failed to execute show database, err:{}", source))]
    ShowDatabases { source: crate::show::Error },

    #[snafu(display("Failed to execute exists, err:{}", source))]
    Exists { source: crate::exists::Error },

    #[snafu(display("Failed to transfer ouput to records"))]
    IntoRecords,
}

define_result!(Error);

// TODO(yingwen): Maybe add a stream variant for streaming result
/// The interpreter output
pub enum Output {
    /// Affected rows number
    AffectedRows(usize),
    /// A vec of RecordBatch
    Records(RecordBatchVec),
}

impl Output {
    /// Helper function to transfer into inner records just for test.
    pub fn into_records(self) -> Result<RecordBatchVec> {
        if let Output::Records(records) = self {
            Ok(records)
        } else {
            Err(Error::IntoRecords)
        }
    }
}

/// Interpreter executes the plan it holds
#[async_trait]
pub trait Interpreter {
    async fn execute(self: Box<Self>) -> Result<Output>;
}

/// A pointer to Interpreter
pub type InterpreterPtr = Box<dyn Interpreter + Send>;
