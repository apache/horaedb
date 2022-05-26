// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Interpreter for insert statement

use async_trait::async_trait;
use common_types::{column_schema::ColumnId, datum::Datum, hash::hash64};
use common_util::codec::{compact::MemCompactEncoder, Encoder};
use snafu::{ResultExt, Snafu};
use sql::plan::InsertPlan;
use table_engine::table::WriteRequest;

use crate::{
    context::Context,
    interpreter::{Insert, Interpreter, InterpreterPtr, Output, Result},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to write table, err:{}", source))]
    WriteTable { source: table_engine::table::Error },

    #[snafu(display("Failed to encode tsid, err:{}", source))]
    EncodeTsid {
        source: common_util::codec::compact::Error,
    },
}

pub struct InsertInterpreter {
    ctx: Context,
    plan: InsertPlan,
}

impl InsertInterpreter {
    pub fn create(ctx: Context, plan: InsertPlan) -> InterpreterPtr {
        Box::new(Self { ctx, plan })
    }
}

#[async_trait]
impl Interpreter for InsertInterpreter {
    async fn execute(mut self: Box<Self>) -> Result<Output> {
        // Generate tsid if needed.
        self.maybe_generate_tsid()?;
        let InsertPlan { table, rows } = self.plan;

        // Context is unused now
        let _ctx = self.ctx;

        let request = WriteRequest { row_group: rows };

        let num_rows = table
            .write(request)
            .await
            .context(WriteTable)
            .context(Insert)?;

        Ok(Output::AffectedRows(num_rows))
    }
}

impl InsertInterpreter {
    fn maybe_generate_tsid(&mut self) -> Result<()> {
        let schema = self.plan.rows.schema();
        let tsid_idx = schema.index_of_tsid();

        if let Some(idx) = tsid_idx {
            // Vec of (`index of tag`, `column id of tag`).
            let tag_idx_column_ids: Vec<_> = schema
                .columns()
                .iter()
                .enumerate()
                .filter_map(|(i, column)| {
                    if column.is_tag {
                        Some((i, column.id))
                    } else {
                        None
                    }
                })
                .collect();

            let mut hash_bytes = Vec::new();
            for i in 0..self.plan.rows.num_rows() {
                let row = self.plan.rows.get_row_mut(i).unwrap();

                let mut tsid_builder = TsidBuilder::new(&mut hash_bytes);

                for (idx, column_id) in &tag_idx_column_ids {
                    tsid_builder.maybe_write_datum(*column_id, &row[*idx])?;
                }

                let tsid = tsid_builder.finish();
                row[idx] = Datum::UInt64(tsid);
            }
        }
        Ok(())
    }
}

struct TsidBuilder<'a> {
    encoder: MemCompactEncoder,
    hash_bytes: &'a mut Vec<u8>,
}

impl<'a> TsidBuilder<'a> {
    fn new(hash_bytes: &'a mut Vec<u8>) -> Self {
        // Clear the bytes buffer.
        hash_bytes.clear();

        Self {
            encoder: MemCompactEncoder,
            hash_bytes,
        }
    }

    fn maybe_write_datum(&mut self, column_id: ColumnId, datum: &Datum) -> Result<()> {
        // Null datum will be ignored, so tsid remains unchanged after adding a null
        // column.
        if datum.is_null() {
            return Ok(());
        }

        // Write column id first.
        self.encoder
            .encode(self.hash_bytes, &Datum::UInt64(u64::from(column_id)))
            .context(EncodeTsid)
            .context(Insert)?;
        // Write datum.
        self.encoder
            .encode(self.hash_bytes, datum)
            .context(EncodeTsid)
            .context(Insert)?;
        Ok(())
    }

    fn finish(self) -> u64 {
        hash64(self.hash_bytes)
    }
}
