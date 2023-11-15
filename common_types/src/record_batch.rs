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

//! Record batch

use std::{cmp, convert::TryFrom, mem, sync::Arc};

use arrow::{
    array::BooleanArray,
    compute,
    datatypes::{DataType, Field, Schema, SchemaRef as ArrowSchemaRef, TimeUnit},
    error::ArrowError,
    record_batch::RecordBatch as ArrowRecordBatch,
};
use arrow_ext::operation;
use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};

use crate::{
    column_block::{cast_nanosecond_to_mills, ColumnBlock, ColumnBlockBuilder},
    datum::DatumKind,
    projected_schema::{ProjectedSchema, RecordFetchingContext},
    row::{
        contiguous::{ContiguousRow, ProjectedContiguousRow},
        Row, RowViewOnBatch,
    },
    schema::{RecordSchema, RecordSchemaWithKey},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid schema len to build RecordBatch.\nBacktrace:\n{}", backtrace))]
    SchemaLen { backtrace: Backtrace },

    #[snafu(display("Failed to create column block, err:{}", source))]
    CreateColumnBlock { source: crate::column_block::Error },

    #[snafu(display(
        "Failed to create arrow record batch, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    CreateArrow {
        source: ArrowError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to iterate datum, err:{}", source))]
    IterateDatum { source: crate::row::Error },

    #[snafu(display("Failed to append datum, err:{}", source))]
    AppendDatum { source: crate::column_block::Error },

    #[snafu(display(
        "Column not in schema with key, column_name:{}.\nBacktrace:\n{}",
        name,
        backtrace
    ))]
    ColumnNotInSchemaWithKey { name: String, backtrace: Backtrace },

    #[snafu(display("Failed to convert arrow schema, err:{}", source))]
    ConvertArrowSchema { source: crate::schema::Error },

    #[snafu(display("Mismatch record schema to build RecordBatch, column_name:{}, schema_type:{:?}, column_type:{:?}.\nBacktrace:\n{}", column_name, schema_type, column_type, backtrace))]
    MismatchRecordSchema {
        column_name: String,
        schema_type: DatumKind,
        column_type: DatumKind,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Projection is out of the index, source_projection:{:?}, arrow_schema:{}.\nBacktrace:\n{}",
        source_projection,
        arrow_schema,
        backtrace
    ))]
    OutOfIndexProjection {
        source_projection: Vec<Option<usize>>,
        arrow_schema: ArrowSchemaRef,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to reverse record batch data, err:{:?}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    ReverseRecordBatchData {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to select record batch data, err:{:?}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    SelectRecordBatchData {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
        backtrace: Backtrace,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct RecordBatchData {
    arrow_record_batch: ArrowRecordBatch,
    column_blocks: Vec<ColumnBlock>,
}

impl RecordBatchData {
    fn new(arrow_schema: ArrowSchemaRef, column_blocks: Vec<ColumnBlock>) -> Result<Self> {
        let arrays = column_blocks
            .iter()
            .map(|column| column.to_arrow_array_ref())
            .collect();

        let arrow_record_batch =
            ArrowRecordBatch::try_new(arrow_schema, arrays).context(CreateArrow)?;

        Ok(RecordBatchData {
            arrow_record_batch,
            column_blocks,
        })
    }

    fn num_rows(&self) -> usize {
        self.column_blocks
            .first()
            .map(|column| column.num_rows())
            .unwrap_or(0)
    }

    fn take_column_block(&mut self, index: usize) -> ColumnBlock {
        let num_rows = self.num_rows();
        mem::replace(
            &mut self.column_blocks[index],
            ColumnBlock::new_null(num_rows),
        )
    }

    /// Returns a zero-copy slice of this array with the indicated offset and
    /// length.
    ///
    /// Panics if offset with length is greater than column length.
    fn slice(&self, offset: usize, length: usize) -> Self {
        let column_blocks = self
            .column_blocks
            .iter()
            .map(|col| col.slice(offset, length))
            .collect();

        Self {
            arrow_record_batch: self.arrow_record_batch.slice(offset, length),
            column_blocks,
        }
    }
}

fn build_column_blocks_from_arrow_record_batch(
    arrow_record_batch: &ArrowRecordBatch,
    record_schema: &RecordSchema,
) -> Result<Vec<ColumnBlock>> {
    let mut column_blocks = Vec::with_capacity(arrow_record_batch.num_columns());
    for (column_schema, array) in record_schema
        .columns()
        .iter()
        .zip(arrow_record_batch.columns())
    {
        let column = ColumnBlock::try_from_arrow_array_ref(&column_schema.data_type, array)
            .context(CreateColumnBlock)?;
        column_blocks.push(column);
    }

    Ok(column_blocks)
}

impl TryFrom<ArrowRecordBatch> for RecordBatchData {
    type Error = Error;

    fn try_from(arrow_record_batch: ArrowRecordBatch) -> Result<Self> {
        let record_schema =
            RecordSchema::try_from(arrow_record_batch.schema()).context(ConvertArrowSchema)?;
        let column_blocks =
            build_column_blocks_from_arrow_record_batch(&arrow_record_batch, &record_schema)?;
        Ok(Self {
            arrow_record_batch,
            column_blocks,
        })
    }
}

// TODO(yingwen): The schema in RecordBatch should be much simple because it may
// lack some information.
#[derive(Debug, Clone)]
pub struct RecordBatch {
    schema: RecordSchema,
    data: RecordBatchData,
}

impl RecordBatch {
    pub fn new_empty(schema: RecordSchema) -> Self {
        let arrow_schema = schema.to_arrow_schema_ref();
        let arrow_record_batch = ArrowRecordBatch::new_empty(arrow_schema);

        Self {
            schema,
            data: RecordBatchData {
                arrow_record_batch,
                column_blocks: Vec::new(),
            },
        }
    }

    pub fn new(schema: RecordSchema, column_blocks: Vec<ColumnBlock>) -> Result<Self> {
        ensure!(schema.num_columns() == column_blocks.len(), SchemaLen);

        // Validate schema and column_blocks.
        for (column_schema, column_block) in schema.columns().iter().zip(column_blocks.iter()) {
            ensure!(
                column_schema.data_type == column_block.datum_kind(),
                MismatchRecordSchema {
                    column_name: &column_schema.name,
                    schema_type: column_schema.data_type,
                    column_type: column_block.datum_kind(),
                }
            );
        }

        let arrow_schema = schema.to_arrow_schema_ref();
        let data = RecordBatchData::new(arrow_schema, column_blocks)?;

        Ok(Self { schema, data })
    }

    pub fn schema(&self) -> &RecordSchema {
        &self.schema
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }

    // REQUIRE: index is valid
    #[inline]
    pub fn column(&self, index: usize) -> &ColumnBlock {
        &self.data.column_blocks[index]
    }

    #[inline]
    pub fn num_columns(&self) -> usize {
        self.schema.num_columns()
    }

    #[inline]
    pub fn num_rows(&self) -> usize {
        self.data.num_rows()
    }

    #[inline]
    pub fn as_arrow_record_batch(&self) -> &ArrowRecordBatch {
        &self.data.arrow_record_batch
    }

    #[inline]
    pub fn into_arrow_record_batch(self) -> ArrowRecordBatch {
        self.data.arrow_record_batch
    }
}

impl TryFrom<ArrowRecordBatch> for RecordBatch {
    type Error = Error;

    fn try_from(arrow_record_batch: ArrowRecordBatch) -> Result<Self> {
        let record_schema =
            RecordSchema::try_from(arrow_record_batch.schema()).context(ConvertArrowSchema)?;

        let column_blocks =
            build_column_blocks_from_arrow_record_batch(&arrow_record_batch, &record_schema)?;

        let arrow_record_batch = cast_arrow_record_batch(arrow_record_batch)?;
        Ok(Self {
            schema: record_schema,
            data: RecordBatchData {
                arrow_record_batch,
                column_blocks,
            },
        })
    }
}

fn cast_arrow_record_batch(source: ArrowRecordBatch) -> Result<ArrowRecordBatch> {
    let row_count = source.num_columns();
    if row_count == 0 {
        return Ok(source);
    }
    let columns = source.columns();
    let mut casted_columns = Vec::with_capacity(columns.len());
    for column in columns {
        let column = match column.data_type() {
            DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                cast_nanosecond_to_mills(column).context(AppendDatum)?
            }
            _ => column.clone(),
        };
        casted_columns.push(column);
    }

    let schema = source.schema();
    let fields = schema.all_fields();
    let mills_fileds = fields
        .iter()
        .map(|field| {
            let mut f = match field.data_type() {
                DataType::Timestamp(TimeUnit::Nanosecond, None) => Field::new(
                    field.name(),
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    field.is_nullable(),
                ),
                _ => {
                    let (dict_id, dict_is_ordered) = {
                        match field.data_type() {
                            DataType::Dictionary(_, _) => {
                                (field.dict_id().unwrap(), field.dict_is_ordered().unwrap())
                            }
                            _ => (0, false),
                        }
                    };
                    Field::new_dict(
                        field.name(),
                        field.data_type().clone(),
                        field.is_nullable(),
                        dict_id,
                        dict_is_ordered,
                    )
                }
            };
            f.set_metadata(field.metadata().clone());
            f
        })
        .collect::<Vec<_>>();
    let mills_schema = Schema {
        fields: mills_fileds.into(),
        metadata: schema.metadata().clone(),
    };
    let result =
        ArrowRecordBatch::try_new(Arc::new(mills_schema), casted_columns).context(CreateArrow)?;
    Ok(result)
}

#[derive(Debug)]
pub struct FetchingRecordBatch {
    schema: RecordSchema,
    // TODO: remove it later, `FetchingRecordBatch` is unnecessary to know anything about primary
    // keys.
    primary_key_indexes: Option<Vec<usize>>,
    data: RecordBatchData,
}

impl FetchingRecordBatch {
    pub fn try_new(
        ctx: &RecordFetchingContext,
        arrow_record_batch: ArrowRecordBatch,
    ) -> Result<Self> {
        let column_indexes = ctx.fetching_projected_source_column_indexes();
        let schema = ctx.fetching_schema().clone();
        let mut column_blocks = Vec::with_capacity(schema.num_columns());

        let num_rows = arrow_record_batch.num_rows();
        let num_columns = arrow_record_batch.num_columns();
        for (col_idx_opt, col_schema) in column_indexes.iter().zip(schema.columns()) {
            match col_idx_opt {
                Some(col_idx) => {
                    ensure!(
                        *col_idx < num_columns,
                        OutOfIndexProjection {
                            source_projection: column_indexes,
                            arrow_schema: arrow_record_batch.schema()
                        }
                    );

                    let array = arrow_record_batch.column(*col_idx);
                    let column_block =
                        ColumnBlock::try_from_arrow_array_ref(&col_schema.data_type, array)
                            .context(CreateColumnBlock)?;

                    column_blocks.push(column_block);
                }
                None => {
                    // Need to push row with specific type.
                    let null_block = ColumnBlock::new_null_with_type(
                        &col_schema.data_type,
                        num_rows,
                        col_schema.is_dictionary,
                    )
                    .context(CreateColumnBlock)?;
                    column_blocks.push(null_block);
                }
            }
        }

        let data = RecordBatchData::new(schema.to_arrow_schema_ref(), column_blocks)?;

        Ok(FetchingRecordBatch {
            schema,
            primary_key_indexes: ctx.primary_key_indexes().map(|idxs| idxs.to_vec()),
            data,
        })
    }

    pub fn new_from_parts(
        schema: RecordSchema,
        primary_key_indexes: Option<Vec<usize>>,
        data: RecordBatchData,
    ) -> Self {
        Self {
            schema,
            primary_key_indexes,
            data,
        }
    }

    pub fn num_rows(&self) -> usize {
        self.data.num_rows()
    }

    pub fn num_columns(&self) -> usize {
        self.data.arrow_record_batch.num_columns()
    }

    pub fn columns(&self) -> &[ColumnBlock] {
        &self.data.column_blocks
    }

    pub fn clone_row_at(&self, index: usize) -> Row {
        let datums = self
            .data
            .column_blocks
            .iter()
            .map(|column_block| column_block.datum(index))
            .collect();

        Row::from_datums(datums)
    }

    /// Project the [RecordBatchWithKey] into a [RecordBatch] according to
    /// [ProjectedSchema].
    ///
    /// REQUIRE: The schema_with_key of the [RecordBatchWithKey] is the same as
    /// the schema_with_key of [ProjectedSchema].
    pub fn try_project(mut self, projected_schema: &ProjectedSchema) -> Result<RecordBatch> {
        // FIXME
        // debug_assert_eq!(
        //     &self.schema,
        //     projected_schema.as_record_schema_with_key()
        // );

        // Get the schema after projection.
        let record_schema = projected_schema.to_record_schema();
        let mut column_blocks = Vec::with_capacity(record_schema.num_columns());

        for column_schema in record_schema.columns() {
            let column_index =
                self.schema
                    .index_of(&column_schema.name)
                    .context(ColumnNotInSchemaWithKey {
                        name: &column_schema.name,
                    })?;

            // Take the column block out.
            let column_block = self.data.take_column_block(column_index);
            column_blocks.push(column_block);
        }

        let data = RecordBatchData::new(record_schema.to_arrow_schema_ref(), column_blocks)?;

        Ok(RecordBatch {
            schema: record_schema,
            data,
        })
    }

    pub fn into_record_batch(self) -> RecordBatch {
        RecordBatch {
            schema: self.schema,
            data: self.data,
        }
    }

    pub fn as_arrow_record_batch(&self) -> &ArrowRecordBatch {
        &self.data.arrow_record_batch
    }

    pub fn into_arrow_record_batch(self) -> ArrowRecordBatch {
        self.data.arrow_record_batch
    }

    pub fn schema_with_key(&self) -> Option<RecordSchemaWithKey> {
        self.primary_key_indexes
            .clone()
            .map(|idxs| RecordSchemaWithKey::new(self.schema.clone(), idxs))
    }

    #[inline]
    pub fn schema(&self) -> &RecordSchema {
        &self.schema
    }

    #[inline]
    pub fn primary_key_indexes(&self) -> Option<&[usize]> {
        self.primary_key_indexes.as_deref()
    }

    #[inline]
    pub fn column(&self, index: usize) -> &ColumnBlock {
        &self.data.column_blocks[index]
    }

    /// Reverse the rows in the data.
    ///
    /// The data retains intact if failed.
    pub fn reverse_data(&mut self) -> Result<()> {
        let reversed_record_batch = operation::reverse_record_batch(&self.data.arrow_record_batch)
            .map_err(|e| Box::new(e) as _)
            .context(ReverseRecordBatchData)?;

        self.data = RecordBatchData::try_from(reversed_record_batch)
            .map_err(|e| Box::new(e) as _)
            .context(ReverseRecordBatchData)?;

        Ok(())
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }

    /// Returns a zero-copy slice of this array with the indicated offset and
    /// length.
    ///
    /// Panics if offset with length is greater than column length.
    #[must_use]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        Self {
            schema: self.schema.clone(),
            primary_key_indexes: self.primary_key_indexes.clone(),
            data: self.data.slice(offset, length),
        }
    }

    /// Select the rows according to the `filter_array`.
    pub fn select_data(&mut self, filter_array: &BooleanArray) -> Result<()> {
        assert_eq!(self.num_rows(), filter_array.len());
        let selected_record_batch =
            compute::filter_record_batch(&self.data.arrow_record_batch, filter_array)
                .map_err(|e| Box::new(e) as _)
                .context(SelectRecordBatchData)?;

        self.data = RecordBatchData::try_from(selected_record_batch)
            .map_err(|e| Box::new(e) as _)
            .context(SelectRecordBatchData)?;

        Ok(())
    }
}

pub struct FetchingRecordBatchBuilder {
    fetching_schema: RecordSchema,
    primary_key_indexes: Option<Vec<usize>>,
    builders: Vec<ColumnBlockBuilder>,
}

impl FetchingRecordBatchBuilder {
    pub fn new(fetching_schema: RecordSchema, primary_key_indexes: Option<Vec<usize>>) -> Self {
        let builders = fetching_schema
            .columns()
            .iter()
            .map(|column_schema| {
                ColumnBlockBuilder::with_capacity(
                    &column_schema.data_type,
                    0,
                    column_schema.is_dictionary,
                )
            })
            .collect();
        Self {
            fetching_schema,
            primary_key_indexes,
            builders,
        }
    }

    pub fn with_capacity(
        record_schema: RecordSchema,
        primary_key_indexes: Option<Vec<usize>>,
        capacity: usize,
    ) -> Self {
        let builders = record_schema
            .columns()
            .iter()
            .map(|column_schema| {
                ColumnBlockBuilder::with_capacity(
                    &column_schema.data_type,
                    capacity,
                    column_schema.is_dictionary,
                )
            })
            .collect();
        Self {
            fetching_schema: record_schema,
            primary_key_indexes,
            builders,
        }
    }

    /// Append row into builder.
    ///
    /// REQUIRE: The row and the builder must have the same schema.
    pub fn append_row(&mut self, row: Row) -> Result<()> {
        for (builder, datum) in self.builders.iter_mut().zip(row) {
            builder.append(datum).context(AppendDatum)?;
        }

        Ok(())
    }

    /// Append projected contiguous row into builder.
    ///
    /// REQUIRE:
    /// - The schema of `row` is the same as the source schema of the
    ///   `projector`.
    /// - The projected schema (with key) is the same as the schema of the
    ///   builder.
    pub fn append_projected_contiguous_row<T: ContiguousRow>(
        &mut self,
        row: &ProjectedContiguousRow<T>,
    ) -> Result<()> {
        assert_eq!(row.num_datum_views(), self.builders.len());

        for (index, builder) in self.builders.iter_mut().enumerate() {
            let datum_view = row.datum_view_at(index);
            builder.append_view(datum_view).context(AppendDatum)?;
        }

        Ok(())
    }

    /// Append the row from the [RowView] to the builder.
    ///
    /// REQUIRE: The `row_view` and the builder must have the same schema.
    pub fn append_row_view(&mut self, row_view: &RowViewOnBatch) -> Result<()> {
        for (builder, datum_view) in self.builders.iter_mut().zip(row_view.iter_columns()) {
            let datum_view = datum_view.context(IterateDatum)?;
            builder.append_view(datum_view).context(AppendDatum)?;
        }

        Ok(())
    }

    /// Append `len` from `start` (inclusive) to this builder.
    ///
    /// REQUIRE:
    /// - The `record_batch` and the builder must have the same schema.
    pub fn append_batch_range(
        &mut self,
        record_batch: &FetchingRecordBatch,
        start: usize,
        len: usize,
    ) -> Result<usize> {
        let num_rows = record_batch.num_rows();
        if start >= num_rows {
            return Ok(0);
        }

        let added = cmp::min(num_rows - start, len);

        for (builder, column) in self.builders.iter_mut().zip(record_batch.columns().iter()) {
            builder
                .append_block_range(column, start, added)
                .context(AppendDatum)?;
        }

        Ok(added)
    }

    /// The number of the appended rows.
    pub fn len(&self) -> usize {
        self.builders
            .first()
            .map(|builder| builder.len())
            .unwrap_or(0)
    }

    /// Returns true if the builder is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Reset the builders for reuse.
    pub fn clear(&mut self) {
        for builder in &mut self.builders {
            builder.clear();
        }
    }

    /// Build [RecordBatchWithKey] and reset the builder.
    pub fn build(&mut self) -> Result<FetchingRecordBatch> {
        let column_blocks: Vec<_> = self
            .builders
            .iter_mut()
            .map(|builder| builder.build())
            .collect();
        let arrow_schema = self.fetching_schema.to_arrow_schema_ref();

        Ok(FetchingRecordBatch {
            schema: self.fetching_schema.clone(),
            primary_key_indexes: self.primary_key_indexes.clone(),
            data: RecordBatchData::new(arrow_schema, column_blocks)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        record_batch::{FetchingRecordBatch, FetchingRecordBatchBuilder},
        row::RowViewOnBatch,
        tests::{
            build_fetching_record_batch_by_rows, build_projected_schema, build_rows,
            check_record_batch_with_key_with_rows,
        },
    };

    fn build_fetching_record_batch() -> FetchingRecordBatch {
        let rows = build_rows();
        build_fetching_record_batch_by_rows(rows)
    }

    fn check_record_batch_with_key(
        record_batch_with_key: FetchingRecordBatch,
        row_num: usize,
        column_num: usize,
    ) -> bool {
        let rows = build_rows();
        check_record_batch_with_key_with_rows(&record_batch_with_key, row_num, column_num, rows)
    }

    #[test]
    fn test_append_projected_contiguous_row() {
        let record_batch_with_key = build_fetching_record_batch();
        assert_eq!(record_batch_with_key.num_rows(), 5);
        assert_eq!(record_batch_with_key.num_columns(), 5);

        check_record_batch_with_key(record_batch_with_key, 5, 5);
    }

    #[test]
    fn test_append_row_view() {
        let projected_schema = build_projected_schema();
        let fetching_record_batch = build_fetching_record_batch();
        let mut builder =
            FetchingRecordBatchBuilder::with_capacity(projected_schema.to_record_schema(), None, 2);
        let view = RowViewOnBatch {
            record_batch: &fetching_record_batch,
            row_idx: 1,
        };
        builder.append_row_view(&view).unwrap();
        let record_batch_with_key = builder.build().unwrap();
        assert_eq!(record_batch_with_key.num_rows(), 1);
        assert_eq!(record_batch_with_key.num_columns(), 5);

        check_record_batch_with_key(record_batch_with_key, 1, 5);
    }

    #[test]
    fn test_append_batch_range() {
        let projected_schema = build_projected_schema();
        let record_batch_with_key = build_fetching_record_batch();

        let mut builder =
            FetchingRecordBatchBuilder::with_capacity(projected_schema.to_record_schema(), None, 2);
        builder
            .append_batch_range(&record_batch_with_key, 0, 2)
            .unwrap();
        let record_batch_with_key = builder.build().unwrap();
        assert_eq!(record_batch_with_key.num_rows(), 2);
        assert_eq!(record_batch_with_key.num_columns(), 5);

        check_record_batch_with_key(record_batch_with_key, 2, 5);
    }
}
