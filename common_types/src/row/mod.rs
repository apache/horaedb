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

//! Row type

use std::{
    collections::HashMap,
    ops::{Index, IndexMut},
};

use snafu::{ensure, Backtrace, OptionExt, Snafu};

use crate::{
    column_schema::{ColumnId, ColumnSchema},
    datum::{Datum, DatumKind, DatumView},
    record_batch::RecordBatchWithKey,
    schema::{RecordSchemaWithKey, Schema},
    time::Timestamp,
};

pub mod bitset;
pub mod contiguous;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Column out of bound, len:{}, given:{}.\nBacktrace:\n{}",
        len,
        given,
        backtrace
    ))]
    ColumnOutOfBound {
        len: usize,
        given: usize,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Invalid row num, expect:{}, given:{}.\nBacktrace:\n{}",
        expect,
        given,
        backtrace
    ))]
    InvalidRowNum {
        expect: usize,
        given: usize,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Invalid column num, expect:{}, given:{}.\nBacktrace:\n{}",
        expect,
        given,
        backtrace
    ))]
    InvalidColumnNum {
        expect: usize,
        given: usize,
        backtrace: Backtrace,
    },

    #[snafu(display("Column cannot be null, name:{}.\nBacktrace:\n{}", column, backtrace))]
    NullColumn {
        column: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Column type mismatch, name:{}, expect:{:?}, given:{:?}.\nBacktrace:\n{}",
        column,
        expect,
        given,
        backtrace
    ))]
    TypeMismatch {
        column: String,
        expect: DatumKind,
        given: DatumKind,
        backtrace: Backtrace,
    },

    #[snafu(display("Missing columns to build row.\nBacktrace:\n{}", backtrace))]
    MissingColumns { backtrace: Backtrace },

    #[snafu(display("Convert column failed, column:{}, err:{}", column, source))]
    ConvertColumn {
        column: String,
        source: crate::datum::Error,
    },

    #[snafu(display("Column in the schema is not found, column_name:{}", column,))]
    ColumnNameNotFound { column: String },

    #[snafu(display(
        "Column in the schema is not found, column_name:{}.\nBacktrace:\n{}",
        column,
        backtrace
    ))]
    ColumnNotFoundInSchema {
        column: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Duplicate column id is found, column_id:{column_id}.\nBacktrace:\n{backtrace}",
    ))]
    DuplicateColumnId {
        column_id: ColumnId,
        backtrace: Backtrace,
    },
}

// Do not depend on test_util crates
pub type Result<T> = std::result::Result<T, Error>;

// TODO(yingwen):
// - Memory pooling (or Arena) and statistics
// - Custom Debug format
// - Add a type RowWithSchema so we can ensure the row always matches the schema
// - Maybe add a type RowOperation like kudu

/// Row contains multiple columns, each column is represented by a datum
/// The internal representation of row is not specific
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    cols: Vec<Datum>,
}

impl Row {
    /// Convert vec of Datum into Row
    #[inline]
    pub fn from_datums(cols: Vec<Datum>) -> Self {
        Self { cols }
    }

    /// Returns the column num
    #[inline]
    pub fn num_columns(&self) -> usize {
        self.cols.len()
    }

    /// Iterate all datums
    #[inline]
    pub fn iter(&self) -> IterDatum {
        IterDatum {
            iter: self.cols.iter(),
        }
    }

    /// Get the timestamp column
    #[inline]
    pub fn timestamp(&self, schema: &Schema) -> Option<Timestamp> {
        let timestamp_index = schema.timestamp_index();

        self.cols[timestamp_index].as_timestamp()
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.cols.iter().map(|col| col.size()).sum()
    }
}

#[derive(Debug)]
pub struct IterDatum<'a> {
    iter: std::slice::Iter<'a, Datum>,
}

impl<'a> Iterator for IterDatum<'a> {
    type Item = &'a Datum;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl Index<usize> for Row {
    type Output = Datum;

    fn index(&self, index: usize) -> &Self::Output {
        &self.cols[index]
    }
}

impl IndexMut<usize> for Row {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.cols[index]
    }
}

impl<'a> IntoIterator for &'a Row {
    type IntoIter = std::slice::Iter<'a, Datum>;
    type Item = &'a Datum;

    fn into_iter(self) -> Self::IntoIter {
        self.cols.iter()
    }
}

impl IntoIterator for Row {
    type IntoIter = std::vec::IntoIter<Self::Item>;
    type Item = Datum;

    fn into_iter(self) -> Self::IntoIter {
        self.cols.into_iter()
    }
}

/// Check whether the schema of the row equals to given `schema`
pub fn check_row_schema(row: &Row, schema: &Schema) -> Result<()> {
    ensure!(
        schema.num_columns() == row.num_columns(),
        InvalidColumnNum {
            expect: schema.num_columns(),
            given: row.num_columns(),
        }
    );

    for (index, datum) in row.iter().enumerate() {
        let column = schema.column(index);
        check_datum_type(datum, column)?;
    }

    Ok(())
}

// TODO(yingwen): For multiple rows that share the same schema, no need to store
// Datum for each row element, we can store the whole row as a binary and
// provide more efficient way to convert rows into columns
/// RowGroup
///
/// The min/max timestamp of an empty RowGroup is 0.
///
/// Rows in the RowGroup have the same schema. The internal representation of
/// rows is not specific.
#[derive(Clone, Debug)]
pub struct RowGroup {
    /// Schema of the row group, all rows in the row group should have same
    /// schema
    schema: Schema,
    /// Rows in the row group
    rows: Vec<Row>,
}

impl RowGroup {
    /// Create [RowGroup] without any check.
    ///
    /// The caller should ensure all the rows share the same schema as the
    /// provided one.
    #[inline]
    pub fn new_unchecked(schema: Schema, rows: Vec<Row>) -> Self {
        Self { schema, rows }
    }

    /// Check and create row group.
    ///
    /// [None] will be thrown if the rows have different schema from the
    /// provided one.
    #[inline]
    pub fn try_new(schema: Schema, rows: Vec<Row>) -> Result<Self> {
        rows.iter()
            .try_for_each(|row| check_row_schema(row, &schema))?;

        Ok(Self { schema, rows })
    }

    /// Returns true if the row group is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Returns number of rows in the row group
    #[inline]
    pub fn num_rows(&self) -> usize {
        self.rows.len()
    }

    /// Returns the idx-th row in the row group
    #[inline]
    pub fn get_row(&self, idx: usize) -> Option<&Row> {
        self.rows.get(idx)
    }

    /// Returns the idx-th mutable row in the row group
    #[inline]
    pub fn get_row_mut(&mut self, idx: usize) -> Option<&mut Row> {
        self.rows.get_mut(idx)
    }

    /// Iter all datum of the column
    ///
    /// Will panic if col_index is out of bound
    pub fn iter_column(&self, col_index: usize) -> IterCol {
        IterCol {
            rows: &self.rows,
            row_index: 0,
            col_index,
        }
    }

    /// The schema of the row group
    #[inline]
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    #[inline]
    pub fn take_rows(&mut self) -> Vec<Row> {
        std::mem::take(&mut self.rows)
    }

    #[inline]
    pub fn into_schema(self) -> Schema {
        self.schema
    }

    /// Iter the row group by rows
    // TODO(yingwen): Add a iter_with_schema
    pub fn iter(&self) -> IterRow {
        IterRow {
            iter: self.rows.iter(),
        }
    }
}

impl<'a> IntoIterator for &'a RowGroup {
    type IntoIter = std::slice::Iter<'a, Row>;
    type Item = &'a Row;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.iter()
    }
}

impl IntoIterator for RowGroup {
    type IntoIter = std::vec::IntoIter<Self::Item>;
    type Item = Row;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

#[derive(Debug)]
pub struct IterRow<'a> {
    iter: std::slice::Iter<'a, Row>,
}

impl<'a> Iterator for IterRow<'a> {
    type Item = &'a Row;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

#[derive(Clone, Debug)]
pub struct IterCol<'a> {
    rows: &'a Vec<Row>,
    row_index: usize,
    col_index: usize,
}

impl<'a> Iterator for IterCol<'a> {
    type Item = &'a Datum;

    fn next(&mut self) -> Option<Self::Item> {
        if self.rows.is_empty() {
            return None;
        }

        if self.row_index >= self.rows.len() {
            return None;
        }

        let row = &self.rows[self.row_index];
        self.row_index += 1;

        Some(&row[self.col_index])
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.rows.len() - self.row_index;
        (remaining, Some(remaining))
    }
}

/// Build the [`RowGroup`] from the columns.
pub struct RowGroupBuilderFromColumn {
    schema: Schema,
    cols: HashMap<ColumnId, Vec<Datum>>,
}

impl RowGroupBuilderFromColumn {
    pub fn with_capacity(schema: Schema, num_cols: usize) -> Self {
        Self {
            schema,
            cols: HashMap::with_capacity(num_cols),
        }
    }

    /// The newly-added column should have the same elements as the
    /// previously-added column's.
    pub fn try_add_column(&mut self, col_id: ColumnId, col: Vec<Datum>) -> Result<()> {
        if let Some(num_rows) = self.num_rows() {
            ensure!(
                num_rows == col.len(),
                InvalidRowNum {
                    expect: num_rows,
                    given: col.len(),
                }
            );
        }

        let old = self.cols.insert(col_id, col);
        ensure!(old.is_none(), DuplicateColumnId { column_id: col_id });

        Ok(())
    }

    pub fn build(mut self) -> RowGroup {
        let num_rows = self.num_rows();
        if Some(0) == num_rows {
            return RowGroup {
                schema: self.schema,
                rows: vec![],
            };
        };

        let num_rows = num_rows.unwrap();
        let num_cols = self.schema.num_columns();
        let mut rows = Vec::with_capacity(num_rows);

        // Pre-allocate the memory for column data in every row.
        for _ in 0..num_rows {
            let row = Vec::with_capacity(num_cols);
            rows.push(row);
        }

        let mut add_column_to_row = |row_idx: usize, datum: Datum| {
            rows[row_idx].push(datum);
        };

        for col_schema in self.schema.columns() {
            let col_id = col_schema.id;
            let datums = self.cols.remove(&col_id);

            match datums {
                Some(v) => {
                    for (row_idx, datum) in v.into_iter().enumerate() {
                        add_column_to_row(row_idx, datum);
                    }
                }
                None => {
                    for row_idx in 0..num_rows {
                        add_column_to_row(row_idx, Datum::Null);
                    }
                }
            }
        }

        RowGroup {
            schema: self.schema,
            rows: rows.into_iter().map(Row::from_datums).collect::<Vec<_>>(),
        }
    }

    #[inline]
    fn num_rows(&self) -> Option<usize> {
        self.cols.iter().next().map(|(_, v)| v.len())
    }
}

/// Check whether the datum kind matches the column schema
pub fn check_datum_type(datum: &Datum, column_schema: &ColumnSchema) -> Result<()> {
    // Check null datum
    if let Datum::Null = datum {
        ensure!(
            column_schema.is_nullable,
            NullColumn {
                column: &column_schema.name,
            }
        );
    } else {
        ensure!(
            datum.kind() == column_schema.data_type,
            TypeMismatch {
                column: &column_schema.name,
                expect: column_schema.data_type,
                given: datum.kind(),
            }
        );
    }

    Ok(())
}

/// Row builder for the row group
#[derive(Debug)]
pub struct RowBuilder<'a> {
    schema: &'a Schema,
    cols: Vec<Datum>,
}

impl<'a> RowBuilder<'a> {
    pub fn new(schema: &'a Schema) -> RowBuilder<'a> {
        Self {
            schema,
            cols: Vec::with_capacity(schema.num_columns()),
        }
    }

    /// Append a datum into the row
    pub fn append_datum(mut self, datum: Datum) -> Result<Self> {
        self.check_datum(&datum)?;

        self.cols.push(datum);

        Ok(self)
    }

    /// Check whether the datum is valid
    fn check_datum(&self, datum: &Datum) -> Result<()> {
        let index = self.cols.len();
        ensure!(
            index < self.schema.num_columns(),
            ColumnOutOfBound {
                len: self.schema.num_columns(),
                given: index,
            }
        );

        let column = self.schema.column(index);
        check_datum_type(datum, column)
    }

    /// Finish building this row and append this row into the row group
    pub fn finish(self) -> Result<Row> {
        ensure!(self.cols.len() == self.schema.num_columns(), MissingColumns);

        Ok(Row { cols: self.cols })
    }
}

pub trait RowView {
    fn try_get_column_by_name(&self, column_name: &str) -> Result<Option<Datum>>;

    fn column_by_idx(&self, column_idx: usize) -> Datum;
}

// TODO(yingwen): Add a method to get row view on RecordBatchWithKey.
/// A row view on the [RecordBatchWithKey].
///
/// `row_idx < record_batch.num_rows()` is ensured.
#[derive(Debug)]
pub struct RowViewOnBatch<'a> {
    pub record_batch: &'a RecordBatchWithKey,
    pub row_idx: usize,
}

impl<'a> RowViewOnBatch<'a> {
    pub fn iter_columns(&self) -> RowViewOnBatchColumnIter {
        RowViewOnBatchColumnIter {
            next_column_idx: 0,
            row_idx: self.row_idx,
            record_batch: self.record_batch,
        }
    }
}

pub struct RowViewOnBatchColumnIter<'a> {
    next_column_idx: usize,
    row_idx: usize,
    record_batch: &'a RecordBatchWithKey,
}

impl<'a> RowView for RowViewOnBatch<'a> {
    fn try_get_column_by_name(&self, column_name: &str) -> Result<Option<Datum>> {
        let column_idx = self
            .record_batch
            .schema_with_key()
            .index_of(column_name)
            .context(ColumnNameNotFound {
                column: column_name,
            })?;
        Ok(Some(self.column_by_idx(column_idx)))
    }

    #[inline]
    fn column_by_idx(&self, column_idx: usize) -> Datum {
        let column = self.record_batch.column(column_idx);
        column.datum(self.row_idx)
    }
}

impl<'a> Iterator for RowViewOnBatchColumnIter<'a> {
    type Item = Result<DatumView<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_column_idx >= self.record_batch.num_columns() {
            return None;
        }

        let curr_column_idx = self.next_column_idx;
        let column = self.record_batch.column(curr_column_idx);
        let datum_view = column.datum_view_opt(self.row_idx).map(Ok);

        self.next_column_idx += 1;

        datum_view
    }
}

#[derive(Debug, Clone)]
pub struct RowWithMeta<'a> {
    pub row: &'a Row,
    pub schema: &'a RecordSchemaWithKey,
}

impl<'a> RowView for RowWithMeta<'a> {
    fn try_get_column_by_name(&self, column_name: &str) -> Result<Option<Datum>> {
        let idx = self
            .schema
            .index_of(column_name)
            .context(ColumnNotFoundInSchema {
                column: column_name,
            })?;
        Ok(Some(self.column_by_idx(idx)))
    }

    #[inline]
    fn column_by_idx(&self, column_idx: usize) -> Datum {
        self.row.cols[column_idx].clone()
    }
}
