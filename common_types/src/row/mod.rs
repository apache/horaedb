// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Row type

use std::{
    cmp,
    ops::{Index, IndexMut, Range},
};

use snafu::{ensure, Backtrace, OptionExt, Snafu};

use crate::{
    column_schema::ColumnSchema,
    datum::{Datum, DatumKind},
    record_batch::RecordBatchWithKey,
    schema::{RecordSchemaWithKey, Schema},
    time::Timestamp,
};

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
        "Invalid column num of row, expect:{}, given:{}.\nBacktrace:\n{}",
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
}

// Do not depend on common_util crates
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
    pub fn from_datums(cols: Vec<Datum>) -> Self {
        Self { cols }
    }

    /// Returns the column num
    pub fn num_columns(&self) -> usize {
        self.cols.len()
    }

    /// Iterate all datums
    pub fn iter(&self) -> IterDatum {
        IterDatum {
            iter: self.cols.iter(),
        }
    }

    /// Get the timestamp column
    pub fn timestamp(&self, schema: &Schema) -> Option<Timestamp> {
        let timestamp_index = schema.timestamp_index();

        self.cols[timestamp_index].as_timestamp()
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

#[derive(Debug)]
pub struct RowGroupSlicer<'a> {
    range: Range<usize>,
    row_group: &'a RowGroup,
}

impl<'a> From<&'a RowGroup> for RowGroupSlicer<'a> {
    fn from(value: &'a RowGroup) -> RowGroupSlicer<'a> {
        Self {
            range: 0..value.rows.len(),
            row_group: value,
        }
    }
}

impl<'a> RowGroupSlicer<'a> {
    pub fn new(range: Range<usize>, row_group: &'a RowGroup) -> Self {
        Self { range, row_group }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.range.is_empty()
    }

    #[inline]
    pub fn schema(&self) -> &Schema {
        self.row_group.schema()
    }

    #[inline]
    pub fn iter(&self) -> IterRow<'a> {
        IterRow {
            iter: self.row_group.rows[self.range.start..self.range.end].iter(),
        }
    }

    #[inline]
    pub fn slice_range(&self) -> Range<usize> {
        self.range.clone()
    }

    #[inline]
    pub fn num_rows(&self) -> usize {
        self.range.len()
    }
}

// TODO(yingwen): For multiple rows that share the same schema, no need to store
// Datum for each row element, we can store the whole row as a binary and
// provide more efficent way to convert rows into columns
/// RowGroup
///
/// The min/max timestamp of an empty RowGroup is 0.
///
/// Rows in the RowGroup have the same schema. The internal representation of
/// rows is not specific.
#[derive(Debug)]
pub struct RowGroup {
    /// Schema of the row group, all rows in the row group should have same
    /// schema
    schema: Schema,
    /// Rows in the row group
    rows: Vec<Row>,
    // TODO(yingwen): Maybe remove min/max timestamp
    /// Min timestamp of all the rows
    min_timestamp: Timestamp,
    /// Max timestamp of all the rows
    max_timestamp: Timestamp,
}

impl RowGroup {
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

    /// Iter the row group by rows
    // TODO(yingwen): Add a iter_with_schema
    pub fn iter(&self) -> IterRow {
        IterRow {
            iter: self.rows.iter(),
        }
    }

    /// Get the min timestamp of rows
    #[inline]
    pub fn min_timestamp(&self) -> Timestamp {
        self.min_timestamp
    }

    /// Get the max timestamp of rows
    #[inline]
    pub fn max_timestamp(&self) -> Timestamp {
        self.max_timestamp
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

#[derive(Debug)]
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

/// RowGroup builder
#[derive(Debug)]
pub struct RowGroupBuilder {
    schema: Schema,
    rows: Vec<Row>,
    min_timestamp: Option<Timestamp>,
    max_timestamp: Timestamp,
}

impl RowGroupBuilder {
    /// Create a new builder
    pub fn new(schema: Schema) -> Self {
        Self::with_capacity(schema, 0)
    }

    /// Create a new builder with given capacity
    pub fn with_capacity(schema: Schema, capacity: usize) -> Self {
        Self {
            schema,
            rows: Vec::with_capacity(capacity),
            min_timestamp: None,
            max_timestamp: Timestamp::new(0),
        }
    }

    /// Create a new builder with schema and rows
    ///
    /// Return error if the `rows` do not matched the `schema`
    pub fn with_rows(schema: Schema, rows: Vec<Row>) -> Result<Self> {
        let mut row_group = Self::new(schema);

        // Check schema and update min/max timestamp
        for row in &rows {
            check_row_schema(row, &row_group.schema)?;
            row_group.update_timestamps(row);
        }

        row_group.rows = rows;

        Ok(row_group)
    }

    /// Add a schema checked row
    ///
    /// REQUIRE: Caller should ensure the schema of row must equal to the schema
    /// of this builder
    pub fn push_checked_row(&mut self, row: Row) {
        self.update_timestamps(&row);

        self.rows.push(row);
    }

    /// Acquire builder to build next row of the row group
    pub fn row_builder(&mut self) -> RowBuilder {
        RowBuilder {
            // schema: &self.schema,
            cols: Vec::with_capacity(self.schema.num_columns()),
            // rows: &mut self.rows,
            group_builder: self,
        }
    }

    /// Build the row group
    pub fn build(self) -> RowGroup {
        RowGroup {
            schema: self.schema,
            rows: self.rows,
            min_timestamp: self.min_timestamp.unwrap_or_else(|| Timestamp::new(0)),
            max_timestamp: self.max_timestamp,
        }
    }

    /// Update min/max timestamp of the row group
    fn update_timestamps(&mut self, row: &Row) {
        // check_row_schema() ensures this datum is a timestamp, so we just unwrap here
        let row_timestamp = row.timestamp(&self.schema).unwrap();

        self.min_timestamp = match self.min_timestamp {
            Some(min_timestamp) => Some(cmp::min(min_timestamp, row_timestamp)),
            None => Some(row_timestamp),
        };
        self.max_timestamp = cmp::max(self.max_timestamp, row_timestamp);
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

// TODO(yingwen): This builder is used to build RowGroup, need to provide a
// builder to build one row
/// Row builder for the row group
#[derive(Debug)]
pub struct RowBuilder<'a> {
    group_builder: &'a mut RowGroupBuilder,
    cols: Vec<Datum>,
}

impl<'a> RowBuilder<'a> {
    /// Append a datum into the row
    pub fn append_datum(mut self, datum: Datum) -> Result<Self> {
        self.check_datum(&datum)?;

        self.cols.push(datum);

        Ok(self)
    }

    /// Check whether the datum is valid
    fn check_datum(&self, datum: &Datum) -> Result<()> {
        let index = self.cols.len();
        let schema = &self.group_builder.schema;
        ensure!(
            index < schema.num_columns(),
            ColumnOutOfBound {
                len: schema.num_columns(),
                given: index,
            }
        );

        let column = schema.column(index);
        check_datum_type(datum, column)
    }

    /// Finish building this row and append this row into the row group
    pub fn finish(self) -> Result<()> {
        ensure!(
            self.cols.len() == self.group_builder.schema.num_columns(),
            MissingColumns
        );

        self.group_builder.push_checked_row(Row { cols: self.cols });
        Ok(())
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
    type Item = Result<Datum>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_column_idx >= self.record_batch.num_columns() {
            return None;
        }

        let curr_column_idx = self.next_column_idx;
        let column = self.record_batch.column(curr_column_idx);
        let datum = column.datum_opt(self.row_idx).map(Ok);

        self.next_column_idx += 1;

        datum
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
