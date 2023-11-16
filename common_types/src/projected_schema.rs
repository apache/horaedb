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

//! Projected schema

use std::{fmt, sync::Arc};

use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};

use crate::{
    column_schema::{ColumnSchema, ReadOp},
    datum::{Datum, DatumKind},
    row::Row,
    schema::{ArrowSchemaRef, RecordSchema, RecordSchemaWithKey, Schema},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Invalid projection index, index:{}.\nBacktrace:\n{}",
        index,
        backtrace
    ))]
    InvalidProjectionIndex { index: usize, backtrace: Backtrace },

    #[snafu(display("Incompatible column schema for read, err:{}", source))]
    IncompatReadColumn {
        source: crate::column_schema::CompatError,
    },

    #[snafu(display("Failed to build projected schema, err:{}", source))]
    BuildProjectedSchema { source: crate::schema::Error },

    #[snafu(display(
        "Missing not null column for read, name:{}.\nBacktrace:\n{}",
        name,
        backtrace
    ))]
    MissingReadColumn { name: String, backtrace: Backtrace },

    #[snafu(display("Empty table schema.\nBacktrace:\n{}", backtrace))]
    EmptyTableSchema { backtrace: Backtrace },

    #[snafu(display("Failed to covert table schema, err:{}", source))]
    ConvertTableSchema {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct RecordFetchingContext {
    /// The schema for data fetching
    /// It is derived from table schema and some columns may not exist in data
    /// source.
    fetching_schema: RecordSchema,

    ///
    primary_key_indexes: Option<Vec<usize>>,

    /// Schema in data source
    /// It is possible to be different with the table
    /// schema caused by table schema altering.
    source_schema: Schema,

    /// The Vec stores the column index in source, and `None` means this column
    /// is not in source but required by reader, and need to filled by null.
    /// The length of Vec is the same as the number of columns reader intended
    /// to read.
    fetching_source_column_indexes: Vec<Option<usize>>,

    /// Similar as `fetching_source_column_indexes`, but storing the projected
    /// source column index
    ///
    /// For example:
    ///   source column indexes: 0,1,2,3,4
    ///   data fetching indexes in source: 2,1,3
    ///
    /// We can see, only columns:[1,2,3] in source is needed,
    /// and their indexes in pulled projected record bath are: [0,1,2].
    ///
    /// So the stored data fetching indexes in projected source are: [1,0,2].
    fetching_projected_source_column_indexes: Vec<Option<usize>>,
}

impl RecordFetchingContext {
    pub fn new(
        fetching_schema: &RecordSchema,
        primary_key_indexes: Option<Vec<usize>>,
        table_schema: &Schema,
        source_schema: &Schema,
    ) -> Result<Self> {
        // Get `fetching_source_column_indexes`.
        let mut fetching_source_column_indexes = Vec::with_capacity(fetching_schema.num_columns());
        let mut projected_source_indexes = Vec::with_capacity(fetching_schema.num_columns());
        for column_schema in fetching_schema.columns() {
            Self::try_project_column(
                column_schema,
                table_schema,
                source_schema,
                &mut fetching_source_column_indexes,
                &mut projected_source_indexes,
            )?;
        }

        // Get `fetching_projected_source_column_indexes` from
        // `fetching_source_column_indexes`.
        projected_source_indexes.sort_unstable();
        let fetching_projected_source_column_indexes = fetching_source_column_indexes
            .iter()
            .map(|source_idx_opt| {
                source_idx_opt.map(|src_idx| {
                    // Safe to unwrap, index exists in `fetching_source_column_indexes` is ensured
                    // to exist in `projected_source_indexes`.
                    projected_source_indexes
                        .iter()
                        .position(|proj_idx| src_idx == *proj_idx)
                        .unwrap()
                })
            })
            .collect();

        Ok(RecordFetchingContext {
            fetching_schema: fetching_schema.clone(),
            primary_key_indexes,
            source_schema: source_schema.clone(),
            fetching_source_column_indexes,
            fetching_projected_source_column_indexes,
        })
    }

    fn try_project_column(
        column: &ColumnSchema,
        table_schema: &Schema,
        source_schema: &Schema,
        fetching_source_column_indexes: &mut Vec<Option<usize>>,
        projected_source_indexes: &mut Vec<usize>,
    ) -> Result<()> {
        match source_schema.index_of(&column.name) {
            Some(source_idx) => {
                // Column is in source
                if table_schema.version() == source_schema.version() {
                    // Same version, just use that column in source
                    fetching_source_column_indexes.push(Some(source_idx));
                    projected_source_indexes.push(source_idx);
                } else {
                    // Different version, need to check column schema
                    let source_column = source_schema.column(source_idx);
                    // TODO(yingwen): Data type is not checked here because we do not support alter
                    // data type now.
                    match column
                        .compatible_for_read(source_column)
                        .context(IncompatReadColumn)?
                    {
                        ReadOp::Exact => {
                            fetching_source_column_indexes.push(Some(source_idx));
                            projected_source_indexes.push(source_idx);
                        }
                        ReadOp::FillNull => {
                            fetching_source_column_indexes.push(None);
                        }
                    }
                }
            }
            None => {
                // Column is not in source
                ensure!(column.is_nullable, MissingReadColumn { name: &column.name });
                // Column is nullable, fill this column by null
                fetching_source_column_indexes.push(None);
            }
        }

        Ok(())
    }

    pub fn source_schema(&self) -> &Schema {
        &self.source_schema
    }

    pub fn fetching_schema(&self) -> &RecordSchema {
        &self.fetching_schema
    }

    /// The projected indexes of existed columns in the source schema.
    pub fn existed_source_projection(&self) -> Vec<usize> {
        self.fetching_source_column_indexes
            .iter()
            .filter_map(|index| *index)
            .collect()
    }

    /// The projected indexes of all columns(existed and not exist) in the
    /// source schema.
    pub fn fetching_source_column_indexes(&self) -> &[Option<usize>] {
        &self.fetching_source_column_indexes
    }

    /// The projected indexes of all columns(existed and not exist) in the
    /// projected source schema.
    pub fn fetching_projected_source_column_indexes(&self) -> &[Option<usize>] {
        &self.fetching_projected_source_column_indexes
    }

    pub fn primary_key_indexes(&self) -> Option<&[usize]> {
        self.primary_key_indexes.as_deref()
    }

    /// Project the row.
    ///
    /// REQUIRE: The schema of row is the same as source schema.
    pub fn project_row(&self, row: &Row, mut datums_buffer: Vec<Datum>) -> Row {
        assert_eq!(self.source_schema.num_columns(), row.num_columns());

        datums_buffer.reserve(self.fetching_schema.num_columns());

        for p in &self.fetching_source_column_indexes {
            let datum = match p {
                Some(index_in_source) => row[*index_in_source].clone(),
                None => Datum::Null,
            };

            datums_buffer.push(datum);
        }

        Row::from_datums(datums_buffer)
    }

    /// Returns a datum kind selected
    /// using an index into the source schema columns.
    pub fn datum_kind(&self, index: usize) -> &DatumKind {
        assert!(index < self.source_schema.num_columns());

        &self.source_schema.column(index).data_type
    }
}

#[derive(Debug, Clone)]
pub struct RecordFetchingContextBuilder {
    fetching_schema: RecordSchema,
    table_schema: Schema,
    primary_key_indexes: Option<Vec<usize>>,
}

impl RecordFetchingContextBuilder {
    pub fn new(
        fetching_schema: RecordSchema,
        table_schema: Schema,
        primary_key_indexes: Option<Vec<usize>>,
    ) -> Self {
        Self {
            fetching_schema,
            table_schema,
            primary_key_indexes,
        }
    }

    pub fn build(&self, source_schema: &Schema) -> Result<RecordFetchingContext> {
        RecordFetchingContext::new(
            &self.fetching_schema,
            self.primary_key_indexes.clone(),
            &self.table_schema,
            source_schema,
        )
    }
}

#[derive(Clone)]
pub struct ProjectedSchema(Arc<ProjectedSchemaInner>);

impl fmt::Debug for ProjectedSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProjectedSchema")
            .field("original_schema", &self.0.table_schema)
            .field("projection", &self.0.projection)
            .finish()
    }
}

impl ProjectedSchema {
    pub fn no_projection(schema: Schema) -> Self {
        let inner = ProjectedSchemaInner::no_projection(schema);
        Self(Arc::new(inner))
    }

    pub fn new(table_schema: Schema, projection: Option<Vec<usize>>) -> Result<Self> {
        let inner = ProjectedSchemaInner::new(table_schema, projection)?;
        Ok(Self(Arc::new(inner)))
    }

    pub fn is_all_projection(&self) -> bool {
        self.0.is_all_projection()
    }

    pub fn projection(&self) -> Option<Vec<usize>> {
        self.0.projection()
    }

    // Returns the record schema after projection with key.
    pub fn to_record_schema_with_key(&self) -> RecordSchemaWithKey {
        self.0.record_schema_with_key.clone()
    }

    pub fn as_record_schema_with_key(&self) -> &RecordSchemaWithKey {
        &self.0.record_schema_with_key
    }

    // Returns the record schema after projection.
    pub fn to_record_schema(&self) -> RecordSchema {
        self.0.target_record_schema.clone()
    }

    /// Returns the arrow schema after projection.
    pub fn to_projected_arrow_schema(&self) -> ArrowSchemaRef {
        self.0.target_record_schema.to_arrow_schema_ref()
    }

    pub fn table_schema(&self) -> &Schema {
        &self.0.table_schema
    }
}

impl From<ProjectedSchema> for ceresdbproto::schema::ProjectedSchema {
    fn from(request: ProjectedSchema) -> Self {
        let table_schema_pb = (&request.0.table_schema).into();
        let projection_pb = request.0.projection.as_ref().map(|project| {
            let project = project
                .iter()
                .map(|one_project| *one_project as u64)
                .collect::<Vec<u64>>();
            ceresdbproto::schema::Projection { idx: project }
        });

        Self {
            table_schema: Some(table_schema_pb),
            projection: projection_pb,
        }
    }
}

impl TryFrom<ceresdbproto::schema::ProjectedSchema> for ProjectedSchema {
    type Error = Error;

    fn try_from(
        pb: ceresdbproto::schema::ProjectedSchema,
    ) -> std::result::Result<Self, Self::Error> {
        let schema: Schema = pb
            .table_schema
            .context(EmptyTableSchema)?
            .try_into()
            .map_err(|e| Box::new(e) as _)
            .context(ConvertTableSchema)?;
        let projection = pb
            .projection
            .map(|v| v.idx.into_iter().map(|id| id as usize).collect());

        ProjectedSchema::new(schema, projection)
    }
}

/// Schema with projection informations
struct ProjectedSchemaInner {
    /// The table schema used to generate plan, possible to differ from recorded
    /// schema in ssts.
    table_schema: Schema,
    /// Index of the projected columns in `self.schema`, `None` if
    /// all columns are needed.
    projection: Option<Vec<usize>>,

    /// The fetching record schema from `self.schema` with key columns after
    /// projection.
    record_schema_with_key: RecordSchemaWithKey,
    /// The fetching record schema from `self.schema` after projection.
    target_record_schema: RecordSchema,
}

impl ProjectedSchemaInner {
    fn no_projection(table_schema: Schema) -> Self {
        let record_schema_with_key = table_schema.to_record_schema_with_key();
        let target_record_schema = table_schema.to_record_schema();

        Self {
            table_schema,
            projection: None,
            record_schema_with_key,
            target_record_schema,
        }
    }

    fn new(table_schema: Schema, projection: Option<Vec<usize>>) -> Result<Self> {
        if let Some(p) = &projection {
            // Projection is provided, validate the projection is valid. This is necessary
            // to avoid panic when creating RecordSchema and
            // RecordSchemaWithKey.
            if let Some(max_idx) = p.iter().max() {
                ensure!(
                    *max_idx < table_schema.num_columns(),
                    InvalidProjectionIndex { index: *max_idx }
                );
            }

            let record_schema_with_key = table_schema.project_record_schema_with_key(p);
            let target_record_schema = table_schema.project_record_schema(p);

            Ok(Self {
                table_schema,
                projection,
                record_schema_with_key,
                target_record_schema,
            })
        } else {
            Ok(Self::no_projection(table_schema))
        }
    }

    /// Selecting all the columns is the all projection.
    fn is_all_projection(&self) -> bool {
        self.projection.is_none()
    }

    fn projection(&self) -> Option<Vec<usize>> {
        self.projection.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::{projected_schema::ProjectedSchema, tests::build_schema};

    #[test]
    fn test_projected_schema() {
        let schema = build_schema();
        assert!(schema.num_columns() > 1);
        let projection: Vec<usize> = (0..schema.num_columns() - 1).collect();
        let projected_schema = ProjectedSchema::new(schema.clone(), Some(projection)).unwrap();
        assert_eq!(
            projected_schema.0.record_schema_with_key.num_columns(),
            schema.num_columns() - 1
        );
        assert!(!projected_schema.is_all_projection());
    }
}
