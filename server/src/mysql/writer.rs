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

use common_types::{
    column_schema::ColumnSchema,
    datum::{Datum, DatumKind},
};
use interpreters::interpreter::Output;
use opensrv_mysql::{Column, ColumnFlags, ColumnType, OkResponse, QueryResultWriter};
use query_engine::executor::RecordBatchVec;

use crate::mysql::error::Result;

pub struct MysqlQueryResultWriter<'a, W: std::io::Write> {
    inner: Option<QueryResultWriter<'a, W>>,
}

impl<'a, W: std::io::Write> MysqlQueryResultWriter<'a, W> {
    pub fn create(inner: QueryResultWriter<'a, W>) -> Self {
        Self { inner: Some(inner) }
    }

    pub fn write(&mut self, query_result: Output) -> Result<()> {
        if let Some(inner) = self.inner.take() {
            return match query_result {
                Output::AffectedRows(count) => Self::write_affected_rows(inner, count),
                Output::Records(rows) => Self::write_rows(inner, rows),
            };
        }
        Ok(())
    }

    fn write_affected_rows(writer: QueryResultWriter<'a, W>, count: usize) -> Result<()> {
        let res = OkResponse {
            affected_rows: count as u64,
            ..Default::default()
        };
        writer.completed(res)?;
        Ok(())
    }

    fn write_rows(writer: QueryResultWriter<'a, W>, records: RecordBatchVec) -> Result<()> {
        let default_response = OkResponse::default();
        if records.is_empty() {
            writer.completed(default_response)?;
            return Ok(());
        }

        // Schema of records should be the same, so only get columns using first record.
        let columns = records[0]
            .schema()
            .columns()
            .iter()
            .map(make_column_by_field)
            .collect::<Vec<_>>();
        let mut row_writer = writer.start(&columns)?;

        for record_batch in records {
            let num_cols = record_batch.num_columns();
            let num_rows = record_batch.num_rows();
            for row_idx in 0..num_rows {
                for col_idx in 0..num_cols {
                    let val = record_batch.column(col_idx).datum(row_idx);
                    let data_type = convert_datum_kind_type(&val.kind());
                    match (data_type, val) {
                        (_, Datum::Varbinary(v)) => row_writer.write_col(v.as_ref()),
                        (_, Datum::Null) => row_writer.write_col(None::<u8>),
                        (ColumnType::MYSQL_TYPE_LONG, Datum::Timestamp(t)) => {
                            row_writer.write_col(t.as_i64())
                        }
                        (ColumnType::MYSQL_TYPE_VARCHAR, v) => {
                            row_writer.write_col(v.as_str().map_or("", |s| s))
                        }
                        (ColumnType::MYSQL_TYPE_LONG, v) => {
                            row_writer.write_col(v.as_u64().map_or(0, |v| v))
                        }
                        (ColumnType::MYSQL_TYPE_SHORT, Datum::Boolean(b)) => {
                            row_writer.write_col(b as i8)
                        }
                        (ColumnType::MYSQL_TYPE_DOUBLE, v) => {
                            row_writer.write_col(v.as_f64().map_or(0.0, |v| v))
                        }
                        (ColumnType::MYSQL_TYPE_FLOAT, v) => {
                            row_writer.write_col(v.as_f64().map_or(0.0, |v| v))
                        }
                        (_, v) => Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("Unsupported column type, val: {v:?}"),
                        )),
                    }?
                }

                row_writer.end_row()?;
            }
        }

        Ok(())
    }
}

fn make_column_by_field(column_schema: &ColumnSchema) -> Column {
    let column_type = convert_datum_kind_type(&column_schema.data_type);
    Column {
        table: "".to_string(),
        column: column_schema.name.clone(),
        coltype: column_type,
        colflags: ColumnFlags::empty(),
    }
}

fn convert_datum_kind_type(data_type: &DatumKind) -> ColumnType {
    match data_type {
        DatumKind::Timestamp => ColumnType::MYSQL_TYPE_LONG,
        DatumKind::Double => ColumnType::MYSQL_TYPE_DOUBLE,
        DatumKind::Float => ColumnType::MYSQL_TYPE_FLOAT,
        DatumKind::Varbinary => ColumnType::MYSQL_TYPE_LONG_BLOB,
        DatumKind::String => ColumnType::MYSQL_TYPE_VARCHAR,
        DatumKind::UInt64 => ColumnType::MYSQL_TYPE_LONG,
        DatumKind::UInt32 => ColumnType::MYSQL_TYPE_LONG,
        DatumKind::UInt16 => ColumnType::MYSQL_TYPE_LONG,
        DatumKind::UInt8 => ColumnType::MYSQL_TYPE_LONG,
        DatumKind::Int64 => ColumnType::MYSQL_TYPE_LONG,
        DatumKind::Int32 => ColumnType::MYSQL_TYPE_LONG,
        DatumKind::Int16 => ColumnType::MYSQL_TYPE_LONG,
        DatumKind::Int8 => ColumnType::MYSQL_TYPE_LONG,
        DatumKind::Boolean => ColumnType::MYSQL_TYPE_SHORT,
        DatumKind::Null => ColumnType::MYSQL_TYPE_NULL,
        DatumKind::Date => ColumnType::MYSQL_TYPE_DATE,
        DatumKind::Time => ColumnType::MYSQL_TYPE_TIME,
    }
}

#[cfg(test)]
mod tests {
    use common_types::{column_schema::ColumnSchema, datum::DatumKind};
    use opensrv_mysql::{Column, ColumnFlags, ColumnType};

    use crate::mysql::writer::make_column_by_field;

    struct MakeColumnTest {
        column: ColumnSchema,
        target_type: ColumnType,
    }

    #[test]
    fn test_make_column_by_field() {
        let tests = [
            MakeColumnTest {
                column: ColumnSchema {
                    id: 1,
                    name: "id".to_string(),
                    data_type: DatumKind::Int32,
                    is_nullable: false,
                    is_dictionary: false,
                    is_tag: false,
                    comment: "".to_string(),
                    escaped_name: "id".to_string(),
                    default_value: None,
                },
                target_type: ColumnType::MYSQL_TYPE_LONG,
            },
            MakeColumnTest {
                column: ColumnSchema {
                    id: 2,
                    name: "name".to_string(),
                    data_type: DatumKind::String,
                    is_nullable: true,
                    is_dictionary: false,
                    is_tag: true,
                    comment: "".to_string(),
                    escaped_name: "name".to_string(),
                    default_value: None,
                },
                target_type: ColumnType::MYSQL_TYPE_VARCHAR,
            },
            MakeColumnTest {
                column: ColumnSchema {
                    id: 3,
                    name: "birthday".to_string(),
                    data_type: DatumKind::Timestamp,
                    is_nullable: true,
                    is_tag: true,
                    is_dictionary: false,
                    comment: "".to_string(),
                    escaped_name: "birthday".to_string(),
                    default_value: None,
                },
                target_type: ColumnType::MYSQL_TYPE_LONG,
            },
            MakeColumnTest {
                column: ColumnSchema {
                    id: 4,
                    name: "is_show".to_string(),
                    data_type: DatumKind::Boolean,
                    is_nullable: true,
                    is_tag: true,
                    is_dictionary: false,
                    comment: "".to_string(),
                    escaped_name: "is_show".to_string(),
                    default_value: None,
                },
                target_type: ColumnType::MYSQL_TYPE_SHORT,
            },
            MakeColumnTest {
                column: ColumnSchema {
                    id: 5,
                    name: "money".to_string(),
                    data_type: DatumKind::Double,
                    is_nullable: true,
                    is_tag: true,
                    is_dictionary: false,
                    comment: "".to_string(),
                    escaped_name: "money".to_string(),
                    default_value: None,
                },
                target_type: ColumnType::MYSQL_TYPE_DOUBLE,
            },
        ];

        for test in tests {
            let target_column = Column {
                table: "".to_string(),
                column: test.column.name.clone(),
                coltype: test.target_type,
                colflags: ColumnFlags::default(),
            };
            assert_eq!(target_column, make_column_by_field(&test.column));
        }
    }
}
