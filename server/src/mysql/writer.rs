// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use common_types::datum::{Datum, DatumKind};
use interpreters::interpreter::Output;
use opensrv_mysql::{Column, ColumnFlags, ColumnType, OkResponse, QueryResultWriter};
use query_engine::executor::RecordBatchVec;

use crate::{
    handlers::sql::{ResponseColumn},
    mysql::error::Result,
};

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

        let mut column_names = vec![];
        let mut column_data = vec![];

        for record_batch in records {
            let num_cols = record_batch.num_columns();
            let num_rows = record_batch.num_rows();
            let schema = record_batch.schema();

            for col_idx in 0..num_cols {
                let column_schema = schema.column(col_idx).clone();
                column_names.push(ResponseColumn {
                    name: column_schema.name,
                    data_type: column_schema.data_type,
                });
            }

            for row_idx in 0..num_rows {
                let mut row_data = Vec::with_capacity(num_cols);
                for col_idx in 0..num_cols {
                    let column = record_batch.column(col_idx);
                    let column = column.datum(row_idx);

                    row_data.push(column);
                }

                column_data.push(row_data);
            }
        }

        let columns = &column_names
            .iter()
            .map(make_column_by_field)
            .collect::<Vec<_>>();
        let mut row_writer = writer.start(columns)?;

        for row in &column_data {
            for val in row {
                let data_type = convert_field_type(val);
                let re = match (data_type, val) {
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
                        row_writer.write_col(*b as i8)
                    }
                    (ColumnType::MYSQL_TYPE_DOUBLE, v) => {
                        row_writer.write_col(v.as_f64().map_or(0.0, |v| v))
                    }
                    (ColumnType::MYSQL_TYPE_FLOAT, v) => {
                        row_writer.write_col(v.as_f64().map_or(0.0, |v| v))
                    }
                    (_, v) => Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Unsupported column type, val: {:?}", v),
                    )),
                };
                re?;
            }
            row_writer.end_row()?;
        }

        Ok(())
    }
}

fn make_column_by_field(column: &ResponseColumn) -> Column {
    let column_type = convert_datum_kind_type(&column.data_type);
    Column {
        table: "".to_string(),
        column: column.name.clone(),
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
    }
}

fn convert_field_type(field: &Datum) -> ColumnType {
    match field {
        Datum::Timestamp(_) => ColumnType::MYSQL_TYPE_LONG,
        Datum::Double(_) => ColumnType::MYSQL_TYPE_DOUBLE,
        Datum::Float(_) => ColumnType::MYSQL_TYPE_FLOAT,
        Datum::Varbinary(_) => ColumnType::MYSQL_TYPE_LONG_BLOB,
        Datum::String(_) => ColumnType::MYSQL_TYPE_VARCHAR,
        Datum::UInt64(_) => ColumnType::MYSQL_TYPE_LONG,
        Datum::UInt32(_) => ColumnType::MYSQL_TYPE_LONG,
        Datum::UInt16(_) => ColumnType::MYSQL_TYPE_LONG,
        Datum::UInt8(_) => ColumnType::MYSQL_TYPE_LONG,
        Datum::Int64(_) => ColumnType::MYSQL_TYPE_LONG,
        Datum::Int32(_) => ColumnType::MYSQL_TYPE_LONG,
        Datum::Int16(_) => ColumnType::MYSQL_TYPE_LONG,
        Datum::Int8(_) => ColumnType::MYSQL_TYPE_LONG,
        Datum::Boolean(_) => ColumnType::MYSQL_TYPE_SHORT,
        Datum::Null => ColumnType::MYSQL_TYPE_NULL,
    }
}

#[cfg(test)]
mod tests {
    use common_types::datum::DatumKind;
    use opensrv_mysql::{Column, ColumnFlags, ColumnType};

    use crate::{handlers::sql::ResponseColumn, mysql::writer::make_column_by_field};

    struct MakeColumnTest {
        column: ResponseColumn,
        target_type: ColumnType,
    }

    #[test]
    fn test_make_column_by_field() {
        let tests = [
            MakeColumnTest {
                column: ResponseColumn {
                    name: "id".to_string(),
                    data_type: DatumKind::Int32,
                },
                target_type: ColumnType::MYSQL_TYPE_LONG,
            },
            MakeColumnTest {
                column: ResponseColumn {
                    name: "name".to_string(),
                    data_type: DatumKind::String,
                },
                target_type: ColumnType::MYSQL_TYPE_VARCHAR,
            },
            MakeColumnTest {
                column: ResponseColumn {
                    name: "birthday".to_string(),
                    data_type: DatumKind::Timestamp,
                },
                target_type: ColumnType::MYSQL_TYPE_LONG,
            },
            MakeColumnTest {
                column: ResponseColumn {
                    name: "is_show".to_string(),
                    data_type: DatumKind::Boolean,
                },
                target_type: ColumnType::MYSQL_TYPE_SHORT,
            },
            MakeColumnTest {
                column: ResponseColumn {
                    name: "money".to_string(),
                    data_type: DatumKind::Double,
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
