// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;

use common_types::datum::Datum;
use opensrv_mysql::{Column, ColumnFlags, ColumnType, OkResponse, QueryResultWriter};

use crate::handlers::sql::Response;

pub struct MysqlQueryResultWriter<'a, W: std::io::Write> {
    inner: Option<QueryResultWriter<'a, W>>,
}

impl<'a, W: std::io::Write> MysqlQueryResultWriter<'a, W> {
    pub fn create(inner: QueryResultWriter<'a, W>) -> Self {
        Self { inner: Some(inner) }
    }

    pub fn write(&mut self, query_result: Response) -> std::io::Result<()> {
        if let Some(inner) = self.inner.take() {
            return match query_result {
                Response::AffectedRows(count) => Self::write_affected_rows(inner, count),
                Response::Rows(rows) => Self::write_rows(inner, rows),
            };
        }
        Ok(())
    }

    fn write_affected_rows(writer: QueryResultWriter<'a, W>, count: usize) -> std::io::Result<()> {
        let res = OkResponse {
            affected_rows: count as u64,
            ..Default::default()
        };
        writer.completed(res)?;
        Ok(())
    }

    fn write_rows(
        writer: QueryResultWriter<'a, W>,
        rows: Vec<HashMap<String, Datum>>,
    ) -> std::io::Result<()> {
        let default_response = OkResponse::default();
        if rows.is_empty() {
            writer.completed(default_response)?;
            return Ok(());
        }

        let columns = &rows[0]
            .iter()
            .map(|(k, v)| make_column_by_field(k, v))
            .collect::<Vec<_>>();
        let mut row_writer = writer.start(columns)?;

        for row in &rows {
            for column in columns {
                let key = &column.column;
                if let Some(val) = row.get(key) {
                    let data_type = convert_field_type(val);
                    let re = match (data_type, val) {
                        (_, Datum::Varbinary(_)) => row_writer.write_col("[Varbinary]"),
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
                } else {
                    row_writer.write_col(None::<u8>)?;
                }
            }
            row_writer.end_row()?;
        }

        Ok(())
    }
}

fn make_column_by_field(k: &str, v: &Datum) -> Column {
    let column_type = convert_field_type(v);
    Column {
        table: "".to_string(),
        column: k.to_string(),
        coltype: column_type,
        colflags: ColumnFlags::empty(),
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
    use common_types::{datum::Datum, time::Timestamp};
    use opensrv_mysql::{Column, ColumnFlags, ColumnType};

    use crate::mysql::writer::make_column_by_field;

    struct MakeColumnTest {
        k: &'static str,
        v: Datum,
        target_type: ColumnType,
    }

    #[test]
    fn test_make_column_by_field() {
        let tests = [
            MakeColumnTest {
                k: "id",
                v: Datum::UInt64(1),
                target_type: ColumnType::MYSQL_TYPE_LONG,
            },
            MakeColumnTest {
                k: "name",
                v: Datum::String("Bob".into()),
                target_type: ColumnType::MYSQL_TYPE_VARCHAR,
            },
            MakeColumnTest {
                k: "birthday",
                v: Datum::Timestamp(Timestamp::now()),
                target_type: ColumnType::MYSQL_TYPE_LONG,
            },
            MakeColumnTest {
                k: "is_show",
                v: Datum::Boolean(true),
                target_type: ColumnType::MYSQL_TYPE_SHORT,
            },
            MakeColumnTest {
                k: "money",
                v: Datum::Double(12.25),
                target_type: ColumnType::MYSQL_TYPE_DOUBLE,
            },
        ];

        for test in tests {
            let target_column = Column {
                table: "".to_string(),
                column: test.k.to_string(),
                coltype: test.target_type,
                colflags: ColumnFlags::default(),
            };
            assert_eq!(target_column, make_column_by_field(test.k, &test.v));
        }
    }
}
