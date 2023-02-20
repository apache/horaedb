// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Avro utility

use std::collections::HashMap;

use avro_rs::{
    schema::{Name, RecordField, RecordFieldOrder},
    types::{Record, Value},
    Schema as AvroSchema,
};
use chrono::Datelike;
use common_types::{
    bytes::{ByteVec, Bytes},
    column::{ColumnBlock, ColumnBlockBuilder},
    datum::{Datum, DatumKind},
    record_batch::RecordBatch,
    row::{Row, RowGroup, RowGroupBuilder},
    schema::{RecordSchema, Schema},
    string::StringBytes,
    time::Timestamp,
};
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};

use crate::error::{BoxError, GenericError};

/// Schema name of the record
const RECORD_NAME: &str = "Result";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to write avro record, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    WriteAvroRecord {
        source: avro_rs::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to convert to avro record, err:{}", source))]
    ParseAvroRecord { source: GenericError },

    #[snafu(display(
        "Invalid avro record, expect record, value:{:?}.\nBacktrace:\n{}",
        value,
        backtrace
    ))]
    InvalidAvroRecord { value: Value, backtrace: Backtrace },

    #[snafu(display(
        "Column not found in record schema, column:{}.\nBacktrace:\n{}",
        column,
        backtrace
    ))]
    ColumnNotFound {
        column: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Invalid avro record, expect record, value:{:?}.\nBacktrace:\n{}",
        value,
        backtrace
    ))]
    AvroRowsToRowNoCause { value: Value, backtrace: Backtrace },

    #[snafu(display(
        "Failed to convert avro rows to record batch, msg:{}, err:{}",
        msg,
        source
    ))]
    AvroRowsToRecordBatch { msg: String, source: GenericError },

    #[snafu(display(
        "Failed to convert avro rows to row group, msg:{}, err:{}",
        msg,
        source
    ))]
    AvroRowsToRowGroup { msg: String, source: GenericError },

    #[snafu(display(
        "Failed to convert row group to avro rows with no cause, msg:{}.\nBacktrace:\n{}",
        msg,
        backtrace
    ))]
    RowGroupToAvroRowsNoCause { msg: String, backtrace: Backtrace },

    #[snafu(display(
        "Failed to convert row group to avro rows with cause, msg:{}, err:{}",
        msg,
        source
    ))]
    RowGroupToAvroRowsWithCause { msg: String, source: GenericError },

    #[snafu(display("Unsupported conversion from avro value to datum, value:{:?}, datum_type:{}.\nBacktrace:\n{}", value, datum_type, backtrace))]
    UnsupportedConversion {
        value: Value,
        datum_type: DatumKind,
        backtrace: Backtrace,
    },
}

define_result!(Error);

/// Create [avro_rs::Schema] with given `name` from [RecordSchema]
pub fn to_avro_schema(name: &str, schema: &RecordSchema) -> avro_rs::Schema {
    let columns = schema.columns();
    let mut lookup = HashMap::with_capacity(columns.len());
    let mut avro_fields = Vec::with_capacity(columns.len());

    for (pos, column) in columns.iter().enumerate() {
        // Create avro record field
        let default = if column.is_nullable {
            Some(serde_json::value::Value::Null)
        } else {
            None
        };

        let field_schema = if column.is_nullable {
            // We want to declare a schema which may be either a null or non-null value,
            // for example: ["null", "string"].
            //
            // However, `avro_rs` does not provide an accessible API to build a `Union`.
            // We can't find a better way to do this than using JSON.
            let field_schema_str = format!(
                r#"["null", {}]"#,
                data_type_to_schema(&column.data_type).canonical_form()
            );
            avro_rs::Schema::parse_str(&field_schema_str).unwrap()
        } else {
            data_type_to_schema(&column.data_type)
        };

        // In dummy select like select "xxx", column name will be "Utf8("xxx")", which
        // is not a valid json string. So, escaped name is used here.
        let record_field = RecordField {
            name: column.escaped_name.clone(),
            doc: None,
            default,
            schema: field_schema,
            order: RecordFieldOrder::Ignore,
            position: pos,
        };

        avro_fields.push(record_field);
        lookup.insert(column.escaped_name.clone(), pos);
    }

    avro_rs::Schema::Record {
        name: Name::new(name),
        doc: None,
        fields: avro_fields,
        lookup,
    }
}

pub fn record_batch_to_avro_rows(record_batch: &RecordBatch) -> Result<Vec<ByteVec>> {
    let mut rows = Vec::new();
    let avro_schema = to_avro_schema(RECORD_NAME, record_batch.schema());
    record_batch_to_avro(record_batch, &avro_schema, &mut rows)?;
    Ok(rows)
}

pub fn avro_rows_to_record_batch(
    raws: Vec<Vec<u8>>,
    record_schema: RecordSchema,
) -> Result<RecordBatch> {
    let avro_schema = to_avro_schema(RECORD_NAME, &record_schema);

    // Collect datums and append to `ColumnBlockBuilder`s.
    let mut row_buf = Vec::with_capacity(record_schema.num_columns());
    let mut column_block_builders = record_schema
        .columns()
        .iter()
        .map(|col_schema| ColumnBlockBuilder::new(&col_schema.data_type))
        .collect::<Vec<_>>();

    for raw in raws {
        row_buf.clear();
        avro_row_to_row(&avro_schema, &record_schema, &raw, &mut row_buf)
            .box_err()
            .context(AvroRowsToRecordBatch {
                msg: format!(
                    "parse avro raw to row failed, avro schema:{avro_schema:?}, raw:{raw:?}"
                ),
            })?;
        assert_eq!(row_buf.len(), column_block_builders.len());

        for (col_idx, datum) in row_buf.iter().enumerate() {
            let column_block_builder = column_block_builders.get_mut(col_idx).unwrap();
            column_block_builder
                .append(datum.clone())
                .box_err()
                .context(AvroRowsToRecordBatch {
                    msg: format!(
                        "append datum to column block builder failed, datum:{datum:?}, builder:{column_block_builder:?}"
                    ),
                })?
        }
    }

    // Build `RecordBatch`.
    let column_blocks = column_block_builders
        .into_iter()
        .map(|mut builder| builder.build())
        .collect::<Vec<_>>();
    RecordBatch::new(record_schema, column_blocks)
        .box_err()
        .context(AvroRowsToRecordBatch {
            msg: "build record batch failed",
        })
}

pub fn avro_rows_to_row_group(schema: Schema, rows: &[Vec<u8>]) -> Result<RowGroup> {
    let record_schema = schema.to_record_schema();
    let avro_schema = to_avro_schema(RECORD_NAME, &record_schema);
    let mut builder = RowGroupBuilder::with_capacity(schema.clone(), rows.len());
    for raw_row in rows {
        let mut row = Vec::with_capacity(schema.num_columns());
        avro_row_to_row(&avro_schema, &record_schema, raw_row, &mut row)?;
        builder.push_checked_row(Row::from_datums(row));
    }

    Ok(builder.build())
}

pub fn row_group_to_avro_rows(row_group: RowGroup) -> Result<Vec<Vec<u8>>> {
    let record_schema = row_group.schema().to_record_schema();
    let column_schemas = record_schema.columns();
    let avro_schema = to_avro_schema(RECORD_NAME, &record_schema);

    let mut rows = Vec::with_capacity(row_group.num_rows());
    let row_len = row_group.num_rows();
    for row_idx in 0..row_len {
        // Convert `Row` to `Record` in avro.
        let row = row_group.get_row(row_idx).unwrap();
        let mut avro_record = Record::new(&avro_schema).context(RowGroupToAvroRowsNoCause {
            msg: format!("new avro record with schema failed, schema:{avro_schema:?}"),
        })?;

        for (col_idx, column_schema) in column_schemas.iter().enumerate() {
            let column_value = row[col_idx].clone();
            let avro_value = datum_to_avro_value(column_value, column_schema.is_nullable);
            avro_record.put(&column_schema.name, avro_value);
        }

        let row_bytes = avro_rs::to_avro_datum(&avro_schema, avro_record)
            .box_err()
            .context(RowGroupToAvroRowsWithCause {
                msg: format!("new avro record with schema failed, schema:{avro_schema:?}"),
            })?;
        rows.push(row_bytes);
    }

    Ok(rows)
}

fn data_type_to_schema(data_type: &DatumKind) -> avro_rs::Schema {
    match data_type {
        DatumKind::Null => avro_rs::Schema::Null,
        DatumKind::Timestamp => avro_rs::Schema::TimestampMillis,
        DatumKind::Double => avro_rs::Schema::Double,
        DatumKind::Float => avro_rs::Schema::Float,
        DatumKind::Varbinary => avro_rs::Schema::Bytes,
        DatumKind::String => avro_rs::Schema::String,
        DatumKind::UInt32 | DatumKind::Int64 | DatumKind::UInt64 => avro_rs::Schema::Long,
        DatumKind::UInt16
        | DatumKind::UInt8
        | DatumKind::Int32
        | DatumKind::Int16
        | DatumKind::Int8 => avro_rs::Schema::Int,
        DatumKind::Boolean => avro_rs::Schema::Boolean,
        DatumKind::Date => avro_rs::Schema::Date,
        DatumKind::Time => avro_rs::Schema::TimeMicros,
    }
}

/// Convert record batch to avro format
fn record_batch_to_avro(
    record_batch: &RecordBatch,
    schema: &avro_rs::Schema,
    rows: &mut Vec<ByteVec>,
) -> Result<()> {
    let record_batch_schema = record_batch.schema();
    assert_eq!(
        record_batch_schema.num_columns(),
        record_batch.num_columns()
    );

    rows.reserve(record_batch.num_rows());

    let column_schemas = record_batch_schema.columns();
    for row_idx in 0..record_batch.num_rows() {
        let mut record = Record::new(schema).unwrap();
        for (col_idx, column_schema) in column_schemas.iter().enumerate() {
            let column = record_batch.column(col_idx);
            let value = column_to_value(column, row_idx, column_schema.is_nullable);

            record.put(&column_schema.escaped_name, value);
        }

        let row_bytes = avro_rs::to_avro_datum(schema, record).context(WriteAvroRecord)?;

        rows.push(row_bytes);
    }

    Ok(())
}

/// Panic if row_idx is out of bound.
fn column_to_value(array: &ColumnBlock, row_idx: usize, is_nullable: bool) -> Value {
    let datum = array.datum(row_idx);
    datum_to_avro_value(datum, is_nullable)
}

pub fn datum_to_avro_value(datum: Datum, is_nullable: bool) -> Value {
    match datum {
        Datum::Null => may_union(Value::Null, is_nullable),
        Datum::Timestamp(v) => may_union(Value::TimestampMillis(v.as_i64()), is_nullable),
        Datum::Double(v) => may_union(Value::Double(v), is_nullable),
        Datum::Float(v) => may_union(Value::Float(v), is_nullable),
        Datum::Varbinary(v) => may_union(Value::Bytes(v.to_vec()), is_nullable),
        Datum::String(v) => may_union(Value::String(v.to_string()), is_nullable),
        Datum::UInt64(v) => may_union(Value::Long(v as i64), is_nullable),
        Datum::Int64(v) => may_union(Value::Long(v), is_nullable),
        Datum::UInt32(v) => may_union(Value::Long(v as i64), is_nullable),
        Datum::UInt16(v) => may_union(Value::Int(i32::from(v)), is_nullable),
        Datum::UInt8(v) => may_union(Value::Int(i32::from(v)), is_nullable),
        Datum::Int32(v) => may_union(Value::Int(v), is_nullable),
        Datum::Int16(v) => may_union(Value::Int(i32::from(v)), is_nullable),
        Datum::Int8(v) => may_union(Value::Int(i32::from(v)), is_nullable),
        Datum::Boolean(v) => may_union(Value::Boolean(v), is_nullable),
        // Value::Date in avro(https://docs.rs/avro-rs/latest/avro_rs/schema/enum.Schema.html) is the number of days since the unix epoch
        // But Datum::Date is the number of days since ce
        Datum::Date(v) => {
            let days = days_from_ce_to_ue();
            may_union(Value::Date(v-days), is_nullable)},
        // this will lose some accuracy
        Datum::Time(v) => {
            let nanos = (v >> 32) * 1_000_000_000 + (v & 0xFFFF_FFFF);
            may_union(Value::TimeMicros(nanos / 1000), is_nullable)
        },
    }
}

// days from ce to unix epoch
fn days_from_ce_to_ue() -> i32 {
    let option = chrono::NaiveDate::from_ymd_opt(1970, 1, 1);
    let days = option.unwrap().num_days_from_ce();
    days
}

/// Convert the avro `Value` into the `Datum`.
///
/// Some types defined by avro are not used and the conversion rule is totally
/// based on the implementation in the server.
fn avro_value_to_datum(value: Value, datum_type: DatumKind) -> Result<Datum> {
    let datum = match (value, datum_type) {
        (Value::Null, _) => Datum::Null,
        (Value::TimestampMillis(v), DatumKind::Timestamp) => Datum::Timestamp(Timestamp::new(v)),
        (Value::Double(v), DatumKind::Double) => Datum::Double(v),
        (Value::Float(v), DatumKind::Float) => Datum::Float(v),
        (Value::Bytes(v), DatumKind::Varbinary) => Datum::Varbinary(Bytes::from(v)),
        (Value::String(v), DatumKind::String) => Datum::String(StringBytes::from(v)),
        (Value::Boolean(v), DatumKind::Boolean) => Datum::Boolean(v),
        (Value::Long(v), DatumKind::Int64) => Datum::Int64(v),
        (Value::Long(v), DatumKind::UInt64) => Datum::UInt64(v as u64),
        (Value::Long(v), DatumKind::UInt32) => Datum::UInt32(v as u32),
        (Value::Int(v), DatumKind::Int8) => Datum::Int8(v as i8),
        (Value::Int(v), DatumKind::UInt8) => Datum::UInt8(v as u8),
        (Value::Int(v), DatumKind::Int16) => Datum::Int16(v as i16),
        (Value::Int(v), DatumKind::UInt16) => Datum::UInt16(v as u16),
        (Value::Int(v), DatumKind::Int32) => Datum::Int32(v),
        (Value::Date(v), DatumKind::Date) => {
            let days = days_from_ce_to_ue();
            Datum::Date(v+days)},
        (Value::TimeMicros(v), DatumKind::Time) =>{
            let secs= v/1_000_000;
            let nans = (v%1_000_000)*1000;
            Datum::Time((secs<<32)+nans)
        },
        (Value::Union(inner_val), _) => avro_value_to_datum(*inner_val, datum_type)?,
        (other_value, _) => {
            return UnsupportedConversion {
                value: other_value,
                datum_type,
            }
            .fail()
        }
    };

    Ok(datum)
}

#[inline]
fn may_union(val: Value, is_nullable: bool) -> Value {
    if is_nullable {
        Value::Union(Box::new(val))
    } else {
        val
    }
}

fn avro_row_to_row(
    schema: &AvroSchema,
    record_schema: &RecordSchema,
    mut raw: &[u8],
    row: &mut Vec<Datum>,
) -> Result<()> {
    let record = avro_rs::from_avro_datum(schema, &mut raw, None)
        .box_err()
        .context(ParseAvroRecord)?;
    if let Value::Record(cols) = record {
        for (column_name, column_value) in cols {
            let column_schema =
                record_schema
                    .column_by_name(&column_name)
                    .context(ColumnNotFound {
                        column: column_name,
                    })?;
            let datum = avro_value_to_datum(column_value, column_schema.data_type)?;
            row.push(datum);
        }

        Ok(())
    } else {
        InvalidAvroRecord { value: record }.fail()
    }
}

#[cfg(test)]
mod tests {
    use avro_rs::types::Value::TimeMicros;
    use chrono::Timelike;
    use super::*;

    #[test]
    fn test_avro_value_to_datum_overflow() {
        let overflow_value = i64::MAX as u64 + 1;
        let avro_value = Value::Long(overflow_value as i64);
        let datum = avro_value_to_datum(avro_value, DatumKind::UInt64).unwrap();
        let expected = Datum::UInt64(overflow_value);

        assert_eq!(datum, expected);
    }

    #[test]
    fn test_avro_value_to_datum_date() {
        let date =  chrono::NaiveDate::from_ymd_opt(2022,12,31).unwrap();
        let days = date.num_days_from_ce();
        let expected = Datum::Date(days);
        let value = datum_to_avro_value(expected, true);
        let datum = avro_value_to_datum(value, DatumKind::Date).unwrap();
        let expected = Datum::Date(days);
        assert_eq!(datum, expected);
    }

    #[test]
    fn test_avro_value_to_datum_time() {
       let time =  chrono::NaiveTime::from_hms_milli_opt(23,59,59,999).unwrap();
        let secs = time.num_seconds_from_midnight() as i64;
        let nanos = time.nanosecond() as i64;
        let expected = Datum::Time((secs << 32) + nanos);
        let value = datum_to_avro_value(expected, true);
        let datum = avro_value_to_datum(value, DatumKind::Time).unwrap();
        let expected = Datum::Time((secs << 32) + nanos);
        assert_eq!(datum, expected);
    }
}
